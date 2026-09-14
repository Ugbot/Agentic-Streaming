package org.jagentic.ports.quarkus;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

import org.jagentic.core.AgentContext;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;

import org.jagentic.ports.quarkus.AgentMessages.AgentReply;
import org.jagentic.ports.quarkus.AgentMessages.AgentRequest;

/**
 * The inbound REST edge (essence §2.5). A single synchronous turn: run the unchanged
 * {@link RoutedGraph#handle(Event, AgentContext)} over the shared {@link ConversationStore}
 * and {@link Retrieval.TwoTierRetriever}, and return the reply.
 *
 * <p>The (blocking) core runs on a worker thread via Mutiny so the request never occupies
 * the reactive event loop — the C4 async-return shape from the design doc, expressed as a
 * {@link Uni}.
 */
@Path("/agent")
public class AgentResource {

  @Inject RoutedGraph graph;
  @Inject ConversationStore conversations;
  @Inject KeyedStateStore shortTerm;
  @Inject ToolRegistry tools;
  @Inject Retrieval.TwoTierRetriever retriever;

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<AgentReply> turn(AgentRequest request) {
    return Uni.createFrom().item(() -> runTurn(request))
        // off the event loop: the RoutedGraph + tools are blocking, synchronous core code.
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }

  private AgentReply runTurn(AgentRequest request) {
    String cid = request.conversationId();
    String userId = request.userId();
    conversations.associateUser(cid, userId);

    Event event = new Event(cid, userId, request.text());
    AgentContext ctx =
        new AgentContext(cid, userId, conversations, shortTerm, tools, retriever);

    TurnResult result = graph.handle(event, ctx);
    return new AgentReply(result.conversationId, result.path, result.ok, result.reply);
  }
}
