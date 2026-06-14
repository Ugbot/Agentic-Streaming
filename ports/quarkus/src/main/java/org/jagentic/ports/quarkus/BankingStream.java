package org.jagentic.ports.quarkus;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.common.annotation.RunOnVirtualThread;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

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
 * The streaming agent edge over Kafka (essence §2.4 / design doc §3c). Consumes the
 * {@code requests} channel and emits to {@code replies}, running the unchanged
 * {@link RoutedGraph}.
 *
 * <p><b>Single-writer-per-conversation (C2):</b> the {@code requests} topic is keyed by
 * {@code conversationId}, so each partition is owned by exactly one consumer in the group
 * — that consumer is the single writer for every conversation on its partitions. No lock
 * is needed as long as the producer keys honestly. Durable keyed state (C1) comes from the
 * {@link ConversationStore} SPI (here in-memory; swap for Redis/Fluss) rather than an
 * engine state primitive; the store is written inside {@code RoutedGraph.handle} before the
 * Kafka offset commits, so the store leads the offset (at-least-once + idempotent core).
 *
 * <p>The blocking core runs on a virtual thread (C4), keeping the event loop free.
 */
@ApplicationScoped
public class BankingStream {

  @Inject RoutedGraph graph;
  @Inject ConversationStore conversations;
  @Inject KeyedStateStore shortTerm;
  @Inject ToolRegistry tools;
  @Inject Retrieval.TwoTierRetriever retriever;

  @Incoming("requests")
  @Outgoing("replies")
  @RunOnVirtualThread
  public AgentReply onTurn(AgentRequest request) {
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
