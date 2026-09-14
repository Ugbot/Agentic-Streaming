package org.jagentic.ports.spring;

import java.util.List;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import org.jagentic.core.AgentContext;
import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;

/**
 * The inbound edge (§3h / §4 step 1 of the design doc): a synchronous request/response
 * front door. {@code POST /agent} parses an {@link AgentRequest}, builds a per-turn
 * {@link AgentContext} over the singleton {@link ConversationStore}, and runs one turn
 * of the shared {@link RoutedGraph} (router -> path -> verifier). The reply is returned
 * directly.
 *
 * <p>This is the "request/response and multi-turn conversational agent" sweet spot the
 * doc calls out (§6): the transcript persists across turns in the {@code ConversationStore},
 * so the next call to the same {@code conversationId} resumes the conversation.
 */
@RestController
public class AgentController {

  private final ConversationStore store;
  private final KeyedStateStore state;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;
  private final RoutedGraph graph;

  public AgentController(ConversationStore store,
                         KeyedStateStore state,
                         ToolRegistry tools,
                         Retrieval.TwoTierRetriever retriever,
                         RoutedGraph graph) {
    this.store = store;
    this.state = state;
    this.tools = tools;
    this.retriever = retriever;
    this.graph = graph;
  }

  @PostMapping("/agent")
  public AgentResponse handle(@RequestBody AgentRequest req) {
    Event event = new Event(req.conversationId(), req.userId(), req.text());

    // Persist the inbound turn + maintain the userId index (the multi-tenant key).
    store.associateUser(req.conversationId(), req.userId());
    store.append(req.conversationId(), ChatMessage.user(req.text()));

    AgentContext ctx = new AgentContext(
        req.conversationId(), req.userId(), store, state, tools, retriever);

    TurnResult result = graph.handle(event, ctx);

    store.append(req.conversationId(), ChatMessage.assistant(result.reply));

    return new AgentResponse(
        result.conversationId, result.reply, result.path, result.ok, result.toolCalls);
  }

  /** Inbound payload: {conversationId, userId, text}. */
  public record AgentRequest(String conversationId, String userId, String text) {}

  /** Outbound payload: the verified reply + which path handled it + tool calls made. */
  public record AgentResponse(String conversationId, String reply, String path,
                              boolean ok, List<String> toolCalls) {}
}
