package org.jagentic.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Per-conversation handle an agent's brain uses for one turn — decouples agent
 * logic from the engine. */
public final class AgentContext {
  public final String conversationId;
  public final String userId;
  public final ConversationStore store;
  public final KeyedStateStore state;
  public final ToolRegistry tools;
  public final Retrieval.TwoTierRetriever retriever; // may be null
  public final List<String> toolCalls = new ArrayList<>();
  public List<AgentListener> listeners = List.of(); // set by RoutedGraph; tool-call hooks fire here

  public AgentContext(String conversationId, String userId, ConversationStore store,
                      KeyedStateStore state, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this.conversationId = conversationId;
    this.userId = userId;
    this.store = store;
    this.state = state;
    this.tools = tools;
    this.retriever = retriever;
  }

  public Object callTool(String toolId, Map<String, Object> params) {
    toolCalls.add(toolId);
    for (AgentListener l : listeners) {
      l.onToolCallStart(toolId, this);
    }
    try {
      Object result = tools.execute(toolId, params);
      for (AgentListener l : listeners) {
        l.onToolCallEnd(toolId, result, this);
      }
      return result;
    } catch (RuntimeException e) {
      for (AgentListener l : listeners) {
        l.onError("tool:" + toolId, e, this);
      }
      throw e;
    }
  }
}
