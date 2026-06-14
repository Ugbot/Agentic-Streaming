package org.jagentic.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-process runtime: shared stores + a per-conversation lock so each conversation
 * is processed by a single writer at a time (the local stand-in for Flink keyBy / a
 * Kafka partition / a Ray actor). Engine ports supply their own runtime where the
 * engine provides per-key ordering.
 */
public final class LocalRuntime implements Runtime {
  private final RoutedGraph graph;
  private final ConversationStore store;
  private final KeyedStateStore state;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;
  private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();

  public LocalRuntime(RoutedGraph graph, ConversationStore store, KeyedStateStore state,
                      ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this.graph = graph;
    this.store = store;
    this.state = state;
    this.tools = tools;
    this.retriever = retriever;
  }

  public ConversationStore store() {
    return store;
  }

  @Override
  public TurnResult submit(Event event) {
    ReentrantLock lock = locks.computeIfAbsent(event.conversationId(), k -> new ReentrantLock());
    lock.lock();
    try {
      AgentContext ctx = new AgentContext(event.conversationId(), event.userId(), store, state, tools, retriever);
      return graph.handle(event, ctx);
    } finally {
      lock.unlock();
    }
  }
}
