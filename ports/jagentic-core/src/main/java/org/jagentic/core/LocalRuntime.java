package org.jagentic.core;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-process runtime: shared stores, a shared {@link ConversationLog}, and a per-conversation
 * single writer. {@link #submit} runs the turn on the caller's thread under the conversation's fair
 * lock; {@link #submitAsync} appends the turn to the conversation's serial queue so that turns
 * submitted concurrently are applied in submission order (the local stand-in for Flink keyBy / a
 * Kafka partition / an actor mailbox).
 *
 * <p>Idempotency, retries, verification attempts and sagas are implemented by {@link RoutedGraph}
 * over the log; this class only guarantees the ordering the spec requires and hands each turn a
 * context bound to the shared log and the graph's policies. A new {@code LocalRuntime} built over an
 * existing log ({@link #LocalRuntime(RoutedGraph, ConversationStore, KeyedStateStore, ToolRegistry,
 * Retrieval.TwoTierRetriever, ConversationLog)}) is a restart: state comes from replaying the log,
 * nothing is re-executed.</p>
 */
public final class LocalRuntime implements Runtime {
  private final RoutedGraph graph;
  private final ConversationStore store;
  private final KeyedStateStore state;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;
  private final ConversationLog log;
  private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<?>> tails = new ConcurrentHashMap<>();
  private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r, "jagentic-local-runtime");
    t.setDaemon(true);
    return t;
  });

  public LocalRuntime(RoutedGraph graph, ConversationStore store, KeyedStateStore state,
                      ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this(graph, store, state, tools, retriever, new ConversationLog.InMemory());
  }

  public LocalRuntime(RoutedGraph graph, ConversationStore store, KeyedStateStore state,
                      ToolRegistry tools, Retrieval.TwoTierRetriever retriever, ConversationLog log) {
    this.graph = graph;
    this.store = store;
    this.state = state;
    this.tools = tools;
    this.retriever = retriever;
    this.log = log == null ? new ConversationLog.InMemory() : log;
    rebuildMaterializedViews();
  }

  public ConversationStore store() {
    return store;
  }

  public ConversationLog log() {
    return log;
  }

  public RoutedGraph graph() {
    return graph;
  }

  /** The folded state of one conversation: the {@code replay} verb, no side effects. */
  public ConversationState replay(String conversationId) {
    return log.state(conversationId, graph.contextWindow());
  }

  @Override
  public TurnResult submit(Event event) {
    ReentrantLock lock = locks.computeIfAbsent(event.conversationId(), k -> new ReentrantLock(true));
    lock.lock();
    try {
      AgentContext ctx = new AgentContext(event.conversationId(), event.turnId(), event.userId(),
          store, state, tools, retriever, log, graph.policies());
      return graph.handle(event, ctx);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Enqueues the turn behind every turn already submitted for the same conversation and returns
   * its future result. Turns of different conversations run concurrently.
   */
  public CompletableFuture<TurnResult> submitAsync(Event event) {
    synchronized (tails) {
      CompletableFuture<?> tail = tails.getOrDefault(event.conversationId(),
          CompletableFuture.completedFuture(null));
      CompletableFuture<TurnResult> next = tail.handleAsync((ignored, err) -> submit(event), executor);
      tails.put(event.conversationId(), next);
      return next;
    }
  }

  /**
   * Fills the conversation store's transcript from the log for conversations whose transcript is
   * missing: what a restart over a durable log does for the materialized view.
   */
  private void rebuildMaterializedViews() {
    for (String cid : log.conversations()) {
      if (store.messageCount(cid) == 0) {
        for (ChatMessage m : log.state(cid).transcript()) {
          store.append(cid, m);
        }
      }
    }
  }
}
