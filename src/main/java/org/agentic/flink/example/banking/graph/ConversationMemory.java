package org.agentic.flink.example.banking.graph;

import java.util.List;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.memory.conversation.ConversationStores;

/**
 * Per-{@code contextId} conversation transcript for the banking graph — a thin typed facade over the
 * framework's {@link ConversationStore}. The cross-operator, cross-turn transcript the routed graph
 * needs now lives in the core framework ({@code org.agentic.flink.memory.conversation}); this class
 * just routes to the discovered store so existing call sites (router appends the user turn, paths
 * read history, verifier appends the reply) stay unchanged.
 *
 * <p>By default that store is the process-wide in-JVM {@link
 * org.agentic.flink.memory.conversation.InMemoryConversationStore} — correct for the embedded
 * single-JVM deployment. Drop a Redis/Postgres-backed {@code ConversationStore} on the classpath
 * (via {@code ServiceLoader}) to make the same transcript shared across a distributed cluster, with
 * no change here.
 */
public final class ConversationMemory {

  private static final ConversationStore STORE = ConversationStores.discover();

  private ConversationMemory() {}

  /** The underlying framework store (for operators that want to inject it explicitly). */
  public static ConversationStore store() {
    return STORE;
  }

  /** Append a message to a session's transcript. */
  public static void append(String contextId, ChatMessage message) {
    STORE.append(contextId, message);
  }

  /** A snapshot copy of the session's transcript (safe to iterate). */
  public static List<ChatMessage> history(String contextId) {
    return STORE.history(contextId);
  }

  /** Drop a session's transcript (and any attributes). */
  public static void clear(String contextId) {
    STORE.clear(contextId);
  }
}
