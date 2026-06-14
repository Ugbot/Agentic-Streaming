package org.agentic.flink.memory.conversation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.llm.ChatMessage;

/**
 * Per-conversation memory shared <b>across operators</b> — the multi-turn transcript (and small
 * scalar workflow attributes) for a conversation, keyed by a stable conversation identifier
 * (A2A {@code contextId}, an agent {@code flowId}, a session id, …).
 *
 * <p>This fills the gap between the two existing memory tiers:
 *
 * <ul>
 *   <li>{@link org.agentic.flink.memory.ShortTermMemory} is <b>per-operator, per-key</b> Flink
 *       state — fast, checkpointed, but isolated to a single operator's keyed state partition. Two
 *       different operators in a routed graph (router → path → verifier) keyed on the same
 *       conversation do <b>not</b> see each other's short-term state.
 *   <li>{@link org.agentic.flink.storage.LongTermMemoryStore} persists {@link
 *       org.agentic.flink.context.core.AgentContext} + facts for resumption/archival, not the raw
 *       turn-by-turn chat transcript a multi-turn agent replays into the model each turn.
 * </ul>
 *
 * <p>A {@code ConversationStore} is the conversation-scoped layer those need: any operator (or the
 * single in-proc brain) can {@link #append append} the turn it just produced and {@link #history
 * read} the running dialogue, so a conversation split across operators still progresses. It is the
 * formal, swappable home for what the banking example previously hard-coded as an in-JVM map.
 *
 * <p>Implementations must be {@link Serializable} (the store rides the Flink job graph into every
 * operator) and thread-safe (operators in one TaskManager share an instance across threads). The
 * default {@link InMemoryConversationStore} is process-local — correct for the embedded single-JVM
 * deployment; back it with Redis/Postgres (same interface) for a distributed cluster. Discover the
 * configured implementation via {@link ConversationStores}.
 *
 * <p>Methods do not declare checked exceptions: the transcript is a hot path, and remote-backed
 * implementations should degrade gracefully (log + return empty/no-op) rather than fail a turn.
 */
public interface ConversationStore extends Serializable {

  /** Append one message to a conversation's transcript (in arrival order). No-op if id is null. */
  void append(String conversationId, ChatMessage message);

  /** Append several messages, preserving order. */
  default void appendAll(String conversationId, List<ChatMessage> messages) {
    if (messages == null) {
      return;
    }
    for (ChatMessage m : messages) {
      append(conversationId, m);
    }
  }

  /**
   * A snapshot copy of the full transcript in arrival order (oldest first), safe to iterate and
   * feed to a model. Empty (never null) for an unknown conversation.
   */
  List<ChatMessage> history(String conversationId);

  /**
   * The last {@code maxMessages} of the transcript (oldest-first within the tail) — a cheap context
   * window for replaying recent dialogue. {@code maxMessages <= 0} returns the full history.
   */
  default List<ChatMessage> recent(String conversationId, int maxMessages) {
    List<ChatMessage> all = history(conversationId);
    if (maxMessages <= 0 || all.size() <= maxMessages) {
      return all;
    }
    return new java.util.ArrayList<>(all.subList(all.size() - maxMessages, all.size()));
  }

  /** Number of messages currently retained for a conversation. */
  int messageCount(String conversationId);

  /**
   * Set a small scalar workflow attribute for a conversation (e.g. a routing phase or flag) shared
   * across operators. Values are strings so any backend can persist them uniformly; callers
   * serialize enums via {@code name()} and parse back.
   */
  void putAttribute(String conversationId, String key, String value);

  /** Read a previously-set workflow attribute. */
  Optional<String> getAttribute(String conversationId, String key);

  /** All workflow attributes for a conversation (empty, never null, if none). */
  Map<String, String> attributes(String conversationId);

  /**
   * Associate a conversation with a user (idempotent), so it can be retrieved per-user via {@link
   * #conversationsForUser}. A conversation belongs to one user; associating a new user replaces the
   * prior association. Lets the same conversation be addressed both by {@code conversationId} and by
   * the owning user — e.g. to list or resume a user's conversations.
   */
  void associateUser(String conversationId, String userId);

  /** The user a conversation was associated with, if any. */
  Optional<String> userOf(String conversationId);

  /** Conversation ids associated with a user, in association order (empty, never null). */
  List<String> conversationsForUser(String userId);

  /** Forget a conversation entirely — transcript, attributes, and its user association. */
  void clear(String conversationId);

  /**
   * Conversation ids with retained state (transcript or attributes). Primarily for monitoring,
   * eviction sweeps, and tests.
   */
  List<String> conversations();
}
