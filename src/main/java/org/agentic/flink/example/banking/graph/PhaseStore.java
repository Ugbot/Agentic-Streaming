package org.agentic.flink.example.banking.graph;

import org.agentic.flink.memory.conversation.ConversationStore;

/**
 * Typed per-{@code contextId} {@link BankingPhase} accessor — a thin facade over a conversation
 * attribute (key {@code "banking.phase"}) in the framework's {@link ConversationStore}. Written by
 * {@link BankingVerifierFunction} and read by {@link BankingRouterFunction}; because those are
 * <b>separate</b> keyed operators, the phase must live in the cross-operator conversation store, not
 * in any single operator's keyed state. It shares the same store as {@link ConversationMemory}, so
 * the transcript and the workflow phase are co-located per session and swappable together (in-JVM by
 * default, Redis/Postgres-backed for a distributed cluster).
 */
public final class PhaseStore {

  private static final String PHASE_ATTR = "banking.phase";
  private static final ConversationStore STORE = ConversationMemory.store();

  private PhaseStore() {}

  /** Current phase for a session ({@link BankingPhase#NEW} if unseen or unparsable). */
  public static BankingPhase get(String contextId) {
    return STORE
        .getAttribute(contextId, PHASE_ATTR)
        .map(PhaseStore::parse)
        .orElse(BankingPhase.NEW);
  }

  public static void set(String contextId, BankingPhase phase) {
    if (contextId != null && phase != null) {
      STORE.putAttribute(contextId, PHASE_ATTR, phase.name());
    }
  }

  public static void clear(String contextId) {
    if (contextId != null) {
      STORE.putAttribute(contextId, PHASE_ATTR, BankingPhase.NEW.name());
    }
  }

  private static BankingPhase parse(String s) {
    try {
      return BankingPhase.valueOf(s);
    } catch (IllegalArgumentException | NullPointerException e) {
      return BankingPhase.NEW;
    }
  }
}
