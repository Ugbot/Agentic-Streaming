package org.jagentic.pekko.durability;

/** Where conversation state durably lives. The first three are <b>event-sourced</b> — the entity
 * is journal-agnostic, so they're selected purely by the active config's
 * {@code pekko.persistence.journal.plugin} (see {@code application-cluster-*.conf}). {@code REDIS}
 * is a <b>write-through</b> profile (no Pekko Redis journal exists): the entity keeps no event log
 * and flushes each turn's transcript to a {@code RedisConversationStore}. */
public enum DurabilityProfile {
  /** In-memory journal — dev/test, not durable across a JVM restart. */
  MEMORY,
  /** Event-sourced on Postgres via pekko-persistence-jdbc. */
  POSTGRES,
  /** Event-sourced on Cassandra via pekko-persistence-cassandra. */
  CASSANDRA,
  /** Write-through to a Redis-backed ConversationStore (not event-sourced). */
  REDIS;

  public boolean isWriteThrough() {
    return this == REDIS;
  }

  public static DurabilityProfile from(String value) {
    if (value == null || value.isBlank()) {
      return MEMORY;
    }
    return DurabilityProfile.valueOf(value.trim().toUpperCase());
  }
}
