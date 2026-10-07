package org.jagentic.core;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * An inbound turn: one user message on one conversation. {@code turnId} is the idempotency key the
 * spec (v1 primitives §4) makes mandatory; a redelivered {@code turnId} returns the recorded result
 * with status {@code duplicate}. {@code signal} is non-null only when the event resumes a suspended
 * turn of the same {@code turnId}.
 *
 * <p>The pre-spec constructors ({@code (conversationId, userId, text[, metadata])}) still work and
 * mint a fresh random {@code turnId}, so callers that never redeliver keep their behaviour.</p>
 */
public record Event(String conversationId, String turnId, String userId, String text,
                    Map<String, String> metadata, Map<String, Object> signal) implements Serializable {

  public Event {
    turnId = turnId == null || turnId.isBlank() ? UUID.randomUUID().toString() : turnId;
    metadata = metadata == null ? Map.of() : metadata;
  }

  public Event(String conversationId, String userId, String text) {
    this(conversationId, null, userId, text, Map.of(), null);
  }

  public Event(String conversationId, String userId, String text, Map<String, String> metadata) {
    this(conversationId, null, userId, text, metadata, null);
  }

  /** A turn with an explicit idempotency key. */
  public static Event turn(String conversationId, String turnId, String userId, String text) {
    return new Event(conversationId, turnId, userId, text, Map.of(), null);
  }

  /** A turn carrying metadata, such as {@code event_time_ms} for the event clock. */
  public static Event turn(String conversationId, String turnId, String userId, String text,
                           Map<String, String> metadata) {
    return new Event(conversationId, turnId, userId, text, metadata == null ? Map.of() : metadata, null);
  }

  /** The resume command for a suspended turn: same {@code turnId}, carrying the external signal. */
  public static Event resume(String conversationId, String turnId, Map<String, Object> signal) {
    return new Event(conversationId, turnId, null, "", Map.of(), signal == null ? Map.of() : signal);
  }

  public boolean isResume() {
    return signal != null;
  }
}
