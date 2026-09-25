package org.jagentic.ports.kafkastreams;

import java.util.Map;
import java.util.Objects;

import org.jagentic.core.Event;

/**
 * Named-field construction of a core {@link Event}. The core constructors take
 * {@code userId} and {@code text} as adjacent positional {@code String} arguments (the
 * Python core takes them in the other order), so adapter call sites build events here
 * and never depend on argument position.
 */
final class EventBuilder {

  private String conversationId;
  private String turnId;
  private String userId;
  private String text;
  private Map<String, String> metadata = Map.of();

  private EventBuilder() {}

  static EventBuilder turn() {
    return new EventBuilder();
  }

  EventBuilder conversationId(String conversationId) {
    this.conversationId = conversationId;
    return this;
  }

  /** Optional idempotency key; the core mints one when absent. */
  EventBuilder turnId(String turnId) {
    this.turnId = turnId;
    return this;
  }

  EventBuilder userId(String userId) {
    this.userId = userId;
    return this;
  }

  EventBuilder text(String text) {
    this.text = text;
    return this;
  }

  EventBuilder metadata(Map<String, String> metadata) {
    this.metadata = metadata == null ? Map.of() : metadata;
    return this;
  }

  Event build() {
    return new Event(
        Objects.requireNonNull(conversationId, "conversationId"),
        turnId,
        Objects.requireNonNull(userId, "userId"),
        Objects.requireNonNull(text, "text"),
        metadata,
        null);
  }
}
