package org.jagentic.core;

import java.util.Map;

/** One inbound message. {@code conversationId} is the partition/state key. */
public record Event(String conversationId, String userId, String text, Map<String, String> metadata) {
  public Event(String conversationId, String userId, String text) {
    this(conversationId, userId, text, Map.of());
  }
}
