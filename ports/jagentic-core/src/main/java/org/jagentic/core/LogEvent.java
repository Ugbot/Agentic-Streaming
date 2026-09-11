package org.jagentic.core;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * One appended conversation-log entry. Identity is {@code (conversationId, sequence)}; {@code
 * sequence} is dense and zero-based per conversation. {@code type} is kept as the wire string so an
 * older runtime can replay (and ignore) event types it does not know.
 */
public record LogEvent(String conversationId, long sequence, String turnId, String type,
                       Map<String, Object> payload) implements Serializable {

  public LogEvent {
    payload = payload == null ? Map.of() : payload;
  }

  public Optional<EventType> eventType() {
    return EventType.parse(type);
  }

  public boolean is(EventType t) {
    return t.wire().equals(type);
  }

  /** The {@code events[]} entry shape of {@code spec/v1/result.schema.json}. */
  public Map<String, Object> toMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("type", type);
    m.put("sequence", sequence);
    m.put("payload", payload);
    return m;
  }
}
