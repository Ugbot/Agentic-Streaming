package org.jagentic.core.cep;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jagentic.core.Event;

/**
 * The one place the core reads a turn's event time. A turn carries it as the decimal string
 * {@code metadata.event_time_ms} ({@code spec/v1/primitives.md}, event time); {@link #annotate}
 * copies it, together with the whole metadata map, onto the {@code turn_received} payload so the
 * log alone is enough to replay every event-time decision. Re-point {@link #eventTimeMs} at the
 * shared logical clock when one is wired into the runtime.
 */
public final class EventTime {

  /** The metadata key that carries a turn's event time in milliseconds. */
  public static final String METADATA_KEY = "event_time_ms";

  private EventTime() {}

  /** The event time of {@code event}, or null when its metadata does not carry one. */
  public static Long eventTimeMs(Event event) {
    String raw = event.metadata() == null ? null : event.metadata().get(METADATA_KEY);
    if (raw == null) {
      return null;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("turn " + event.turnId() + " metadata." + METADATA_KEY
          + " is not an integer: " + raw);
    }
  }

  /**
   * Adds {@code event_time_ms} (when present) and {@code metadata} (when non-empty) to a
   * {@code turn_received} payload, exactly as the reference runtime records them.
   */
  public static Map<String, Object> annotate(Map<String, Object> received, Event event) {
    Long eventTime = eventTimeMs(event);
    if (eventTime != null) {
      received.put(METADATA_KEY, eventTime);
    }
    if (event.metadata() != null && !event.metadata().isEmpty()) {
      received.put("metadata", new LinkedHashMap<>(event.metadata()));
    }
    return received;
  }
}
