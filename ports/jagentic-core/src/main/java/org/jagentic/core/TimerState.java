package org.jagentic.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The clock and timer part of a conversation's fold ({@code spec/v1/primitives.md} section 8):
 * the watermark (highest {@code event_time_ms} any {@code turn_received} carried), the timers
 * still pending ({@code timer_scheduled} without a later {@code timer_fired}, in schedule order),
 * the ids that fired in firing order, and the highest processing clock reading recorded. Like
 * {@link ConversationState} it is derived from the log and nowhere else, which is what lets a
 * restart rebuild pending timers without re-scheduling or double-firing them.
 */
public final class TimerState implements Serializable {
  private static final long serialVersionUID = 1L;

  /** A scheduled timer that has not fired: the {@code timer_scheduled} payload. */
  public record Pending(String timerId, TimerSpec.Clock clock, long dueMs) implements Serializable {}

  private static final TimerState EMPTY = new TimerState(null, Map.of(), List.of(), null);

  private final Long watermarkMs;
  private final Map<String, Pending> pending;
  private final List<String> fired;
  private final Long processingTimeMs;

  private TimerState(Long watermarkMs, Map<String, Pending> pending, List<String> fired, Long processingTimeMs) {
    this.watermarkMs = watermarkMs;
    this.pending = pending;
    this.fired = fired;
    this.processingTimeMs = processingTimeMs;
  }

  public static TimerState empty() {
    return EMPTY;
  }

  public static TimerState fold(List<LogEvent> log) {
    Long watermark = null;
    Long processing = null;
    Map<String, Pending> pending = new LinkedHashMap<>();
    List<String> fired = new ArrayList<>();
    for (LogEvent e : log) {
      EventType type = e.eventType().orElse(null);
      if (type == null) {
        continue;
      }
      Map<String, Object> p = e.payload() == null ? Map.of() : e.payload();
      switch (type) {
        case TURN_RECEIVED -> {
          if (p.get("event_time_ms") instanceof Number n) {
            watermark = watermark == null ? n.longValue() : Math.max(watermark, n.longValue());
          }
          if (p.get(LogicalClock.PROCESSING_TIME_KEY) instanceof Number n) {
            processing = processing == null ? n.longValue() : Math.max(processing, n.longValue());
          }
        }
        case TIMER_SCHEDULED -> {
          String id = String.valueOf(p.get("timer_id"));
          if (p.get("due_ms") instanceof Number due) {
            pending.put(id, new Pending(id, TimerSpec.Clock.parse(p.get("clock")), due.longValue()));
          }
        }
        case TIMER_FIRED -> {
          String id = String.valueOf(p.get("timer_id"));
          if (pending.remove(id) != null) {
            fired.add(id);
          }
        }
        default -> {
          // other event types carry no clock or timer state
        }
      }
    }
    if (watermark == null && processing == null && pending.isEmpty() && fired.isEmpty()) {
      return EMPTY;
    }
    return new TimerState(watermark, Collections.unmodifiableMap(pending),
        Collections.unmodifiableList(fired), processing);
  }

  /** The conversation watermark, or null when no turn carried an event time. */
  public Long watermarkMs() {
    return watermarkMs;
  }

  /** The watermark after a turn carrying {@code eventTimeMs} arrives: never moves backwards. */
  public Long watermarkAfter(Long eventTimeMs) {
    if (eventTimeMs == null) {
      return watermarkMs;
    }
    return watermarkMs == null ? eventTimeMs : Math.max(watermarkMs, eventTimeMs);
  }

  /** Pending timers by id, in schedule order. */
  public Map<String, Pending> pending() {
    return pending;
  }

  /** Ids of the timers that fired, in firing order. */
  public List<String> fired() {
    return fired;
  }

  /** The highest processing clock reading a turn recorded, or null when none did. */
  public Long processingTimeMs() {
    return processingTimeMs;
  }

  /** Adds {@code watermark_ms} and {@code fired_timers} to a reduced state, as the reference does. */
  void reduceInto(Map<String, Object> state) {
    if (watermarkMs != null) {
      state.put("watermark_ms", watermarkMs);
    }
    if (!fired.isEmpty()) {
      state.put("fired_timers", fired);
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TimerState s && Objects.equals(watermarkMs, s.watermarkMs)
        && pending.equals(s.pending) && fired.equals(s.fired)
        && Objects.equals(processingTimeMs, s.processingTimeMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(watermarkMs, pending, fired, processingTimeMs);
  }
}
