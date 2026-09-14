package org.jagentic.core;

import java.util.Map;

/**
 * The runtime's processing-time clock as the spec sees it ({@code spec/v1/primitives.md} section 8):
 * a logical reading in milliseconds that never runs backwards and is observable only through turns.
 * {@link #system()} reads the wall clock; {@link Manual} is advanced explicitly (the fixtures'
 * {@code advance_time_ms}) and can be {@linkplain Manual#recoveredFrom(ConversationLog) rebuilt}
 * from the readings the graph records in {@code turn_received}, so a restart does not reset it.
 */
public interface LogicalClock {

  /** Payload key under which {@code turn_received} records the processing clock reading. */
  String PROCESSING_TIME_KEY = "processing_time_ms";

  /** The current processing-time reading in milliseconds. */
  long nowMs();

  /** Wall-clock processing time. */
  static LogicalClock system() {
    return System::currentTimeMillis;
  }

  /** A clock that only moves when told to; the fixtures' logical processing clock. */
  final class Manual implements LogicalClock {
    private long nowMs;

    public Manual() {
      this(0L);
    }

    public Manual(long nowMs) {
      if (nowMs < 0) {
        throw new IllegalArgumentException("logical time cannot be negative: " + nowMs);
      }
      this.nowMs = nowMs;
    }

    /**
     * A clock resumed at the highest reading any {@code turn_received} in {@code log} recorded,
     * or 0 when none did. Because the graph records the reading on every turn of a workflow with
     * timers, this restores the clock the runtime had when it last applied a turn.
     */
    public static Manual recoveredFrom(ConversationLog log) {
      long max = 0L;
      for (String cid : log.conversations()) {
        for (LogEvent e : log.events(cid)) {
          if (!e.is(EventType.TURN_RECEIVED)) {
            continue;
          }
          Map<String, Object> p = e.payload();
          if (p != null && p.get(PROCESSING_TIME_KEY) instanceof Number n) {
            max = Math.max(max, n.longValue());
          }
        }
      }
      return new Manual(max);
    }

    @Override
    public synchronized long nowMs() {
      return nowMs;
    }

    /** Move the clock forward by {@code ms}; logical time never moves backwards. */
    public synchronized void advance(long ms) {
      if (ms < 0) {
        throw new IllegalArgumentException("logical time never moves backwards: advance by " + ms);
      }
      nowMs += ms;
    }
  }
}
