package org.agentic.flink.runtime.testkit;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.agentic.flink.runtime.ProcessingClock;
import org.apache.flink.streaming.api.TimerService;

/**
 * The fixture's logical processing clock for a Flink job: a reading the test advances by hand
 * ({@code advance_time_ms}) and the operator reads instead of the task manager's wall clock. The
 * reading lives in a JVM-wide registry keyed by clock id, like the driver's queues, because
 * MiniCluster tasks share the test JVM and the operator instance itself is serialized. The reading
 * therefore survives a stop-with-savepoint restart the same way wall time does; the pending timers
 * that must be recovered live in the operator's keyed state, not here.
 */
public final class FixtureProcessingClock implements ProcessingClock, AutoCloseable {
  private static final long serialVersionUID = 1L;

  private static final Map<String, AtomicLong> READINGS = new ConcurrentHashMap<>();

  private final String clockId;

  public FixtureProcessingClock() {
    this("clock-" + UUID.randomUUID());
  }

  private FixtureProcessingClock(String clockId) {
    this.clockId = clockId;
    READINGS.putIfAbsent(clockId, new AtomicLong(0L));
  }

  public String clockId() {
    return clockId;
  }

  /** Moves the reading forward; the spec's processing clock never runs backwards. */
  public void advance(long ms) {
    if (ms < 0) {
      throw new IllegalArgumentException("logical time never moves backwards: advance by " + ms);
    }
    reading().addAndGet(ms);
  }

  /** The current reading, without needing a {@link TimerService}. */
  public long nowMs() {
    return reading().get();
  }

  @Override
  public long nowMs(TimerService timerService) {
    return nowMs();
  }

  private AtomicLong reading() {
    AtomicLong r = READINGS.get(clockId);
    if (r == null) {
      throw new IllegalStateException("fixture clock " + clockId + " is closed");
    }
    return r;
  }

  @Override
  public void close() {
    READINGS.remove(clockId);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FixtureProcessingClock c && c.clockId.equals(clockId);
  }

  @Override
  public int hashCode() {
    return clockId.hashCode();
  }
}
