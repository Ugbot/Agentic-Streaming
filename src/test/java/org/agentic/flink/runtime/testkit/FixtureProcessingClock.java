package org.agentic.flink.runtime.testkit;

import org.agentic.flink.runtime.ManualProcessingClock;
import org.agentic.flink.runtime.ProcessingClock;
import org.apache.flink.streaming.api.TimerService;

/**
 * The fixture's logical processing clock for a Flink job: a {@link ManualProcessingClock} with a
 * fresh id that the test advances by hand ({@code advance_time_ms}) and releases when the fixture
 * is done. The reading survives a stop-with-savepoint restart the same way wall time does; the
 * pending timers that must be recovered live in the operator's keyed state, not here.
 */
public final class FixtureProcessingClock implements ProcessingClock, AutoCloseable {
  private static final long serialVersionUID = 1L;

  private final ManualProcessingClock clock;

  public FixtureProcessingClock() {
    this.clock = ManualProcessingClock.create();
  }

  public String clockId() {
    return clock.clockId();
  }

  public void advance(long ms) {
    clock.advance(ms);
  }

  public long nowMs() {
    return clock.nowMs();
  }

  @Override
  public long nowMs(TimerService timerService) {
    return clock.nowMs(timerService);
  }

  @Override
  public void close() {
    clock.release();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FixtureProcessingClock c && c.clock.equals(clock);
  }

  @Override
  public int hashCode() {
    return clock.hashCode();
  }
}
