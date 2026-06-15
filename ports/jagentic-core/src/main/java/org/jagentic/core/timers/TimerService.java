package org.jagentic.core.timers;

import java.util.List;
import java.util.Optional;

import org.jagentic.core.Event;

/**
 * Portable timers — the counterpart of Flink's {@code TimerService} / a Pekko scheduler / a Temporal
 * timer. Time is <b>logical</b>: callers advance the clock with {@link #advanceTo(long)} and get back
 * the due timers (so tests are deterministic); a real-time driver simply calls {@code advanceTo(now)}
 * on a tick. Powers SLAs, escalate-after-N, retries, scheduled follow-ups, and CEP {@code within}
 * expiry. Engine adapters may replace this entirely with a native timer service.
 */
public interface TimerService {

  /** Schedule (or replace, by id) a timer to fire {@code payload} at {@code fireAt}. */
  void schedule(String id, long fireAt, Event payload);

  /** Cancel a pending timer; returns true if one was removed. */
  boolean cancel(String id);

  /** Remove and return all timers due at {@code now} (fireAt &le; now), ascending by fireAt then
   * schedule order. */
  List<Timer> advanceTo(long now);

  /** The earliest pending deadline, or empty if no timers are pending. */
  Optional<Long> nextDeadline();
}
