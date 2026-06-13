package org.agentic.flink.a2a;

import java.util.function.LongSupplier;

/**
 * A minimal, thread-safe circuit breaker scoped to a single A2A peer.
 *
 * <p>Used by {@link ResilientA2AClient} to stop hammering a peer that is repeatedly failing: after
 * {@code threshold} consecutive failures the breaker trips {@link State#OPEN OPEN} and short-circuits
 * calls (they fail fast without touching the network) for {@code openMs}. The first call after that
 * window is admitted as a {@link State#HALF_OPEN HALF_OPEN} trial — its success closes the breaker,
 * its failure re-opens it for another window.
 *
 * <p>A non-positive {@code threshold} disables the breaker entirely ({@link #allowRequest()} always
 * returns {@code true}). The breaker is a runtime object built on the task side; it is not
 * serialized.
 */
final class CircuitBreaker {

  enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int threshold;
  private final long openMs;
  private final LongSupplier clock;

  private State state = State.CLOSED;
  private int consecutiveFailures = 0;
  private long openedAt = 0L;
  // Guards the single in-flight probe while HALF_OPEN, so concurrent callers don't all rush the peer.
  private boolean probeInFlight = false;

  CircuitBreaker(int threshold, long openMs) {
    this(threshold, openMs, System::currentTimeMillis);
  }

  CircuitBreaker(int threshold, long openMs, LongSupplier clock) {
    this.threshold = threshold;
    this.openMs = Math.max(0, openMs);
    this.clock = clock;
  }

  /** Whether the breaker disables short-circuiting altogether. */
  boolean disabled() {
    return threshold <= 0;
  }

  /**
   * Admission check. Returns {@code true} if the call may proceed. When OPEN, transitions to
   * HALF_OPEN and admits exactly one trial once the open window has elapsed.
   */
  synchronized boolean allowRequest() {
    if (disabled()) {
      return true;
    }
    switch (state) {
      case CLOSED:
        return true;
      case OPEN:
        if (clock.getAsLong() - openedAt >= openMs) {
          state = State.HALF_OPEN;
          probeInFlight = true;
          return true; // single probe admitted
        }
        return false;
      case HALF_OPEN:
        if (!probeInFlight) {
          probeInFlight = true;
          return true;
        }
        return false; // a probe is already outstanding
      default:
        return true;
    }
  }

  /** Record a successful call: closes the breaker and clears the failure count. */
  synchronized void recordSuccess() {
    state = State.CLOSED;
    consecutiveFailures = 0;
    probeInFlight = false;
  }

  /** Record a failed call: trips OPEN at the threshold, or re-opens a failed HALF_OPEN probe. */
  synchronized void recordFailure() {
    if (disabled()) {
      return;
    }
    consecutiveFailures++;
    if (state == State.HALF_OPEN || consecutiveFailures >= threshold) {
      state = State.OPEN;
      openedAt = clock.getAsLong();
      probeInFlight = false;
    }
  }

  synchronized State state() {
    return state;
  }
}
