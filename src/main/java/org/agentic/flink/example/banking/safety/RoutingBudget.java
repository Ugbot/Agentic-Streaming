package org.agentic.flink.example.banking.safety;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import org.agentic.flink.typeinfo.JsonTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * Per-session anti-explosion budget — the linchpin that keeps the two banking agents from running
 * each other (or themselves) into the harness's hard timeouts (5 min/turn, 10 min/task → score 0).
 *
 * <p>One instance lives in Flink keyed state per A2A {@code contextId} (so it is naturally isolated
 * across concurrent sessions). The agent operator consults it on every step and, when a cap is hit,
 * stops gracefully with a safe message rather than looping. It bounds three things plus a dedupe:
 *
 * <ul>
 *   <li><b>round-trips</b> — personal↔CS A2A calls in this session;
 *   <li><b>iterations</b> — internal LLM/tool steps in the current turn;
 *   <li><b>deadline</b> — a soft per-turn wall-clock budget, well under the harness's 5 min;
 *   <li><b>dedupe</b> — drops an identical sub-request seen again within a short window (the classic
 *       "two agents echo the same question forever" failure).
 * </ul>
 *
 * <p>Mutable counters + immutable caps, all {@link Serializable} so the object round-trips through
 * Flink state. Time is passed in (never {@code System.currentTimeMillis()} inside) so it stays
 * deterministic and testable.
 */
@TypeInfo(RoutingBudget.Factory.class)
public final class RoutingBudget implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Stores the budget in Flink keyed state as JSON ({@link org.agentic.flink.typeinfo.FlinkJson})
   * rather than the old manual {@code byte[]} Java-serialization workaround. Jackson binds the caps
   * via the all-args constructor ({@code ParameterNamesModule}) and restores the mutable counters +
   * the {@code recentHashes} deque via field access. Mutable, so Flink object reuse deep-copies it.
   */
  public static final class Factory extends JsonTypeInfoFactory<RoutingBudget> {
    public Factory() {
      super(RoutingBudget.class, true);
    }
  }

  private final int maxRoundTrips;
  private final int maxIterations;
  private final long turnDeadlineMs;
  private final int dedupeWindow;

  private int roundTrips;
  private int iterations;
  private long turnStartEpochMs;
  private final Deque<String> recentHashes = new ArrayDeque<>();
  private String lastDenial;

  public RoutingBudget(int maxRoundTrips, int maxIterations, long turnDeadlineMs, int dedupeWindow) {
    this.maxRoundTrips = maxRoundTrips;
    this.maxIterations = maxIterations;
    this.turnDeadlineMs = turnDeadlineMs;
    this.dedupeWindow = Math.max(0, dedupeWindow);
  }

  /** Conservative defaults that comfortably clear the harness timeouts. */
  public static RoutingBudget defaults() {
    return new RoutingBudget(4, 12, 240_000L, 8);
  }

  /** Begin a fresh turn: reset the per-turn iteration count and start the deadline clock. */
  public void startTurn(long nowEpochMs) {
    this.iterations = 0;
    this.turnStartEpochMs = nowEpochMs;
  }

  /**
   * Account for one personal↔CS A2A round-trip. Returns false (and records a denial) once the
   * session has used its allowance — the caller should then answer from what it already has.
   */
  public boolean allowRoundTrip() {
    if (roundTrips >= maxRoundTrips) {
      lastDenial =
          "round-trip budget exhausted (" + roundTrips + "/" + maxRoundTrips + ")";
      return false;
    }
    roundTrips++;
    return true;
  }

  /** Account for one internal LLM/tool iteration this turn. */
  public boolean allowIteration() {
    if (iterations >= maxIterations) {
      lastDenial = "iteration budget exhausted (" + iterations + "/" + maxIterations + ")";
      return false;
    }
    iterations++;
    return true;
  }

  /** True while the current turn is still within its soft deadline. */
  public boolean withinDeadline(long nowEpochMs) {
    boolean ok = (nowEpochMs - turnStartEpochMs) < turnDeadlineMs;
    if (!ok) {
      lastDenial = "turn deadline exceeded (" + (nowEpochMs - turnStartEpochMs) + "ms)";
    }
    return ok;
  }

  /**
   * Dedupe gate: returns false if {@code requestHash} was seen within the recent window (a repeat
   * loop), otherwise records it and returns true. A window of 0 disables deduping.
   */
  public boolean allowDispatch(String requestHash) {
    if (dedupeWindow == 0 || requestHash == null) {
      return true;
    }
    if (recentHashes.contains(requestHash)) {
      lastDenial = "duplicate request suppressed";
      return false;
    }
    recentHashes.addLast(requestHash);
    while (recentHashes.size() > dedupeWindow) {
      recentHashes.removeFirst();
    }
    return true;
  }

  public int roundTripsUsed() {
    return roundTrips;
  }

  public int iterationsUsed() {
    return iterations;
  }

  public int maxRoundTrips() {
    return maxRoundTrips;
  }

  public int maxIterations() {
    return maxIterations;
  }

  /** The most recent reason a gate denied, for the safe fallback message / logs. */
  public String lastDenial() {
    return lastDenial;
  }
}
