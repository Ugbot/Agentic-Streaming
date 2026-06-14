package org.agentic.flink.example.banking;

import java.io.Serializable;
import org.agentic.flink.example.banking.safety.RoutingBudget;

/**
 * The bounded action surface a {@link TurnBrain} gets for one turn. Every outward action is gated by
 * the turn's {@link RoutingBudget}, so the brain physically cannot run away even if its prompt logic
 * tries to: {@link #askCustomerService} consumes a round-trip and refuses past the cap, and {@link
 * #budgetExhausted()} lets the brain bail early with a partial answer.
 */
public final class BankingTurnContext implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Outbound call to the CS agent (personal agent only); a stub in tests, A2A in production. */
  @FunctionalInterface
  public interface CustomerServiceClient extends Serializable {
    String ask(String contextId, String message);
  }

  private final String contextId;
  private final RoutingBudget budget;
  private final long nowEpochMs;
  private final CustomerServiceClient cs; // nullable (CS agent has no peer)

  public BankingTurnContext(
      String contextId, RoutingBudget budget, long nowEpochMs, CustomerServiceClient cs) {
    this.contextId = contextId;
    this.budget = budget;
    this.nowEpochMs = nowEpochMs;
    this.cs = cs;
  }

  public String contextId() {
    return contextId;
  }

  public RoutingBudget budget() {
    return budget;
  }

  /** True while the turn is still within its soft per-turn deadline. */
  public boolean withinDeadline() {
    return budget.withinDeadline(nowEpochMs);
  }

  /** True once any routing gate (round-trips / iterations / deadline) has been hit this turn. */
  public boolean budgetExhausted() {
    return !budget.withinDeadline(nowEpochMs)
        || budget.iterationsUsed() >= budget.maxIterations()
        || budget.roundTripsUsed() >= budget.maxRoundTrips();
  }

  /**
   * Delegate to the bank's customer service, consuming one routing-budget round-trip. Returns a
   * bounded-out marker when the budget is exhausted instead of making the call — this is the hard
   * stop on a personal↔CS ping-pong loop.
   */
  public String askCustomerService(String message) {
    if (cs == null) {
      return "[no customer-service peer available]";
    }
    if (!budget.allowRoundTrip()) {
      return "[customer-service round-trip budget reached: " + budget.lastDenial() + "]";
    }
    return cs.ask(contextId, message);
  }
}
