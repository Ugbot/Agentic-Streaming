package org.agentic.flink.statemachine;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a valid state transition in the agent state machine.
 *
 * <p>Each transition defines:
 * <ul>
 *   <li>Source state (where the transition starts)</li>
 *   <li>Target state (where the transition ends)</li>
 *   <li>Trigger event type (what causes the transition)</li>
 *   <li>Condition predicate (optional guard condition)</li>
 *   <li>Action (optional side effect to execute during transition)</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * AgentTransition transition = AgentTransition.builder()
 *     .from(AgentState.VALIDATING)
 *     .to(AgentState.EXECUTING)
 *     .on(AgentEventType.VALIDATION_PASSED)
 *     .when(event -> event.getData().get("validation_score") > 0.8)
 *     .action(event -> log.info("Validation passed, starting execution"))
 *     .build();
 * }</pre>
 *
 * <p>Transitions are immutable and thread-safe.
 *
 * @author Agentic Flink Team
 * @see AgentState
 * @see AgentStateMachine
 */
public class AgentTransition implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Serializable predicate for guard conditions on transitions.
   *
   * <p>Plain {@code java.util.function.Predicate} does not extend {@code Serializable},
   * which causes {@code NotSerializableException} when Flink serializes the function graph.
   * This interface combines both contracts so lambda conditions survive serialization.
   */
  @FunctionalInterface
  public interface SerializablePredicate<T> extends Serializable {
    boolean test(T value);
  }

  private static final SerializablePredicate<AgentEvent> ALWAYS_TRUE = event -> true;

  private final AgentState fromState;
  private final AgentState toState;
  private final AgentEventType triggerEvent;
  private final SerializablePredicate<AgentEvent> condition;
  private final TransitionAction action;
  private final String description;
  private final int priority; // Higher priority transitions checked first

  private AgentTransition(Builder builder) {
    this.fromState = Objects.requireNonNull(builder.fromState, "fromState cannot be null");
    this.toState = Objects.requireNonNull(builder.toState, "toState cannot be null");
    this.triggerEvent =
        Objects.requireNonNull(builder.triggerEvent, "triggerEvent cannot be null");
    this.condition = builder.condition != null ? builder.condition : ALWAYS_TRUE;
    this.action = builder.action;
    this.description = builder.description;
    this.priority = builder.priority;
  }

  public AgentState getFromState() {
    return fromState;
  }

  public AgentState getToState() {
    return toState;
  }

  public AgentEventType getTriggerEvent() {
    return triggerEvent;
  }

  public String getDescription() {
    return description;
  }

  public int getPriority() {
    return priority;
  }

  /**
   * Checks if this transition can be taken given the current event.
   *
   * @param event The agent event that may trigger this transition
   * @return true if the event type matches and condition is satisfied
   */
  public boolean canTransition(AgentEvent event) {
    if (event == null || event.getEventType() != triggerEvent) {
      return false;
    }
    try {
      return condition.test(event);
    } catch (Exception e) {
      // Guard against condition failures
      return false;
    }
  }

  /**
   * Executes the transition action (side effect) if configured.
   *
   * @param event The event that triggered the transition
   */
  public void executeAction(AgentEvent event) {
    if (action != null) {
      try {
        action.execute(event);
      } catch (Exception e) {
        // Log but don't fail the transition
        System.err.println("Transition action failed: " + e.getMessage());
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Functional interface for transition actions (side effects). */
  @FunctionalInterface
  public interface TransitionAction extends Serializable {
    void execute(AgentEvent event);
  }

  /** Builder for creating immutable AgentTransition instances. */
  public static class Builder {
    private AgentState fromState;
    private AgentState toState;
    private AgentEventType triggerEvent;
    private SerializablePredicate<AgentEvent> condition;
    private TransitionAction action;
    private String description;
    private int priority = 0;

    /**
     * Sets the source state for this transition.
     *
     * @param fromState The state this transition starts from
     * @return this builder
     */
    public Builder from(AgentState fromState) {
      this.fromState = fromState;
      return this;
    }

    /**
     * Sets the target state for this transition.
     *
     * @param toState The state this transition goes to
     * @return this builder
     */
    public Builder to(AgentState toState) {
      this.toState = toState;
      return this;
    }

    /**
     * Sets the event type that triggers this transition.
     *
     * @param triggerEvent The AgentEventType that causes this transition
     * @return this builder
     */
    public Builder on(AgentEventType triggerEvent) {
      this.triggerEvent = triggerEvent;
      return this;
    }

    /**
     * Sets an optional guard condition that must be true for the transition to occur.
     *
     * <p>Example: <pre>{@code
     * .when(event -> event.getData().get("iteration") < maxIterations)
     * }</pre>
     *
     * @param condition Predicate that tests the event
     * @return this builder
     */
    public Builder when(SerializablePredicate<AgentEvent> condition) {
      this.condition = condition;
      return this;
    }

    /**
     * Sets an optional action to execute when the transition occurs.
     *
     * <p>Example: <pre>{@code
     * .action(event -> metricsCollector.recordTransition(fromState, toState))
     * }</pre>
     *
     * @param action Side effect to execute during transition
     * @return this builder
     */
    public Builder action(TransitionAction action) {
      this.action = action;
      return this;
    }

    /**
     * Sets a human-readable description of this transition.
     *
     * @param description What this transition represents
     * @return this builder
     */
    public Builder withDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the priority for this transition (higher = checked first).
     *
     * <p>Useful when multiple transitions from the same state could match the same event. Higher
     * priority transitions are evaluated first.
     *
     * @param priority Transition priority (default 0)
     * @return this builder
     */
    public Builder withPriority(int priority) {
      this.priority = priority;
      return this;
    }

    public AgentTransition build() {
      return new AgentTransition(this);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "Transition[%s -> %s on %s%s]",
        fromState, toState, triggerEvent, description != null ? " (" + description + ")" : "");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AgentTransition that = (AgentTransition) o;
    return fromState == that.fromState
        && toState == that.toState
        && triggerEvent == that.triggerEvent;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromState, toState, triggerEvent);
  }

  // ==================== Common Pre-Defined Transitions ====================

  /**
   * Creates a standard validation pass transition.
   *
   * <p>VALIDATING → EXECUTING on VALIDATION_PASSED
   */
  public static AgentTransition validationPassed() {
    return builder()
        .from(AgentState.VALIDATING)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.VALIDATION_PASSED)
        .withDescription("Validation passed, proceeding to execution")
        .build();
  }

  /**
   * Creates a standard validation fail transition (with correction).
   *
   * <p>VALIDATING → CORRECTING on VALIDATION_FAILED (with retry budget remaining)
   */
  public static AgentTransition validationFailedWithRetry(int maxAttempts) {
    return builder()
        .from(AgentState.VALIDATING)
        .to(AgentState.CORRECTING)
        .on(AgentEventType.VALIDATION_FAILED)
        .when(
            event -> {
              Integer attempts = (Integer) event.getData().getOrDefault("validation_attempts", 0);
              return attempts < maxAttempts;
            })
        .withDescription("Validation failed, attempting correction")
        .withPriority(10) // Higher priority than final failure
        .build();
  }

  /**
   * Creates a standard validation fail transition (final, no retry).
   *
   * <p>VALIDATING → FAILED on VALIDATION_FAILED (max attempts exceeded)
   */
  public static AgentTransition validationFailedFinal(int maxAttempts) {
    return builder()
        .from(AgentState.VALIDATING)
        .to(AgentState.FAILED)
        .on(AgentEventType.VALIDATION_FAILED)
        .when(
            event -> {
              Integer attempts = (Integer) event.getData().getOrDefault("validation_attempts", 0);
              return attempts >= maxAttempts;
            })
        .withDescription("Validation failed, max attempts exceeded")
        .withPriority(5) // Lower priority than retry transition
        .build();
  }

  /**
   * Creates a standard correction complete transition.
   *
   * <p>CORRECTING → EXECUTING on CORRECTION_COMPLETED
   */
  public static AgentTransition correctionCompleted() {
    return builder()
        .from(AgentState.CORRECTING)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.CORRECTION_COMPLETED)
        .withDescription("Correction completed, re-executing")
        .build();
  }

  /**
   * Creates a standard execution complete transition (with supervisor review).
   *
   * <p>EXECUTING → SUPERVISOR_REVIEW on TOOL_CALL_COMPLETED (when supervisor is configured)
   */
  public static AgentTransition executionCompleteWithReview() {
    return builder()
        .from(AgentState.EXECUTING)
        .to(AgentState.SUPERVISOR_REVIEW)
        .on(AgentEventType.TOOL_CALL_COMPLETED)
        .when(event -> event.getData().containsKey("requires_supervisor_review"))
        .withDescription("Execution complete, requesting supervisor review")
        .withPriority(10)
        .build();
  }

  /**
   * Creates a standard execution complete transition (direct to completion).
   *
   * <p>EXECUTING → COMPLETED on TOOL_CALL_COMPLETED (no supervisor required)
   */
  public static AgentTransition executionCompleteDirect() {
    return builder()
        .from(AgentState.EXECUTING)
        .to(AgentState.COMPLETED)
        .on(AgentEventType.FLOW_COMPLETED)
        .withDescription("Execution complete, no review required")
        .withPriority(5)
        .build();
  }

  /**
   * Creates a supervisor approval transition.
   *
   * <p>SUPERVISOR_REVIEW → COMPLETED on SUPERVISOR_APPROVED
   */
  public static AgentTransition supervisorApproved() {
    return builder()
        .from(AgentState.SUPERVISOR_REVIEW)
        .to(AgentState.COMPLETED)
        .on(AgentEventType.SUPERVISOR_APPROVED)
        .withDescription("Supervisor approved result")
        .build();
  }

  /**
   * Creates a supervisor rejection transition (with retry).
   *
   * <p>SUPERVISOR_REVIEW → CORRECTING on SUPERVISOR_REJECTED (with retry budget)
   */
  public static AgentTransition supervisorRejectedWithRetry(int maxAttempts) {
    return builder()
        .from(AgentState.SUPERVISOR_REVIEW)
        .to(AgentState.CORRECTING)
        .on(AgentEventType.SUPERVISOR_REJECTED)
        .when(
            event -> {
              Integer attempts = (Integer) event.getData().getOrDefault("correction_attempts", 0);
              return attempts < maxAttempts;
            })
        .withDescription("Supervisor rejected, attempting correction")
        .withPriority(10)
        .build();
  }

  /**
   * Creates a supervisor rejection transition (final).
   *
   * <p>SUPERVISOR_REVIEW → FAILED on SUPERVISOR_REJECTED (max attempts exceeded)
   */
  public static AgentTransition supervisorRejectedFinal(int maxAttempts) {
    return builder()
        .from(AgentState.SUPERVISOR_REVIEW)
        .to(AgentState.FAILED)
        .on(AgentEventType.SUPERVISOR_REJECTED)
        .when(
            event -> {
              Integer attempts = (Integer) event.getData().getOrDefault("correction_attempts", 0);
              return attempts >= maxAttempts;
            })
        .withDescription("Supervisor rejected, max attempts exceeded")
        .withPriority(5)
        .build();
  }

  /**
   * Creates a failure with compensation transition.
   *
   * <p>FAILED → COMPENSATING on any event (when compensation is enabled)
   */
  public static AgentTransition failureWithCompensation() {
    return builder()
        .from(AgentState.FAILED)
        .to(AgentState.COMPENSATING)
        .on(AgentEventType.FLOW_FAILED)
        .when(event -> Boolean.TRUE.equals(event.getData().get("enable_compensation")))
        .withDescription("Failure detected, starting compensation")
        .build();
  }

  /**
   * Creates a compensation complete transition.
   *
   * <p>COMPENSATING → COMPENSATED on COMPENSATION_COMPLETED
   */
  public static AgentTransition compensationCompleted() {
    return builder()
        .from(AgentState.COMPENSATING)
        .to(AgentState.COMPENSATED)
        .on(AgentEventType.FLOW_COMPLETED) // Reuse event type
        .withDescription("Compensation completed successfully")
        .build();
  }
}
