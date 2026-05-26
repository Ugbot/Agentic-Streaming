package org.agentic.flink.statemachine;

/**
 * Agent execution states in the CEP-based state machine.
 *
 * <p>This enum defines all possible states an agent can be in during its lifecycle. The state
 * machine is implemented using Apache Flink CEP patterns, allowing for complex event-driven
 * transitions and timeout handling.
 *
 * <p><b>State Flow (Happy Path):</b>
 * <pre>
 * INITIALIZED → VALIDATING → EXECUTING → SUPERVISOR_REVIEW → COMPLETED
 * </pre>
 *
 * <p><b>State Flow (With Corrections):</b>
 * <pre>
 * INITIALIZED → VALIDATING → EXECUTING → VALIDATING (failed) → CORRECTING → EXECUTING → COMPLETED
 * </pre>
 *
 * <p><b>State Flow (With Compensation):</b>
 * <pre>
 * EXECUTING → FAILED → COMPENSATING → COMPENSATED
 * </pre>
 *
 * <p>Each state corresponds to specific AgentEventType values and can have:
 * <ul>
 *   <li>Entry actions (executed when entering the state)</li>
 *   <li>Exit actions (executed when leaving the state)</li>
 *   <li>Timeout conditions (automatic transitions after duration)</li>
 *   <li>Validation predicates (must be satisfied to enter)</li>
 * </ul>
 *
 * @author Agentic Flink Team
 * @see AgentTransition
 * @see AgentStateMachine
 */
public enum AgentState {
  /**
   * Initial state when an agent flow is created.
   *
   * <p>Entry conditions: FlowStarted event received
   *
   * <p>Typical duration: Instantaneous
   *
   * <p>Next states: VALIDATING, EXECUTING (if no validation required)
   */
  INITIALIZED("initialized", "Agent flow has been initialized and is ready for processing"),

  /**
   * Agent is validating input or previous step results.
   *
   * <p>Entry conditions: VALIDATION_REQUESTED event
   *
   * <p>Activities:
   * <ul>
   *   <li>LLM-based validation of inputs</li>
   *   <li>Schema validation</li>
   *   <li>Business rule checking</li>
   *   <li>Security checks</li>
   * </ul>
   *
   * <p>Typical duration: 1-5 seconds
   *
   * <p>Next states: EXECUTING (if valid), CORRECTING (if invalid), FAILED (if max attempts)
   */
  VALIDATING(
      "validating",
      "Agent is validating input or results using LLM or predefined validators"),

  /**
   * Agent is executing its primary task (tool calls, LLM generation, etc.).
   *
   * <p>Entry conditions: LOOP_ITERATION_STARTED or VALIDATION_PASSED event
   *
   * <p>Activities:
   * <ul>
   *   <li>Tool execution (async, with retries)</li>
   *   <li>LLM generation</li>
   *   <li>External API calls</li>
   *   <li>Data processing</li>
   * </ul>
   *
   * <p>Typical duration: 5-30 seconds (depends on tool complexity)
   *
   * <p>Next states: VALIDATING, SUPERVISOR_REVIEW, COMPLETED, FAILED, COMPENSATING
   */
  EXECUTING("executing", "Agent is executing tools, making LLM calls, or processing data"),

  /**
   * Agent is correcting a previous failed validation attempt.
   *
   * <p>Entry conditions: CORRECTION_REQUESTED event
   *
   * <p>Activities:
   * <ul>
   *   <li>LLM generates corrected output based on validation feedback</li>
   *   <li>Applies transformation rules</li>
   *   <li>Re-validates internally</li>
   * </ul>
   *
   * <p>Typical duration: 3-10 seconds
   *
   * <p>Max attempts: Configurable (default 2-3)
   *
   * <p>Next states: EXECUTING (retry with correction), FAILED (max attempts exceeded)
   */
  CORRECTING(
      "correcting", "Agent is attempting to correct a failed validation using LLM feedback"),

  /**
   * Agent result is under supervisor review.
   *
   * <p>Entry conditions: SUPERVISOR_REVIEW_REQUESTED event
   *
   * <p>Activities:
   * <ul>
   *   <li>Supervisor agent validates quality</li>
   *   <li>Security/compliance review</li>
   *   <li>Manual approval (if configured)</li>
   *   <li>Escalation to higher tier</li>
   * </ul>
   *
   * <p>Typical duration: Variable (1 second to hours for manual review)
   *
   * <p>Next states: COMPLETED (approved), CORRECTING (rejected), FAILED (rejected without retry)
   */
  SUPERVISOR_REVIEW(
      "supervisor_review",
      "Agent result is being reviewed by a supervisor agent or manual approver"),

  /**
   * Agent has completed successfully.
   *
   * <p>Entry conditions: FLOW_COMPLETED event or SUPERVISOR_APPROVED
   *
   * <p>Terminal state (no outgoing transitions)
   *
   * <p>Activities:
   * <ul>
   *   <li>Emit final result</li>
   *   <li>Update metrics</li>
   *   <li>Archive to cold storage</li>
   *   <li>Trigger completion events</li>
   * </ul>
   */
  COMPLETED("completed", "Agent has successfully completed all tasks and validations"),

  /**
   * Agent execution has failed and cannot recover.
   *
   * <p>Entry conditions: FLOW_FAILED, MAX_ITERATIONS_REACHED, timeout, or unrecoverable error
   *
   * <p>Terminal state (unless compensation is configured)
   *
   * <p>Activities:
   * <ul>
   *   <li>Log error details</li>
   *   <li>Update failure metrics</li>
   *   <li>Trigger alerts</li>
   *   <li>May transition to COMPENSATING</li>
   * </ul>
   *
   * <p>Next states: COMPENSATING (if compensation enabled), none (terminal)
   */
  FAILED("failed", "Agent execution failed and cannot recover without manual intervention"),

  /**
   * Agent is performing compensation/rollback actions.
   *
   * <p>Entry conditions: FAILED state + compensation policy enabled, or explicit COMPENSATION
   * event
   *
   * <p>Activities:
   * <ul>
   *   <li>Reverse tool executions (where possible)</li>
   *   <li>Undo state changes</li>
   *   <li>Send compensation events</li>
   *   <li>Restore previous consistent state</li>
   * </ul>
   *
   * <p>Borrowed from Saga pattern for distributed transaction rollback
   *
   * <p>Typical duration: 1-10 seconds
   *
   * <p>Next states: COMPENSATED (success), FAILED (compensation failed)
   */
  COMPENSATING(
      "compensating", "Agent is performing compensation/rollback actions (Saga pattern)"),

  /**
   * Agent has successfully completed compensation.
   *
   * <p>Entry conditions: COMPENSATING state completed successfully
   *
   * <p>Terminal state
   *
   * <p>Indicates that although the primary execution failed, all side effects have been cleanly
   * rolled back.
   */
  COMPENSATED(
      "compensated", "Agent has successfully rolled back all changes due to failure (Saga pattern)"),

  /**
   * Agent execution is paused, waiting for external event.
   *
   * <p>Entry conditions: FLOW_PAUSED event or explicit pause command
   *
   * <p>Use cases:
   * <ul>
   *   <li>Human-in-the-loop workflows</li>
   *   <li>Waiting for external system</li>
   *   <li>Rate limiting</li>
   *   <li>Debug/inspection</li>
   * </ul>
   *
   * <p>Non-terminal (can resume)
   *
   * <p>Next states: Previous state (when FLOW_RESUMED), FAILED (if timeout)
   */
  PAUSED("paused", "Agent execution is paused, waiting for external event or manual resume"),

  /**
   * Agent is offloading state to warm/cold storage.
   *
   * <p>Entry conditions: STATE_OFFLOAD_TRIGGERED event
   *
   * <p>Activities:
   * <ul>
   *   <li>Move context from hot (Flink) to warm (Redis) storage</li>
   *   <li>Archive conversation to cold (PostgreSQL) storage</li>
   *   <li>Compress large payloads</li>
   *   <li>Update state tier metadata</li>
   * </ul>
   *
   * <p>Typical duration: 50-200ms
   *
   * <p>Next states: Previous state (after offload complete)
   */
  OFFLOADING(
      "offloading",
      "Agent is offloading state to warm (Redis) or cold (PostgreSQL) storage tiers");

  private final String stateId;
  private final String description;

  AgentState(String stateId, String description) {
    this.stateId = stateId;
    this.description = description;
  }

  /** Returns the unique identifier for this state (lowercase, suitable for logging/metrics). */
  public String getStateId() {
    return stateId;
  }

  /** Returns a human-readable description of what happens in this state. */
  public String getDescription() {
    return description;
  }

  /**
   * Checks if this is a terminal state (no outgoing transitions).
   *
   * @return true if COMPLETED, FAILED, or COMPENSATED
   */
  public boolean isTerminal() {
    return this == COMPLETED || this == FAILED || this == COMPENSATED;
  }

  /**
   * Checks if this state requires external interaction.
   *
   * @return true if SUPERVISOR_REVIEW (may require manual approval) or PAUSED
   */
  public boolean requiresExternalInteraction() {
    return this == SUPERVISOR_REVIEW || this == PAUSED;
  }

  /**
   * Checks if this state involves LLM calls.
   *
   * @return true if VALIDATING, EXECUTING, or CORRECTING (typical LLM states)
   */
  public boolean involvesLlmCalls() {
    return this == VALIDATING || this == EXECUTING || this == CORRECTING;
  }

  /**
   * Returns the typical timeout duration for this state in seconds.
   *
   * <p>Used by CEP patterns to set `.within(Time.seconds(timeout))`.
   *
   * @return timeout in seconds, or 0 if no timeout
   */
  public int getTypicalTimeoutSeconds() {
    switch (this) {
      case INITIALIZED:
        return 5; // Should transition quickly
      case VALIDATING:
        return 10; // LLM validation
      case EXECUTING:
        return 30; // Tool execution may take time
      case CORRECTING:
        return 15; // LLM correction
      case SUPERVISOR_REVIEW:
        return 300; // 5 minutes for automated review, manual may be longer
      case COMPENSATING:
        return 10; // Rollback operations
      case OFFLOADING:
        return 5; // Storage operations
      case PAUSED:
        return 0; // No timeout (wait indefinitely)
      case COMPLETED:
      case FAILED:
      case COMPENSATED:
        return 0; // Terminal states have no timeout
      default:
        return 30; // Default fallback
    }
  }

  @Override
  public String toString() {
    return stateId;
  }
}
