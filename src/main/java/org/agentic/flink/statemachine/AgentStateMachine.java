package org.agentic.flink.statemachine;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.time.Duration;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * Defines the complete agent state machine with all states and valid transitions.
 *
 * <p>This class serves as the central definition of agent lifecycle behavior. It:
 * <ul>
 *   <li>Defines all valid state transitions</li>
 *   <li>Provides state validation and transition checking</li>
 *   <li>Generates Apache Flink CEP patterns for state machine implementation</li>
 *   <li>Handles timeout conditions and terminal states</li>
 * </ul>
 *
 * <p><b>Design Pattern:</b> This implements a declarative state machine where states and
 * transitions are defined upfront, then compiled into Flink CEP patterns for distributed execution.
 * This approach borrows from the Saga kit's pattern-based orchestration.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * // Create a state machine with standard transitions
 * AgentStateMachine stateMachine = AgentStateMachine.builder()
 *     .withStandardTransitions()
 *     .withMaxValidationAttempts(3)
 *     .withMaxCorrectionAttempts(2)
 *     .withCompensationEnabled(true)
 *     .build();
 *
 * // Generate CEP pattern for a specific flow
 * Pattern<AgentEvent, ?> pattern = stateMachine.generateCepPattern();
 *
 * // Check if a transition is valid
 * boolean canTransition = stateMachine.canTransition(
 *     AgentState.VALIDATING,
 *     AgentEventType.VALIDATION_PASSED
 * );
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see AgentState
 * @see AgentTransition
 */
public class AgentStateMachine implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String stateMachineId;
  private final AgentState initialState;
  private final Set<AgentState> terminalStates;
  private final List<AgentTransition> transitions;
  private final Map<AgentState, List<AgentTransition>> transitionsByState;
  private final int globalTimeoutSeconds;
  private final boolean enableCompensation;

  private AgentStateMachine(Builder builder) {
    this.stateMachineId = builder.stateMachineId;
    this.initialState = builder.initialState;
    this.terminalStates = Collections.unmodifiableSet(builder.terminalStates);
    this.transitions = Collections.unmodifiableList(builder.transitions);
    this.globalTimeoutSeconds = builder.globalTimeoutSeconds;
    this.enableCompensation = builder.enableCompensation;

    // Index transitions by source state for fast lookup
    this.transitionsByState = new HashMap<>();
    for (AgentTransition transition : transitions) {
      transitionsByState
          .computeIfAbsent(transition.getFromState(), k -> new ArrayList<>())
          .add(transition);
    }

    // Sort transitions by priority (descending)
    transitionsByState.values().forEach(list -> list.sort((a, b) ->
        Integer.compare(b.getPriority(), a.getPriority())));
  }

  public String getStateMachineId() {
    return stateMachineId;
  }

  public AgentState getInitialState() {
    return initialState;
  }

  public Set<AgentState> getTerminalStates() {
    return terminalStates;
  }

  public List<AgentTransition> getTransitions() {
    return transitions;
  }

  /**
   * Gets all transitions from a specific state.
   *
   * @param state The source state
   * @return List of transitions (sorted by priority, descending)
   */
  public List<AgentTransition> getTransitionsFrom(AgentState state) {
    return transitionsByState.getOrDefault(state, Collections.emptyList());
  }

  /**
   * Checks if a transition is valid from the current state given an event.
   *
   * @param currentState The current agent state
   * @param event The event that may trigger a transition
   * @return true if a valid transition exists
   */
  public boolean canTransition(AgentState currentState, AgentEvent event) {
    List<AgentTransition> possibleTransitions = getTransitionsFrom(currentState);
    return possibleTransitions.stream().anyMatch(t -> t.canTransition(event));
  }

  /**
   * Finds the next state given current state and event.
   *
   * @param currentState The current agent state
   * @param event The event that triggers the transition
   * @return Optional containing the next state, or empty if no valid transition
   */
  public Optional<AgentState> getNextState(AgentState currentState, AgentEvent event) {
    List<AgentTransition> possibleTransitions = getTransitionsFrom(currentState);

    // Find first matching transition (already sorted by priority)
    return possibleTransitions.stream()
        .filter(t -> t.canTransition(event))
        .map(AgentTransition::getToState)
        .findFirst();
  }

  /**
   * Executes transition actions if a valid transition exists.
   *
   * @param currentState The current agent state
   * @param event The event that triggers the transition
   * @return true if a transition was executed
   */
  public boolean executeTransition(AgentState currentState, AgentEvent event) {
    List<AgentTransition> possibleTransitions = getTransitionsFrom(currentState);

    Optional<AgentTransition> matchingTransition = possibleTransitions.stream()
        .filter(t -> t.canTransition(event))
        .findFirst();

    if (matchingTransition.isPresent()) {
      matchingTransition.get().executeAction(event);
      return true;
    }
    return false;
  }

  /**
   * Generates an Apache Flink CEP pattern from this state machine.
   *
   * <p>This creates a pattern that matches the sequence of events corresponding to valid state
   * transitions, similar to the saga kit's CEP pattern generation.
   *
   * <p><b>Pattern Structure:</b>
   * <pre>
   * INITIALIZED → VALIDATING → EXECUTING → SUPERVISOR_REVIEW → COMPLETED
   * (with optional loops back for corrections)
   * </pre>
   *
   * @return CEP Pattern for this state machine
   */
  public Pattern<AgentEvent, ?> generateCepPattern() {
    // Start with the initial state
    Pattern<AgentEvent, ?> pattern = Pattern.<AgentEvent>begin("initial")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) throws Exception {
            return event.getEventType() == AgentEventType.FLOW_STARTED;
          }
        });

    // Add validation step (optional)
    pattern = pattern
        .followedBy("validating")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) throws Exception {
            return event.getEventType() == AgentEventType.VALIDATION_REQUESTED
                || event.getEventType() == AgentEventType.VALIDATION_PASSED
                || event.getEventType() == AgentEventType.VALIDATION_FAILED;
          }
        })
        .optional();

    // Add execution step (required)
    pattern = pattern
        .followedBy("executing")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) throws Exception {
            return event.getEventType() == AgentEventType.LOOP_ITERATION_STARTED
                || event.getEventType() == AgentEventType.TOOL_CALL_REQUESTED
                || event.getEventType() == AgentEventType.TOOL_CALL_COMPLETED
                || event.getEventType() == AgentEventType.TOOL_CALL_FAILED;
          }
        })
        .oneOrMore()
        .greedy();

    // Add correction step (optional, may repeat)
    pattern = pattern
        .followedBy("correcting")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) throws Exception {
            return event.getEventType() == AgentEventType.CORRECTION_REQUESTED
                || event.getEventType() == AgentEventType.CORRECTION_COMPLETED
                || event.getEventType() == AgentEventType.CORRECTION_FAILED;
          }
        })
        .optional()
        .oneOrMore();

    // Add supervisor review step (optional)
    pattern = pattern
        .followedBy("supervisor_review")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) throws Exception {
            return event.getEventType() == AgentEventType.SUPERVISOR_REVIEW_REQUESTED
                || event.getEventType() == AgentEventType.SUPERVISOR_APPROVED
                || event.getEventType() == AgentEventType.SUPERVISOR_REJECTED;
          }
        })
        .optional();

    // Add terminal step (completion or failure)
    pattern = pattern
        .followedBy("terminal")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) throws Exception {
            return event.getEventType() == AgentEventType.FLOW_COMPLETED
                || event.getEventType() == AgentEventType.FLOW_FAILED
                || event.getEventType() == AgentEventType.LOOP_MAX_ITERATIONS_REACHED;
          }
        });

    // Apply global timeout if configured
    if (globalTimeoutSeconds > 0) {
      pattern = pattern.within(Duration.ofSeconds(globalTimeoutSeconds));
    }

    return pattern;
  }

  /**
   * Generates a simpler CEP pattern for a specific agent flow path.
   *
   * <p>This creates a more targeted pattern based on agent configuration (e.g., skip validation if
   * not enabled).
   *
   * @param enableValidation Whether to include validation steps
   * @param enableSupervisorReview Whether to include supervisor review
   * @param enableCompensation Whether to include compensation handling
   * @return Customized CEP pattern
   */
  public Pattern<AgentEvent, ?> generateCustomCepPattern(
      boolean enableValidation,
      boolean enableSupervisorReview,
      boolean enableCompensation) {

    // Start pattern
    Pattern<AgentEvent, ?> pattern = Pattern.<AgentEvent>begin("initial")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) {
            return event.getEventType() == AgentEventType.FLOW_STARTED;
          }
        });

    // Conditional validation
    if (enableValidation) {
      pattern = pattern
          .followedBy("validation")
          .where(new SimpleCondition<AgentEvent>() {
            @Override
            public boolean filter(AgentEvent event) {
              return event.getEventType() == AgentEventType.VALIDATION_PASSED
                  || event.getEventType() == AgentEventType.VALIDATION_FAILED;
            }
          });
    }

    // Required execution
    pattern = pattern
        .followedBy("execution")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) {
            return event.getEventType() == AgentEventType.TOOL_CALL_COMPLETED
                || event.getEventType() == AgentEventType.TOOL_CALL_FAILED;
          }
        });

    // Conditional supervisor review
    if (enableSupervisorReview) {
      pattern = pattern
          .followedBy("supervisor")
          .where(new SimpleCondition<AgentEvent>() {
            @Override
            public boolean filter(AgentEvent event) {
              return event.getEventType() == AgentEventType.SUPERVISOR_APPROVED
                  || event.getEventType() == AgentEventType.SUPERVISOR_REJECTED;
            }
          });
    }

    // Terminal state
    pattern = pattern
        .followedBy("terminal")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) {
            return event.getEventType() == AgentEventType.FLOW_COMPLETED
                || event.getEventType() == AgentEventType.FLOW_FAILED;
          }
        });

    // Optional compensation on failure
    if (enableCompensation) {
      // This would be a separate pattern matched on the side output
      // The main pattern completes, then compensation pattern activates
    }

    if (globalTimeoutSeconds > 0) {
      pattern = pattern.within(Duration.ofSeconds(globalTimeoutSeconds));
    }

    return pattern;
  }

  /**
   * Validates that the state machine is well-formed.
   *
   * @throws IllegalStateException if the state machine has issues
   */
  public void validate() {
    // Check that initial state has outgoing transitions
    if (getTransitionsFrom(initialState).isEmpty()) {
      throw new IllegalStateException("Initial state has no outgoing transitions");
    }

    // Check that all non-terminal states have at least one outgoing transition
    for (AgentState state : AgentState.values()) {
      if (!state.isTerminal() && getTransitionsFrom(state).isEmpty()) {
        throw new IllegalStateException("Non-terminal state " + state + " has no outgoing transitions");
      }
    }

    // Check that terminal states have no outgoing transitions
    for (AgentState terminalState : terminalStates) {
      if (!getTransitionsFrom(terminalState).isEmpty()) {
        throw new IllegalStateException("Terminal state " + terminalState + " has outgoing transitions");
      }
    }

    // Check for unreachable states (basic reachability)
    Set<AgentState> reachable = new HashSet<>();
    reachable.add(initialState);
    boolean changed = true;
    while (changed) {
      changed = false;
      Set<AgentState> newReachable = new HashSet<>(reachable);
      for (AgentState state : reachable) {
        for (AgentTransition transition : getTransitionsFrom(state)) {
          if (newReachable.add(transition.getToState())) {
            changed = true;
          }
        }
      }
      reachable = newReachable;
    }

    // Warn about unreachable states (not an error, just informational)
    for (AgentState state : AgentState.values()) {
      if (!reachable.contains(state) && !state.isTerminal()) {
        System.out.println("Warning: State " + state + " is not reachable in state machine " + stateMachineId);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating AgentStateMachine instances. */
  public static class Builder {
    private String stateMachineId = "default-state-machine";
    private AgentState initialState = AgentState.INITIALIZED;
    private Set<AgentState> terminalStates = new HashSet<>(Arrays.asList(
        AgentState.COMPLETED, AgentState.FAILED, AgentState.COMPENSATED));
    private List<AgentTransition> transitions = new ArrayList<>();
    private int globalTimeoutSeconds = 300; // 5 minutes default
    private boolean enableCompensation = false;
    private int maxValidationAttempts = 3;
    private int maxCorrectionAttempts = 2;

    public Builder withId(String stateMachineId) {
      this.stateMachineId = stateMachineId;
      return this;
    }

    public Builder withInitialState(AgentState initialState) {
      this.initialState = initialState;
      return this;
    }

    public Builder withTerminalStates(AgentState... states) {
      this.terminalStates = new HashSet<>(Arrays.asList(states));
      return this;
    }

    public Builder withGlobalTimeout(int seconds) {
      this.globalTimeoutSeconds = seconds;
      return this;
    }

    public Builder withCompensationEnabled(boolean enabled) {
      this.enableCompensation = enabled;
      return this;
    }

    public Builder withMaxValidationAttempts(int attempts) {
      this.maxValidationAttempts = attempts;
      return this;
    }

    public Builder withMaxCorrectionAttempts(int attempts) {
      this.maxCorrectionAttempts = attempts;
      return this;
    }

    public Builder addTransition(AgentTransition transition) {
      this.transitions.add(transition);
      return this;
    }

    /**
     * Adds all standard transitions for a typical agent workflow.
     *
     * <p>Includes:
     * <ul>
     *   <li>Validation pass/fail transitions</li>
     *   <li>Correction transitions</li>
     *   <li>Execution completion</li>
     *   <li>Supervisor approval/rejection</li>
     *   <li>Compensation (if enabled)</li>
     * </ul>
     */
    public Builder withStandardTransitions() {
      // Validation transitions
      addTransition(AgentTransition.validationPassed());
      addTransition(AgentTransition.validationFailedWithRetry(maxValidationAttempts));
      addTransition(AgentTransition.validationFailedFinal(maxValidationAttempts));

      // Correction transitions
      addTransition(AgentTransition.correctionCompleted());

      // Execution transitions
      addTransition(AgentTransition.executionCompleteWithReview());
      addTransition(AgentTransition.executionCompleteDirect());

      // Supervisor transitions
      addTransition(AgentTransition.supervisorApproved());
      addTransition(AgentTransition.supervisorRejectedWithRetry(maxCorrectionAttempts));
      addTransition(AgentTransition.supervisorRejectedFinal(maxCorrectionAttempts));

      // Compensation transitions (if enabled)
      if (enableCompensation) {
        addTransition(AgentTransition.failureWithCompensation());
        addTransition(AgentTransition.compensationCompleted());
      }

      return this;
    }

    public AgentStateMachine build() {
      AgentStateMachine stateMachine = new AgentStateMachine(this);
      stateMachine.validate(); // Validate on construction
      return stateMachine;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "AgentStateMachine[id=%s, initial=%s, transitions=%d, timeout=%ds]",
        stateMachineId, initialState, transitions.size(), globalTimeoutSeconds);
  }
}
