package org.agentic.flink.example.triage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The showcase builds its agent through the legacy DSL, whose default state machine has no
 * transition out of INITIALIZED and fails {@code AgentStateMachine.validate()}. The example
 * therefore supplies its own machine; these checks pin the shape the example relies on.
 */
class SupportTriageExampleTest {

  @Test
  @DisplayName("The triage state machine validates and starts from INITIALIZED")
  void stateMachineValidates() {
    AgentStateMachine sm = SupportTriageExample.triageStateMachine();
    sm.validate();
    assertEquals(AgentState.INITIALIZED, sm.getInitialState());
    List<AgentTransition> fromInitial = sm.getTransitionsFrom(AgentState.INITIALIZED);
    assertFalse(fromInitial.isEmpty());
    assertTrue(
        fromInitial.stream()
            .anyMatch(
                t ->
                    t.getToState() == AgentState.EXECUTING
                        && t.getTriggerEvent() == AgentEventType.FLOW_STARTED));
  }

  @Test
  @DisplayName("Every non-terminal state can leave and execution can complete")
  void everyNonTerminalStateHasAnExit() {
    AgentStateMachine sm = SupportTriageExample.triageStateMachine();
    for (AgentState state : AgentState.values()) {
      if (!state.isTerminal()) {
        assertFalse(sm.getTransitionsFrom(state).isEmpty(), state + " has no exit");
      }
    }
    assertTrue(
        sm.getTransitionsFrom(AgentState.EXECUTING).stream()
            .anyMatch(
                t ->
                    t.getToState() == AgentState.COMPLETED
                        && t.getTriggerEvent() == AgentEventType.FLOW_COMPLETED));
  }
}
