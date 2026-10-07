package org.agentic.flink.statemachine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.junit.jupiter.api.Test;

/** The standard transitions produce a state machine that validates and routes every flow event. */
class AgentStateMachineTest {

  private static AgentEvent event(AgentEventType type) {
    return new AgentEvent("f-" + UUID.randomUUID(), "u", "a", type);
  }

  private static AgentEvent event(AgentEventType type, String key, Object value) {
    AgentEvent e = event(type);
    e.putData(key, value);
    return e;
  }

  private static AgentStateMachine standard(boolean compensation) {
    return AgentStateMachine.builder()
        .withId("sm-" + UUID.randomUUID())
        .withCompensationEnabled(compensation)
        .withStandardTransitions()
        .build();
  }

  @Test
  void standardTransitionsValidateWithAndWithoutCompensation() {
    for (boolean compensation : new boolean[] {false, true}) {
      AgentStateMachine sm = standard(compensation);
      sm.validate();
      for (AgentState state : AgentState.values()) {
        if (state.isTerminal()) {
          assertTrue(sm.getTransitionsFrom(state).isEmpty(), state + " is terminal");
        } else {
          assertFalse(sm.getTransitionsFrom(state).isEmpty(), state + " needs a way out");
        }
      }
      assertNotNull(sm.generateCepPattern());
    }
  }

  @Test
  void happyPathRoutesThroughExecutionToCompleted() {
    AgentStateMachine sm = standard(false);
    assertEquals(
        Optional.of(AgentState.EXECUTING),
        sm.getNextState(AgentState.INITIALIZED, event(AgentEventType.FLOW_STARTED)));
    assertEquals(
        Optional.of(AgentState.PAUSED),
        sm.getNextState(AgentState.EXECUTING, event(AgentEventType.FLOW_PAUSED)));
    assertEquals(
        Optional.of(AgentState.EXECUTING),
        sm.getNextState(AgentState.PAUSED, event(AgentEventType.FLOW_RESUMED)));
    assertEquals(
        Optional.of(AgentState.OFFLOADING),
        sm.getNextState(AgentState.EXECUTING, event(AgentEventType.STATE_OFFLOAD_TRIGGERED)));
    assertEquals(
        Optional.of(AgentState.EXECUTING),
        sm.getNextState(AgentState.OFFLOADING, event(AgentEventType.STATE_OFFLOADED)));
    assertEquals(
        Optional.of(AgentState.VALIDATING),
        sm.getNextState(AgentState.EXECUTING, event(AgentEventType.VALIDATION_REQUESTED)));
    assertEquals(
        Optional.of(AgentState.EXECUTING),
        sm.getNextState(AgentState.VALIDATING, event(AgentEventType.VALIDATION_PASSED)));
    assertEquals(
        Optional.of(AgentState.COMPLETED),
        sm.getNextState(AgentState.EXECUTING, event(AgentEventType.FLOW_COMPLETED)));
    assertEquals(
        Optional.empty(),
        sm.getNextState(AgentState.INITIALIZED, event(AgentEventType.FLOW_COMPLETED)));
  }

  @Test
  void failureGoesToFailedOrCompensatingDependingOnTheFlag() {
    AgentEvent failed = event(AgentEventType.FLOW_FAILED);
    assertEquals(
        Optional.of(AgentState.FAILED), standard(false).getNextState(AgentState.EXECUTING, failed));

    AgentStateMachine saga = standard(true);
    assertEquals(
        Optional.of(AgentState.COMPENSATING), saga.getNextState(AgentState.EXECUTING, failed));
    assertEquals(
        Optional.of(AgentState.COMPENSATED),
        saga.getNextState(AgentState.COMPENSATING, event(AgentEventType.FLOW_COMPENSATED)));
    assertEquals(
        Optional.of(AgentState.COMPENSATED),
        saga.getNextState(AgentState.COMPENSATING, event(AgentEventType.COMPENSATION_COMPLETED)));
  }

  @Test
  void retryBudgetsAreSnapshottedIntoTheTransitions() {
    int validation = ThreadLocalRandom.current().nextInt(1, 6);
    int correction = ThreadLocalRandom.current().nextInt(1, 6);
    AgentStateMachine sm =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withMaxValidationAttempts(validation)
            .withMaxCorrectionAttempts(correction)
            .withStandardTransitions()
            .build();
    assertEquals(
        Optional.of(AgentState.CORRECTING),
        sm.getNextState(
            AgentState.VALIDATING,
            event(AgentEventType.VALIDATION_FAILED, "validation_attempts", validation - 1)));
    assertEquals(
        Optional.of(AgentState.FAILED),
        sm.getNextState(
            AgentState.VALIDATING,
            event(AgentEventType.VALIDATION_FAILED, "validation_attempts", validation)));
    assertEquals(
        Optional.of(AgentState.CORRECTING),
        sm.getNextState(
            AgentState.SUPERVISOR_REVIEW,
            event(AgentEventType.SUPERVISOR_REJECTED, "correction_attempts", correction - 1)));
    assertEquals(
        Optional.of(AgentState.FAILED),
        sm.getNextState(
            AgentState.SUPERVISOR_REVIEW,
            event(AgentEventType.SUPERVISOR_REJECTED, "correction_attempts", correction)));
  }

  @Test
  void machinesWithoutAnExitFromTheInitialStateAreRejected() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("bad")
            .addTransition(AgentTransition.executionCompleteDirect());
    IllegalStateException e = assertThrows(IllegalStateException.class, b::build);
    assertTrue(e.getMessage().contains("Initial state"), e.getMessage());
  }
}
