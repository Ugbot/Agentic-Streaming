package org.agentic.flink.a2a;

import java.util.UUID;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;

/** Shared helpers for A2A tests that need a valid {@link AgentStateMachine} to build an Agent. */
final class A2ATestSupport {

  private A2ATestSupport() {}

  /**
   * A state machine satisfying the framework validator: every non-terminal state has an outgoing
   * transition and every state is reachable from {@code INITIALIZED}. Mirrors the helper in {@code
   * skill/SkillTest}.
   */
  static AgentStateMachine minimalStateMachine() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withInitialState(AgentState.INITIALIZED);
    b.addTransition(t(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED));
    b.addTransition(t(AgentState.EXECUTING, AgentState.VALIDATING, AgentEventType.VALIDATION_REQUESTED));
    b.addTransition(t(AgentState.VALIDATING, AgentState.CORRECTING, AgentEventType.VALIDATION_FAILED));
    b.addTransition(t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(t(AgentState.CORRECTING, AgentState.EXECUTING, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(
        t(AgentState.EXECUTING, AgentState.SUPERVISOR_REVIEW, AgentEventType.SUPERVISOR_REVIEW_REQUESTED));
    b.addTransition(
        t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(t(AgentState.PAUSED, AgentState.EXECUTING, AgentEventType.FLOW_RESUMED));
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition t(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }
}
