package org.jagentic.ports.spring;

import java.util.EnumSet;

import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineBuilder;

/**
 * The agent phase FSM (design doc §3f, C10 -> Spring StateMachine).
 *
 * <p>The Flink port compiles {@code AgentStateMachine}
 * (INITIALIZED -> VALIDATING -> EXECUTING -> SUPERVISOR_REVIEW -> COMPLETED, with
 * CORRECTING / FAILED / COMPENSATING) to CEP patterns. On Spring it becomes a far more
 * natural home: a declarative state machine of states + transitions + guards + actions.
 *
 * <p>This is a small, self-contained illustration of that mapping using the
 * {@code spring-statemachine-core} programmatic {@link StateMachineBuilder} (no extra
 * autoconfiguration so the module stays minimal). In a fuller port you would persist
 * the machine via {@code StateMachinePersister} keyed by {@code conversationId} so the
 * phase survives restarts — the same external-durability story as the
 * {@code ConversationStore} (C1).
 */
public final class AgentPhaseFsm {

  private AgentPhaseFsm() {}

  /** The workflow phases, mirroring the Flink {@code AgentState} enum. */
  public enum Phase {
    INITIALIZED, VALIDATING, EXECUTING, SUPERVISOR_REVIEW,
    CORRECTING, COMPLETED, FAILED, COMPENSATING, COMPENSATED
  }

  /** The triggers that drive phase transitions, mirroring the Flink event types. */
  public enum Trigger {
    START, VALIDATION_PASSED, VALIDATION_FAILED, REVIEW_REQUESTED,
    REVIEW_PASSED, MAX_ITERATIONS_REACHED, COMPENSATION_TRIGGERED
  }

  /**
   * Build the phase machine. The transitions below are the direct Spring StateMachine
   * analog of the Flink {@code AgentTransition}s (each {@code .guard(...)} ==
   * {@code AgentTransition.when(...)}, each {@code .action(...)} ==
   * {@code AgentTransition.action(...)}).
   */
  public static StateMachine<Phase, Trigger> build() throws Exception {
    StateMachineBuilder.Builder<Phase, Trigger> b = StateMachineBuilder.builder();

    b.configureStates()
        .withStates()
        .initial(Phase.INITIALIZED)
        .states(EnumSet.allOf(Phase.class))
        .end(Phase.COMPLETED)
        .end(Phase.FAILED)
        .end(Phase.COMPENSATED);

    b.configureTransitions()
        .withExternal()
            .source(Phase.INITIALIZED).target(Phase.VALIDATING).event(Trigger.START)
        .and().withExternal()
            .source(Phase.VALIDATING).target(Phase.EXECUTING).event(Trigger.VALIDATION_PASSED)
        .and().withExternal()
            .source(Phase.VALIDATING).target(Phase.CORRECTING).event(Trigger.VALIDATION_FAILED)
        .and().withExternal()
            .source(Phase.CORRECTING).target(Phase.VALIDATING).event(Trigger.START)
        .and().withExternal()
            .source(Phase.EXECUTING).target(Phase.SUPERVISOR_REVIEW).event(Trigger.REVIEW_REQUESTED)
        .and().withExternal()
            .source(Phase.SUPERVISOR_REVIEW).target(Phase.COMPLETED).event(Trigger.REVIEW_PASSED)
        .and().withExternal()
            .source(Phase.EXECUTING).target(Phase.FAILED).event(Trigger.MAX_ITERATIONS_REACHED)
        .and().withExternal()
            .source(Phase.FAILED).target(Phase.COMPENSATING).event(Trigger.COMPENSATION_TRIGGERED)
        .and().withExternal()
            .source(Phase.COMPENSATING).target(Phase.COMPENSATED).event(Trigger.REVIEW_PASSED);

    return b.build();
  }
}
