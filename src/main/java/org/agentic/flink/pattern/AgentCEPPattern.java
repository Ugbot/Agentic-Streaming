package org.agentic.flink.pattern;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.time.Duration;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class AgentCEPPattern {

  private AgentCEPPattern() {}

  /**
   * Creates the main agent workflow pattern: 1. Flow starts 2. Tool call requested and completed
   * 3. Validation (optional) 4. Either passes validation or goes to correction/supervisor 5. Loop
   * or complete
   */
  public static Pattern<AgentEvent, ?> createAgentWorkflowPattern() {
    // NOTE: Simplified pattern - complex branching with .or() removed
    // For production, use separate patterns for different workflow paths
    return Pattern.<AgentEvent>begin(AgentPatternName.FLOW_STARTED.name())
        .where(new EventTypeCondition(AgentEventType.FLOW_STARTED))
        .next(AgentPatternName.TOOL_CALL_REQUESTED.name())
        .where(new EventTypeCondition(AgentEventType.TOOL_CALL_REQUESTED))
        .next(AgentPatternName.TOOL_CALL_COMPLETED.name())
        .where(new EventTypeCondition(AgentEventType.TOOL_CALL_COMPLETED))
        .oneOrMore()
        .greedy()
        .next(AgentPatternName.VALIDATION_STAGE.name())
        .where(new EventTypeCondition(AgentEventType.VALIDATION_REQUESTED))
        .within(Duration.ofMinutes(30)); // Overall workflow timeout
  }

  /**
   * Pattern for detecting loop iterations Detects when we should loop back vs complete
   */
  public static Pattern<AgentEvent, ?> createLoopDetectionPattern() {
    return Pattern.<AgentEvent>begin(AgentPatternName.LOOP_ITERATION.name())
        .where(new EventTypeCondition(AgentEventType.LOOP_ITERATION_STARTED))
        .where(
            new IterativeCondition<AgentEvent>() {
              @Override
              public boolean filter(AgentEvent event, Context<AgentEvent> ctx) {
                // Check if we haven't exceeded max iterations
                Integer maxIterations = event.getData("maxIterations", Integer.class);
                if (maxIterations == null) {
                  maxIterations = 10; // default
                }
                return event.getIterationNumber() < maxIterations;
              }
            })
        .within(Duration.ofMinutes(5));
  }

  /**
   * Pattern for detecting inactivity and triggering state offload Triggers when no events received
   * for a flow within timeout period
   */
  public static Pattern<AgentEvent, ?> createInactivityDetectionPattern() {
    // NOTE: Simplified inactivity detection - just detects active state events
    // For production, implement proper inactivity detection using Flink timers
    return Pattern.<AgentEvent>begin("ACTIVE_STATE")
        .where(
            new IterativeCondition<AgentEvent>() {
              @Override
              public boolean filter(AgentEvent event, Context<AgentEvent> ctx) {
                return event.getEventType() == AgentEventType.TOOL_CALL_REQUESTED
                    || event.getEventType() == AgentEventType.VALIDATION_REQUESTED
                    || event.getEventType() == AgentEventType.FLOW_RESUMED;
              }
            })
        .within(Duration.ofMinutes(30)); // 30 minutes of inactivity
  }

  /**
   * Pattern for error handling and rollback Detects error states that require intervention
   */
  public static Pattern<AgentEvent, ?> createErrorHandlingPattern() {
    // NOTE: Simplified to only match ERROR_OCCURRED. For TIMEOUT_OCCURRED, use a separate pattern.
    return Pattern.<AgentEvent>begin(AgentPatternName.ERROR_STATE.name())
        .where(new EventTypeCondition(AgentEventType.ERROR_OCCURRED))
        .within(Duration.ofSeconds(5));
  }

  /**
   * Simple validation-only pattern For agents that don't require correction or supervisor
   */
  public static Pattern<AgentEvent, ?> createSimpleValidationPattern() {
    return Pattern.<AgentEvent>begin(AgentPatternName.FLOW_STARTED.name())
        .where(new EventTypeCondition(AgentEventType.FLOW_STARTED))
        .next(AgentPatternName.TOOL_CALL_REQUESTED.name())
        .where(new EventTypeCondition(AgentEventType.TOOL_CALL_REQUESTED))
        .next(AgentPatternName.TOOL_CALL_COMPLETED.name())
        .where(new EventTypeCondition(AgentEventType.TOOL_CALL_COMPLETED))
        .next(AgentPatternName.VALIDATION_STAGE.name())
        .where(new EventTypeCondition(AgentEventType.VALIDATION_REQUESTED))
        .next(AgentPatternName.VALIDATION_PASSED.name())
        .where(new EventTypeCondition(AgentEventType.VALIDATION_PASSED))
        .next(AgentPatternName.FLOW_COMPLETED.name())
        .where(new EventTypeCondition(AgentEventType.FLOW_COMPLETED))
        .within(Duration.ofMinutes(10));
  }

  /** Reusable condition for matching event types */
  static class EventTypeCondition extends IterativeCondition<AgentEvent> {

    private final AgentEventType type;

    public EventTypeCondition(AgentEventType type) {
      this.type = type;
    }

    @Override
    public boolean filter(AgentEvent event, Context<AgentEvent> ctx) {
      return type.equals(event.getEventType());
    }
  }
}
