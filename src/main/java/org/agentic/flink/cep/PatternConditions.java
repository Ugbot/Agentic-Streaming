package org.agentic.flink.cep;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.statemachine.AgentState;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * Collection of common CEP pattern conditions for agent workflows.
 *
 * <p>This class provides reusable condition implementations inspired by the Saga kit's condition
 * patterns. These conditions are used to filter events in CEP patterns and implement guard logic
 * for state transitions.
 *
 * <p><b>Condition Types:</b>
 * <ul>
 *   <li><b>SimpleCondition</b> - Stateless condition, checks only the event itself</li>
 *   <li><b>IterativeCondition</b> - Stateful condition, can access previous pattern matches</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * Pattern<AgentEvent, ?> pattern = Pattern.<AgentEvent>begin("start")
 *     .where(PatternConditions.eventTypeCondition(AgentEventType.FLOW_STARTED))
 *     .next("execute")
 *     .where(PatternConditions.eventTypeCondition(AgentEventType.TOOL_CALL_COMPLETED))
 *     .where(PatternConditions.metadataContains("requires_review"))
 *     .where(PatternConditions.maxIterations(5));
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see SimpleCondition
 * @see IterativeCondition
 */
public class PatternConditions {

  // Private constructor - utility class
  private PatternConditions() {}

  // ==================== Event Type Conditions ====================

  /**
   * Creates a condition that matches one or more event types.
   *
   * <p>This is the most common condition, matching events by their AgentEventType.
   *
   * @param eventTypes One or more event types to match
   * @return condition that matches any of the specified event types
   */
  public static SimpleCondition<AgentEvent> eventTypeCondition(AgentEventType... eventTypes) {
    Set<AgentEventType> eventTypeSet = Arrays.stream(eventTypes).collect(Collectors.toSet());

    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return eventTypeSet.contains(event.getEventType());
      }
    };
  }

  /**
   * Creates a condition that matches a specific event type.
   *
   * @param eventType The event type to match
   * @return condition that matches the specified event type
   */
  public static SimpleCondition<AgentEvent> eventType(AgentEventType eventType) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return event.getEventType() == eventType;
      }
    };
  }

  // ==================== State Conditions ====================

  /**
   * Creates a condition that matches events associated with specific agent states.
   *
   * <p>This checks the event's metadata for a "state" field and matches against the provided
   * states.
   *
   * @param states One or more agent states to match
   * @return condition that matches any of the specified states
   */
  public static SimpleCondition<AgentEvent> stateCondition(AgentState... states) {
    Set<String> stateIds =
        Arrays.stream(states).map(AgentState::getStateId).collect(Collectors.toSet());

    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        Object stateObj = event.getData().get("state");
        if (stateObj == null) {
          return false;
        }
        String stateId = stateObj.toString();
        return stateIds.contains(stateId);
      }
    };
  }

  /**
   * Creates a condition that matches a specific agent state.
   *
   * @param state The agent state to match
   * @return condition that matches the specified state
   */
  public static SimpleCondition<AgentEvent> state(AgentState state) {
    return stateCondition(state);
  }

  // ==================== Metadata Conditions ====================

  /**
   * Creates a condition that checks if metadata contains a specific key.
   *
   * @param key The metadata key to check
   * @return condition that matches if the key exists
   */
  public static SimpleCondition<AgentEvent> metadataContains(String key) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return event.getData() != null && event.getData().containsKey(key);
      }
    };
  }

  /**
   * Creates a condition that checks if metadata contains a key with a specific value.
   *
   * @param key The metadata key to check
   * @param value The expected value
   * @return condition that matches if key exists with the specified value
   */
  public static SimpleCondition<AgentEvent> metadataEquals(String key, Object value) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        if (event.getData() == null) {
          return false;
        }
        Object actualValue = event.getData().get(key);
        return value.equals(actualValue);
      }
    };
  }

  /**
   * Creates a condition that checks if a numeric metadata value meets a threshold.
   *
   * @param key The metadata key to check
   * @param threshold The minimum value
   * @return condition that matches if value >= threshold
   */
  public static SimpleCondition<AgentEvent> metadataGreaterThan(String key, double threshold) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        if (event.getData() == null) {
          return false;
        }
        Object valueObj = event.getData().get(key);
        if (valueObj instanceof Number) {
          return ((Number) valueObj).doubleValue() > threshold;
        }
        return false;
      }
    };
  }

  // ==================== Iteration Conditions ====================

  /**
   * Creates a condition that checks if iteration count is below maximum.
   *
   * <p>This is useful for implementing retry logic and loop limits.
   *
   * @param maxIterations Maximum allowed iterations
   * @return condition that matches if iterations < max
   */
  public static SimpleCondition<AgentEvent> maxIterations(int maxIterations) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        Object iterationObj = event.getData().get("iteration");
        if (iterationObj instanceof Number) {
          int iteration = ((Number) iterationObj).intValue();
          return iteration < maxIterations;
        }
        return true; // No iteration data, allow by default
      }
    };
  }

  /**
   * Creates a condition that checks if validation attempts are below maximum.
   *
   * @param maxAttempts Maximum allowed validation attempts
   * @return condition that matches if attempts < max
   */
  public static SimpleCondition<AgentEvent> maxValidationAttempts(int maxAttempts) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        Object attemptsObj = event.getData().get("validation_attempts");
        if (attemptsObj instanceof Number) {
          int attempts = ((Number) attemptsObj).intValue();
          return attempts < maxAttempts;
        }
        return true; // No attempt data, allow by default
      }
    };
  }

  /**
   * Creates a condition that checks if correction attempts are below maximum.
   *
   * @param maxAttempts Maximum allowed correction attempts
   * @return condition that matches if attempts < max
   */
  public static SimpleCondition<AgentEvent> maxCorrectionAttempts(int maxAttempts) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        Object attemptsObj = event.getData().get("correction_attempts");
        if (attemptsObj instanceof Number) {
          int attempts = ((Number) attemptsObj).intValue();
          return attempts < maxAttempts;
        }
        return true; // No attempt data, allow by default
      }
    };
  }

  // ==================== User & Flow Conditions ====================

  /**
   * Creates a condition that matches a specific flow ID.
   *
   * @param flowId The flow ID to match
   * @return condition that matches the specified flow
   */
  public static SimpleCondition<AgentEvent> flowId(String flowId) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return flowId.equals(event.getFlowId());
      }
    };
  }

  /**
   * Creates a condition that matches a specific user ID.
   *
   * @param userId The user ID to match
   * @return condition that matches the specified user
   */
  public static SimpleCondition<AgentEvent> userId(String userId) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return userId.equals(event.getUserId());
      }
    };
  }

  /**
   * Creates a condition that matches a specific agent ID.
   *
   * @param agentId The agent ID to match
   * @return condition that matches the specified agent
   */
  public static SimpleCondition<AgentEvent> agentId(String agentId) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return agentId.equals(event.getAgentId());
      }
    };
  }

  // ==================== Tool Conditions ====================

  /**
   * Creates a condition that matches tool call events for a specific tool.
   *
   * @param toolName The tool name to match
   * @return condition that matches tool calls for the specified tool
   */
  public static SimpleCondition<AgentEvent> toolName(String toolName) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        Object toolObj = event.getData().get("tool_name");
        return toolName.equals(toolObj);
      }
    };
  }

  /**
   * Creates a condition that checks if tool execution was successful.
   *
   * @return condition that matches successful tool completions
   */
  public static SimpleCondition<AgentEvent> toolSuccess() {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return event.getEventType() == AgentEventType.TOOL_CALL_COMPLETED
            && !event.getData().containsKey("error");
      }
    };
  }

  /**
   * Creates a condition that checks if tool execution failed.
   *
   * @return condition that matches failed tool calls
   */
  public static SimpleCondition<AgentEvent> toolFailure() {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return event.getEventType() == AgentEventType.TOOL_CALL_FAILED
            || (event.getEventType() == AgentEventType.TOOL_CALL_COMPLETED
                && event.getData().containsKey("error"));
      }
    };
  }

  // ==================== Aggregation Conditions (Iterative) ====================

  /**
   * Creates an iterative condition that aggregates a numeric value across events.
   *
   * <p>This is inspired by the saga kit's quantity aggregation for "N things done" tracking.
   *
   * <p>Example: Track total executed quantity across multiple tool calls.
   *
   * @param key The metadata key containing the numeric value
   * @param targetSum The target sum to reach
   * @return condition that matches when cumulative sum >= target
   */
  public static IterativeCondition<AgentEvent> aggregateSum(String key, double targetSum) {
    return new IterativeCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event, Context<AgentEvent> ctx) throws Exception {
        double sum = 0.0;

        // Sum values from all previous events in the pattern
        for (AgentEvent previousEvent : ctx.getEventsForPattern("executing")) {
          Object valueObj = previousEvent.getData().get(key);
          if (valueObj instanceof Number) {
            sum += ((Number) valueObj).doubleValue();
          }
        }

        // Add current event's value
        Object currentValueObj = event.getData().get(key);
        if (currentValueObj instanceof Number) {
          sum += ((Number) currentValueObj).doubleValue();
        }

        return sum >= targetSum;
      }
    };
  }

  /**
   * Creates an iterative condition that counts matching events.
   *
   * <p>Example: Match when N tool calls have completed.
   *
   * @param patternName The pattern step name to count
   * @param targetCount The target count to reach
   * @return condition that matches when count >= target
   */
  public static IterativeCondition<AgentEvent> aggregateCount(
      String patternName, int targetCount) {
    return new IterativeCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event, Context<AgentEvent> ctx) throws Exception {
        Iterable<AgentEvent> previousEvents = ctx.getEventsForPattern(patternName);
        int count = 0;
        for (@SuppressWarnings("unused") AgentEvent previousEvent : previousEvents) {
          count++;
        }
        return count >= targetCount;
      }
    };
  }

  // ==================== Saga-style Conditions ====================

  /**
   * Creates a condition that checks if compensation is required.
   *
   * <p>This is used in saga patterns to trigger rollback when a step fails.
   *
   * @return condition that matches events requiring compensation
   */
  public static SimpleCondition<AgentEvent> requiresCompensation() {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return event.getEventType() == AgentEventType.FLOW_FAILED
            && Boolean.TRUE.equals(event.getData().get("enable_compensation"));
      }
    };
  }

  /**
   * Creates a condition that checks if timeout occurred.
   *
   * @return condition that matches timeout events
   */
  public static SimpleCondition<AgentEvent> timedOut() {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return event.getEventType() == AgentEventType.TIMEOUT_OCCURRED;
      }
    };
  }

  // ==================== Composite Conditions ====================

  /**
   * Creates a condition that matches if ANY of the provided conditions match (OR).
   *
   * @param conditions Conditions to check
   * @return condition that matches if any condition matches
   */
  @SafeVarargs
  public static SimpleCondition<AgentEvent> anyOf(SimpleCondition<AgentEvent>... conditions) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        for (SimpleCondition<AgentEvent> condition : conditions) {
          if (condition.filter(event)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  /**
   * Creates a condition that matches if ALL of the provided conditions match (AND).
   *
   * @param conditions Conditions to check
   * @return condition that matches if all conditions match
   */
  @SafeVarargs
  public static SimpleCondition<AgentEvent> allOf(SimpleCondition<AgentEvent>... conditions) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        for (SimpleCondition<AgentEvent> condition : conditions) {
          if (!condition.filter(event)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  /**
   * Creates a condition that negates another condition (NOT).
   *
   * @param condition The condition to negate
   * @return condition that matches if the original condition does NOT match
   */
  public static SimpleCondition<AgentEvent> not(SimpleCondition<AgentEvent> condition) {
    return new SimpleCondition<AgentEvent>() {
      @Override
      public boolean filter(AgentEvent event) throws Exception {
        return !condition.filter(event);
      }
    };
  }
}
