package org.agentic.flink.completion;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A goal predicate that counts events of a specific {@link AgentEventType} and checks whether the
 * count reaches a target.
 *
 * <p>Confidence is proportional: {@code Math.min(1.0, (double) count / targetCount)}.
 *
 * @author Agentic Flink Team
 * @see GoalPredicate
 */
public class EventCountPredicate implements GoalPredicate, Serializable {

  private static final long serialVersionUID = 1L;

  private final AgentEventType eventType;
  private final int targetCount;

  /**
   * Creates a predicate requiring at least {@code targetCount} events of the given type.
   *
   * @param eventType the event type to count; must not be null
   * @param targetCount the minimum number of matching events required; must be positive
   */
  public EventCountPredicate(AgentEventType eventType, int targetCount) {
    if (eventType == null) {
      throw new IllegalArgumentException("Event type must not be null");
    }
    if (targetCount <= 0) {
      throw new IllegalArgumentException("Target count must be positive, got: " + targetCount);
    }
    this.eventType = eventType;
    this.targetCount = targetCount;
  }

  @Override
  public boolean isSatisfied(Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    return countMatching(eventHistory) >= targetCount;
  }

  @Override
  public double getConfidence(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    int count = countMatching(eventHistory);
    return Math.min(1.0, (double) count / targetCount);
  }

  @Override
  public String getDescription() {
    return "At least " + targetCount + " " + eventType + " events";
  }

  @Override
  public Map<String, Boolean> getDiagnostics(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    int count = countMatching(eventHistory);
    Map<String, Boolean> diagnostics = new HashMap<>();
    diagnostics.put("goal", count >= targetCount);
    diagnostics.put("count_reached_" + targetCount, count >= targetCount);
    return diagnostics;
  }

  private int countMatching(Iterable<AgentEvent> eventHistory) {
    int count = 0;
    for (AgentEvent event : eventHistory) {
      if (event.getEventType() == eventType) {
        count++;
      }
    }
    return count;
  }
}
