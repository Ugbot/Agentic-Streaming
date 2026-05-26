package org.agentic.flink.completion;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A goal predicate that checks whether a numeric state value exceeds a given threshold.
 *
 * <p>The value at the specified key must be a {@link Number}. If the key is missing or the value is
 * not numeric, the predicate is not satisfied and confidence is 0.0.
 *
 * <p>Confidence is proportional: {@code Math.min(1.0, actualValue / threshold)}, clamped to [0.0,
 * 1.0]. This gives a smooth ramp from 0 to 1 as the value approaches and exceeds the threshold.
 *
 * @author Agentic Flink Team
 * @see GoalPredicate
 */
public class NumericThresholdPredicate implements GoalPredicate, Serializable {

  private static final long serialVersionUID = 1L;

  private final String key;
  private final double threshold;

  /**
   * Creates a predicate that checks whether the state value at {@code key} is a number greater than
   * {@code threshold}.
   *
   * @param key the state key holding a numeric value; must not be null
   * @param threshold the threshold the value must exceed (strictly greater than)
   */
  public NumericThresholdPredicate(String key, double threshold) {
    if (key == null) {
      throw new IllegalArgumentException("State key must not be null");
    }
    this.key = key;
    this.threshold = threshold;
  }

  @Override
  public boolean isSatisfied(Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    Object value = currentState.get(key);
    if (value instanceof Number) {
      return ((Number) value).doubleValue() > threshold;
    }
    return false;
  }

  @Override
  public double getConfidence(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    Object value = currentState.get(key);
    if (value instanceof Number) {
      double actual = ((Number) value).doubleValue();
      if (threshold == 0.0) {
        return actual > 0.0 ? 1.0 : 0.0;
      }
      return Math.max(0.0, Math.min(1.0, actual / threshold));
    }
    return 0.0;
  }

  @Override
  public String getDescription() {
    return "State key '" + key + "' > " + threshold;
  }

  @Override
  public Map<String, Boolean> getDiagnostics(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    Map<String, Boolean> diagnostics = new HashMap<>();
    boolean satisfied = isSatisfied(currentState, eventHistory);
    diagnostics.put("goal", satisfied);
    Object value = currentState.get(key);
    diagnostics.put("key_" + key + "_is_numeric", value instanceof Number);
    diagnostics.put("key_" + key + "_exceeds_threshold", satisfied);
    return diagnostics;
  }
}
