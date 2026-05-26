package org.agentic.flink.completion;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A goal predicate that checks whether a specific key exists (and is non-null) in the accumulated
 * state.
 *
 * <p>This is the simplest predicate: it answers "has the agent produced a value for key X?"
 *
 * @author Agentic Flink Team
 * @see GoalPredicate
 */
public class StateExistsPredicate implements GoalPredicate, Serializable {

  private static final long serialVersionUID = 1L;

  private final String key;

  /**
   * Creates a predicate that checks for the existence of a state key.
   *
   * @param key the state key to check; must not be null
   */
  public StateExistsPredicate(String key) {
    if (key == null) {
      throw new IllegalArgumentException("State key must not be null");
    }
    this.key = key;
  }

  @Override
  public boolean isSatisfied(Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    return currentState.containsKey(key) && currentState.get(key) != null;
  }

  @Override
  public double getConfidence(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    return isSatisfied(currentState, eventHistory) ? 1.0 : 0.0;
  }

  @Override
  public String getDescription() {
    return "State key '" + key + "' exists";
  }

  @Override
  public Map<String, Boolean> getDiagnostics(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    Map<String, Boolean> diagnostics = new HashMap<>();
    boolean exists = currentState.containsKey(key) && currentState.get(key) != null;
    diagnostics.put("key", exists);
    diagnostics.put("key_" + key + "_exists", exists);
    return diagnostics;
  }
}
