package org.agentic.flink.completion;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Interface for goal-based completion predicates (FUTURE ARCHITECTURE).
 *
 * <p>This interface defines the contract for goal-based completion checking, which is more flexible
 * than simple task counting. Instead of tracking "N tasks done", goals allow expressing complex
 * completion conditions like:
 * <ul>
 *   <li>"User data fetched AND preferences loaded AND (history available OR timeout occurred)"</li>
 *   <li>"Total research score >= 0.9"</li>
 *   <li>"All validation checks passed OR manual override received"</li>
 *   <li>"Budget remaining > 0 AND quality threshold met"</li>
 * </ul>
 *
 * <p><b>Architecture Document:</b> See docs/GOAL_BASED_ARCHITECTURE.md for detailed design.
 *
 * <p><b>Future Implementation Plan:</b>
 * <pre>
 * Phase 1 (Current): Event aggregation with TaskList
 *     - Simple "N things done" counting
 *     - Required vs optional tasks
 *     - Percentage-based completion
 *
 * Phase 2 (Future): Goal predicates with boolean logic
 *     - AND, OR, NOT combinators
 *     - Numeric threshold goals
 *     - Time-based goals
 *     - Complex nested conditions
 *
 * Phase 3 (Future): Machine learning goal inference
 *     - Learn completion patterns from historical data
 *     - Adaptive goals based on context
 *     - Confidence-based early termination
 * </pre>
 *
 * <p><b>Usage Example (Future):</b>
 * <pre>{@code
 * GoalPredicate researchGoal = GoalPredicate.builder()
 *     .requireAll(
 *         StatePredicate.exists("research_summary"),
 *         StatePredicate.exists("source_citations"),
 *         NumericPredicate.greaterThan("quality_score", 0.8)
 *     )
 *     .requireAny(
 *         StatePredicate.exists("peer_review"),
 *         StatePredicate.exists("manual_approval")
 *     )
 *     .withTimeout(Duration.ofMinutes(5))
 *     .build();
 *
 * if (researchGoal.isSatisfied(currentState, events)) {
 *     // All goals met, proceed with completion
 * }
 * }</pre>
 *
 * <p><b>Integration with CEP:</b> Goal predicates can be compiled into CEP IterativeConditions,
 * allowing the Flink CEP engine to evaluate them incrementally as events arrive.
 *
 * @author Agentic Flink Team
 * @see TaskList
 * @see CompletionTracker
 */
public interface GoalPredicate extends Serializable {

  /**
   * Checks if the goal is satisfied given current state and event history.
   *
   * <p>This method is called incrementally as events arrive, allowing for efficient streaming
   * evaluation.
   *
   * @param currentState Current agent state (metadata, variables, etc.)
   * @param eventHistory Historical events that have occurred
   * @return true if the goal is satisfied, false otherwise
   */
  boolean isSatisfied(Map<String, Object> currentState, Iterable<AgentEvent> eventHistory);

  /**
   * Returns a confidence score (0.0 to 1.0) indicating how close the goal is to being satisfied.
   *
   * <p>This enables early termination strategies and adaptive behavior:
   * <ul>
   *   <li>0.0 = Not satisfied at all</li>
   *   <li>0.5 = Halfway to satisfaction</li>
   *   <li>1.0 = Fully satisfied</li>
   * </ul>
   *
   * @param currentState Current agent state
   * @param eventHistory Historical events
   * @return confidence score between 0.0 and 1.0
   */
  default double getConfidence(Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    return isSatisfied(currentState, eventHistory) ? 1.0 : 0.0;
  }

  /**
   * Returns a human-readable description of what this goal represents.
   *
   * @return goal description
   */
  String getDescription();

  /**
   * Returns diagnostic information about which sub-goals are satisfied/unsatisfied.
   *
   * <p>Useful for debugging and providing feedback to users.
   *
   * @param currentState Current agent state
   * @param eventHistory Historical events
   * @return map of sub-goal ID to satisfaction status
   */
  default Map<String, Boolean> getDiagnostics(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    return Map.of("goal", isSatisfied(currentState, eventHistory));
  }

  // ==================== Future Factory Methods ====================

  /**
   * Creates a goal that requires a specific state variable to exist.
   *
   * <p><b>Implementation status:</b> Not yet implemented (placeholder for future architecture)
   *
   * @param stateKey The state variable key to check
   * @return goal predicate checking for state existence
   */
  static GoalPredicate stateExists(String stateKey) {
    return new StateExistsPredicate(stateKey);
  }

  /**
   * Creates a goal that requires a numeric state variable to exceed a threshold.
   *
   * <p><b>Implementation status:</b> Not yet implemented (placeholder for future architecture)
   *
   * @param stateKey The state variable key
   * @param threshold The minimum value
   * @return goal predicate checking numeric threshold
   */
  static GoalPredicate greaterThan(String stateKey, double threshold) {
    return new NumericThresholdPredicate(stateKey, threshold);
  }

  /**
   * Creates a goal that requires all sub-goals to be satisfied (AND).
   *
   * <p><b>Implementation status:</b> Not yet implemented (placeholder for future architecture)
   *
   * @param subGoals Sub-goals to combine
   * @return goal predicate that requires all sub-goals
   */
  static GoalPredicate all(GoalPredicate... subGoals) {
    return new CompositeGoalPredicate(CompositeGoalPredicate.CompositeMode.AND, List.of(subGoals));
  }

  /**
   * Creates a goal that requires any sub-goal to be satisfied (OR).
   *
   * <p><b>Implementation status:</b> Not yet implemented (placeholder for future architecture)
   *
   * @param subGoals Sub-goals to combine
   * @return goal predicate that requires any sub-goal
   */
  static GoalPredicate any(GoalPredicate... subGoals) {
    return new CompositeGoalPredicate(CompositeGoalPredicate.CompositeMode.OR, List.of(subGoals));
  }

  /**
   * Creates a goal that negates another goal (NOT).
   *
   * <p><b>Implementation status:</b> Not yet implemented (placeholder for future architecture)
   *
   * @param goal Goal to negate
   * @return goal predicate that negates the input
   */
  static GoalPredicate not(GoalPredicate goal) {
    return new CompositeGoalPredicate(CompositeGoalPredicate.CompositeMode.NOT, List.of(goal));
  }

  /**
   * Creates a goal that counts events of a specific type and checks the count against a target.
   *
   * @param eventType the event type to count
   * @param targetCount the minimum number of matching events required
   * @return goal predicate checking event count
   */
  static GoalPredicate eventCount(AgentEventType eventType, int targetCount) {
    return new EventCountPredicate(eventType, targetCount);
  }
}
