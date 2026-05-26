package org.agentic.flink.completion;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A composite goal predicate that combines child predicates with AND, OR, or NOT logic.
 *
 * <ul>
 *   <li><b>AND</b>: satisfied when <em>all</em> children are satisfied. Confidence is the
 *       <em>minimum</em> of children's confidences.
 *   <li><b>OR</b>: satisfied when <em>any</em> child is satisfied. Confidence is the
 *       <em>maximum</em> of children's confidences.
 *   <li><b>NOT</b>: inverts a single child. Confidence is {@code 1.0 - child.getConfidence()}.
 * </ul>
 *
 * @author Agentic Flink Team
 * @see GoalPredicate
 */
public class CompositeGoalPredicate implements GoalPredicate, Serializable {

  private static final long serialVersionUID = 1L;

  /** The composition mode for combining child predicates. */
  public enum CompositeMode {
    AND,
    OR,
    NOT
  }

  private final CompositeMode mode;
  private final List<GoalPredicate> children;

  /**
   * Creates a composite predicate.
   *
   * @param mode the composition mode
   * @param children the child predicates; for NOT, exactly one child is required
   * @throws IllegalArgumentException if children is empty, or NOT mode has != 1 child
   */
  public CompositeGoalPredicate(CompositeMode mode, List<GoalPredicate> children) {
    if (mode == null) {
      throw new IllegalArgumentException("CompositeMode must not be null");
    }
    if (children == null || children.isEmpty()) {
      throw new IllegalArgumentException("CompositeGoalPredicate requires at least one child");
    }
    if (mode == CompositeMode.NOT && children.size() != 1) {
      throw new IllegalArgumentException(
          "NOT mode requires exactly 1 child, got: " + children.size());
    }
    this.mode = mode;
    this.children = Collections.unmodifiableList(children);
  }

  @Override
  public boolean isSatisfied(Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    switch (mode) {
      case AND:
        for (GoalPredicate child : children) {
          if (!child.isSatisfied(currentState, eventHistory)) {
            return false;
          }
        }
        return true;

      case OR:
        for (GoalPredicate child : children) {
          if (child.isSatisfied(currentState, eventHistory)) {
            return true;
          }
        }
        return false;

      case NOT:
        return !children.get(0).isSatisfied(currentState, eventHistory);

      default:
        throw new IllegalStateException("Unknown CompositeMode: " + mode);
    }
  }

  @Override
  public double getConfidence(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    switch (mode) {
      case AND:
        double minConfidence = 1.0;
        for (GoalPredicate child : children) {
          minConfidence = Math.min(minConfidence, child.getConfidence(currentState, eventHistory));
        }
        return minConfidence;

      case OR:
        double maxConfidence = 0.0;
        for (GoalPredicate child : children) {
          maxConfidence = Math.max(maxConfidence, child.getConfidence(currentState, eventHistory));
        }
        return maxConfidence;

      case NOT:
        return 1.0 - children.get(0).getConfidence(currentState, eventHistory);

      default:
        throw new IllegalStateException("Unknown CompositeMode: " + mode);
    }
  }

  @Override
  public String getDescription() {
    switch (mode) {
      case AND:
        {
          StringBuilder sb = new StringBuilder("ALL(");
          for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
              sb.append(" AND ");
            }
            sb.append(children.get(i).getDescription());
          }
          sb.append(")");
          return sb.toString();
        }

      case OR:
        {
          StringBuilder sb = new StringBuilder("ANY(");
          for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
              sb.append(" OR ");
            }
            sb.append(children.get(i).getDescription());
          }
          sb.append(")");
          return sb.toString();
        }

      case NOT:
        return "NOT(" + children.get(0).getDescription() + ")";

      default:
        throw new IllegalStateException("Unknown CompositeMode: " + mode);
    }
  }

  @Override
  public Map<String, Boolean> getDiagnostics(
      Map<String, Object> currentState, Iterable<AgentEvent> eventHistory) {
    Map<String, Boolean> diagnostics = new HashMap<>();
    diagnostics.put("goal", isSatisfied(currentState, eventHistory));
    diagnostics.put("mode_" + mode.name(), true);
    for (int i = 0; i < children.size(); i++) {
      GoalPredicate child = children.get(i);
      Map<String, Boolean> childDiag = child.getDiagnostics(currentState, eventHistory);
      for (Map.Entry<String, Boolean> entry : childDiag.entrySet()) {
        diagnostics.put("child_" + i + "_" + entry.getKey(), entry.getValue());
      }
      diagnostics.put("child_" + i + "_satisfied", child.isSatisfied(currentState, eventHistory));
    }
    return diagnostics;
  }
}
