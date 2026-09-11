package org.jagentic.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The declarative {@code saga} block: an ordered list of tool steps. When a step fails, the
 * compensations of every completed step run in reverse order. {@code compensateWith} is the resolved
 * compensation tool: the step's {@code compensate_with}, else the tool's declared {@code
 * compensation}; the loader resolves the latter (see {@link #resolve(Map)}).
 */
public record SagaPlan(List<Step> steps) implements Serializable {

  public record Step(String name, String tool, Map<String, Object> args, String compensateWith)
      implements Serializable {
    public Step {
      if (tool == null || tool.isBlank()) {
        throw new IllegalArgumentException("saga step requires a tool");
      }
      name = name == null ? tool : name;
      args = args == null ? Map.of() : args;
    }
  }

  public SagaPlan {
    if (steps == null || steps.isEmpty()) {
      throw new IllegalArgumentException("saga requires at least one step");
    }
    steps = List.copyOf(steps);
  }

  /** Fills each step's missing {@code compensateWith} from {@code toolCompensations} (tool id -> undo tool id). */
  public SagaPlan resolve(Map<String, String> toolCompensations) {
    List<Step> out = new ArrayList<>(steps.size());
    for (Step s : steps) {
      String undo = s.compensateWith() != null ? s.compensateWith() : toolCompensations.get(s.tool());
      out.add(new Step(s.name(), s.tool(), s.args(), undo));
    }
    return new SagaPlan(out);
  }

  @SuppressWarnings("unchecked")
  public static SagaPlan fromMap(Map<String, Object> m) {
    if (m == null) {
      return null;
    }
    Object raw = m.get("steps");
    if (!(raw instanceof List<?> list)) {
      throw new IllegalArgumentException("saga.steps must be a list");
    }
    List<Step> steps = new ArrayList<>(list.size());
    for (Object o : list) {
      Map<String, Object> s = (Map<String, Object>) o;
      Object args = s.get("args");
      steps.add(new Step((String) s.get("name"), (String) s.get("tool"),
          args instanceof Map<?, ?> am ? (Map<String, Object>) am : Map.of(),
          (String) s.get("compensate_with")));
    }
    return new SagaPlan(steps);
  }
}
