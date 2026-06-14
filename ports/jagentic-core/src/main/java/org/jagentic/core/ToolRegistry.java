package org.jagentic.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Tools — named {@code params -> result} functions. Mirror of {@code ToolExecutor}. */
public final class ToolRegistry {

  /** A single tool. */
  public record Tool(String id, String description, Function<Map<String, Object>, Object> fn) {
    public Object execute(Map<String, Object> params) {
      return fn.apply(params == null ? Map.of() : params);
    }
  }

  private final Map<String, Tool> tools = new ConcurrentHashMap<>();

  public ToolRegistry register(String id, String description, Function<Map<String, Object>, Object> fn) {
    tools.put(id, new Tool(id, description, fn));
    return this;
  }

  public Tool get(String id) {
    return tools.get(id);
  }

  public List<String> ids() {
    return List.copyOf(tools.keySet());
  }

  public Object execute(String id, Map<String, Object> params) {
    Tool t = tools.get(id);
    if (t == null) {
      throw new IllegalArgumentException("no such tool: " + id);
    }
    return t.execute(params);
  }
}
