package org.jagentic.core;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One recorded tool invocation attempt, identified by {@code (turnId, index)} plus the attempt
 * number. Exactly one of {@code result} / {@code error} is set. Arguments are the structured map
 * handed to the tool, never a rendered string.
 */
public record ToolCall(String tool, int index, Map<String, Object> args, Object result,
                       String error, int attempt) implements Serializable {

  public ToolCall {
    args = args == null ? Map.of() : args;
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt is 1-based, got " + attempt);
    }
  }

  public static ToolCall succeeded(String tool, int index, Map<String, Object> args, Object result,
                                   int attempt) {
    return new ToolCall(tool, index, args, result, null, attempt);
  }

  public static ToolCall failed(String tool, int index, Map<String, Object> args, String error,
                                int attempt) {
    return new ToolCall(tool, index, args, null, error, attempt);
  }

  public boolean ok() {
    return error == null;
  }

  /** The {@code tool_calls[]} entry shape of {@code spec/v1/result.schema.json}. */
  public Map<String, Object> toMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("tool", tool);
    m.put("index", index);
    m.put("args", args);
    if (error == null) {
      m.put("result", result);
    } else {
      m.put("error", error);
    }
    m.put("attempt", attempt);
    return m;
  }
}
