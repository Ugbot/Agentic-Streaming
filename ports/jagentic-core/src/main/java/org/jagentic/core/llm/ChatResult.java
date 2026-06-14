package org.jagentic.core.llm;

import java.util.Map;

/** One model step: a final {@code text} answer, or a {@code tool} call with {@code args}. */
public record ChatResult(String text, String tool, Map<String, Object> args) {

  public boolean isToolCall() {
    return tool != null && !tool.isBlank();
  }

  public static ChatResult text(String text) {
    return new ChatResult(text, null, Map.of());
  }

  public static ChatResult toolCall(String tool, Map<String, Object> args) {
    return new ChatResult(null, tool, args == null ? Map.of() : args);
  }
}
