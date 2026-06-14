package org.jagentic.core;

/** A transcript entry. {@code role} is one of system|user|assistant|tool. */
public record ChatMessage(String role, String content, String toolName, String toolCallId) {
  public static ChatMessage user(String t) {
    return new ChatMessage("user", t, null, null);
  }

  public static ChatMessage assistant(String t) {
    return new ChatMessage("assistant", t, null, null);
  }

  public static ChatMessage system(String t) {
    return new ChatMessage("system", t, null, null);
  }

  public static ChatMessage tool(String callId, String name, String content) {
    return new ChatMessage("tool", content, name, callId);
  }
}
