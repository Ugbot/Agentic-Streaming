package org.agentic.flink.llm;

import java.io.Serializable;
import java.util.Objects;
import org.agentic.flink.typeinfo.JsonTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * A single message in a chat conversation, decoupled from any vendor SDK.
 *
 * <p>{@code toolCallId} and {@code toolName} are only populated for {@link ChatRole#TOOL}
 * messages — they identify which tool call this message reports the result of.
 */
@TypeInfo(ChatMessage.Factory.class)
public final class ChatMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Serializes via JSON ({@link org.agentic.flink.typeinfo.FlinkJson}) instead of Kryo wherever it
   * flows (e.g. {@code ReActProcessFunction}'s transcript {@code ListState}). Immutable — copied by
   * reference. Jackson binds the all-args constructor via {@code ParameterNamesModule}.
   */
  public static final class Factory extends JsonTypeInfoFactory<ChatMessage> {
    public Factory() {
      super(ChatMessage.class, false);
    }
  }

  private final ChatRole role;
  private final String content;
  private final String toolCallId;
  private final String toolName;

  private ChatMessage(ChatRole role, String content, String toolCallId, String toolName) {
    this.role = Objects.requireNonNull(role, "role");
    this.content = content == null ? "" : content;
    this.toolCallId = toolCallId;
    this.toolName = toolName;
  }

  public static ChatMessage system(String content) {
    return new ChatMessage(ChatRole.SYSTEM, content, null, null);
  }

  public static ChatMessage user(String content) {
    return new ChatMessage(ChatRole.USER, content, null, null);
  }

  public static ChatMessage assistant(String content) {
    return new ChatMessage(ChatRole.ASSISTANT, content, null, null);
  }

  public static ChatMessage tool(String toolCallId, String toolName, String content) {
    return new ChatMessage(ChatRole.TOOL, content, toolCallId, toolName);
  }

  public ChatRole getRole() {
    return role;
  }

  public String getContent() {
    return content;
  }

  public String getToolCallId() {
    return toolCallId;
  }

  public String getToolName() {
    return toolName;
  }

  @Override
  public String toString() {
    return "ChatMessage[" + role + ", " + content.length() + "ch]";
  }
}
