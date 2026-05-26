package org.agentic.flink.llm;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Result of a {@link ChatClient#chat(java.util.List, ChatSetup)} call.
 *
 * <p>Carries the raw assistant text, any parsed tool calls, and token usage if the provider
 * reports it. A typed view of the response is available through {@link #as(OutputSchema)} when
 * the originating {@link ChatSetup} declared an output schema.
 */
public final class ChatResponse implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String text;
  private final String modelName;
  private final List<ChatToolCall> toolCalls;
  private final Long tokensUsed;
  private final FinishReason finishReason;

  public ChatResponse(
      String text,
      String modelName,
      List<ChatToolCall> toolCalls,
      Long tokensUsed,
      FinishReason finishReason) {
    this.text = text == null ? "" : text;
    this.modelName = modelName;
    this.toolCalls = toolCalls == null ? Collections.emptyList() : List.copyOf(toolCalls);
    this.tokensUsed = tokensUsed;
    this.finishReason = finishReason == null ? FinishReason.UNKNOWN : finishReason;
  }

  public String getText() {
    return text;
  }

  public String getModelName() {
    return modelName;
  }

  public List<ChatToolCall> getToolCalls() {
    return toolCalls;
  }

  public Long getTokensUsed() {
    return tokensUsed;
  }

  public FinishReason getFinishReason() {
    return finishReason;
  }

  public boolean hasToolCalls() {
    return !toolCalls.isEmpty();
  }

  /** Parse the response under the given schema. */
  public <T> T as(OutputSchema<T> schema) throws OutputSchema.SchemaViolation {
    return schema.parse(text);
  }

  /** Why the model stopped generating. */
  public enum FinishReason {
    STOP,
    LENGTH,
    TOOL_CALLS,
    CONTENT_FILTER,
    ERROR,
    UNKNOWN
  }
}
