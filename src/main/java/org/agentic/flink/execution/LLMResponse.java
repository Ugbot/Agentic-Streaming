package org.agentic.flink.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Response from LLM call.
 *
 * @author Agentic Flink Team
 */
public class LLMResponse implements Serializable {

  private static final long serialVersionUID = 1L;

  private String text;
  private String model;
  private List<ToolCall> toolCalls;
  private int tokenUsage;

  public LLMResponse() {
    this.toolCalls = new ArrayList<>();
  }

  public String getText() { return text; }
  public void setText(String text) { this.text = text; }

  public String getModel() { return model; }
  public void setModel(String model) { this.model = model; }

  public List<ToolCall> getToolCalls() { return toolCalls; }
  public void setToolCalls(List<ToolCall> toolCalls) { this.toolCalls = toolCalls; }

  public int getTokenUsage() { return tokenUsage; }
  public void setTokenUsage(int tokenUsage) { this.tokenUsage = tokenUsage; }

  public boolean hasToolCalls() {
    return toolCalls != null && !toolCalls.isEmpty();
  }
}
