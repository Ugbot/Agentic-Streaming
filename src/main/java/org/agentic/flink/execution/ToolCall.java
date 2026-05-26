package org.agentic.flink.execution;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a tool call request from the LLM.
 *
 * @author Agentic Flink Team
 */
public class ToolCall implements Serializable {

  private static final long serialVersionUID = 1L;

  private String toolCallId;
  private String toolName;
  private Map<String, Object> parameters;

  public ToolCall() {
    this.parameters = new HashMap<>();
  }

  public ToolCall(String toolCallId, String toolName, Map<String, Object> parameters) {
    this.toolCallId = toolCallId;
    this.toolName = toolName;
    this.parameters = parameters != null ? parameters : new HashMap<>();
  }

  public String getToolCallId() { return toolCallId; }
  public void setToolCallId(String toolCallId) { this.toolCallId = toolCallId; }

  public String getToolName() { return toolName; }
  public void setToolName(String toolName) { this.toolName = toolName; }

  public Map<String, Object> getParameters() { return parameters; }
  public void setParameters(Map<String, Object> parameters) { this.parameters = parameters; }

  @Override
  public String toString() {
    return String.format("ToolCall[id=%s, tool=%s, params=%s]",
        toolCallId, toolName, parameters);
  }
}
