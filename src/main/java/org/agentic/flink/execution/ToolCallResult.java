package org.agentic.flink.execution;

import java.io.Serializable;

/**
 * Result of a tool execution.
 *
 * @author Agentic Flink Team
 */
public class ToolCallResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private String toolCallId;
  private String toolName;
  private Object result;
  private boolean success;
  private String error;
  private long executionTimeMs;

  public ToolCallResult() {}

  public ToolCallResult(String toolCallId, String toolName, Object result, boolean success) {
    this.toolCallId = toolCallId;
    this.toolName = toolName;
    this.result = result;
    this.success = success;
  }

  public static ToolCallResult success(String toolCallId, String toolName, Object result) {
    return new ToolCallResult(toolCallId, toolName, result, true);
  }

  public static ToolCallResult failure(String toolCallId, String toolName, String error) {
    ToolCallResult result = new ToolCallResult();
    result.setToolCallId(toolCallId);
    result.setToolName(toolName);
    result.setSuccess(false);
    result.setError(error);
    return result;
  }

  public String getToolCallId() { return toolCallId; }
  public void setToolCallId(String toolCallId) { this.toolCallId = toolCallId; }

  public String getToolName() { return toolName; }
  public void setToolName(String toolName) { this.toolName = toolName; }

  public Object getResult() { return result; }
  public void setResult(Object result) { this.result = result; }

  public boolean isSuccess() { return success; }
  public void setSuccess(boolean success) { this.success = success; }

  public String getError() { return error; }
  public void setError(String error) { this.error = error; }

  public long getExecutionTimeMs() { return executionTimeMs; }
  public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }

  @Override
  public String toString() {
    return String.format("ToolCallResult[id=%s, tool=%s, success=%s]",
        toolCallId, toolName, success);
  }
}
