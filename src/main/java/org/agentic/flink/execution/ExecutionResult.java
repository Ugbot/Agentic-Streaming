package org.agentic.flink.execution;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import java.util.List;

/**
 * Result of agent execution containing output and metadata.
 *
 * @author Agentic Flink Team
 */
public class ExecutionResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String flowId;
  private final String agentId;
  private final ExecutionStatus status;
  private final String output;
  private final String errorMessage;
  private final List<AgentEvent> events;
  private final List<ToolCallResult> toolCalls;

  private ExecutionResult(
      String flowId,
      String agentId,
      ExecutionStatus status,
      String output,
      String errorMessage,
      List<AgentEvent> events,
      List<ToolCallResult> toolCalls) {
    this.flowId = flowId;
    this.agentId = agentId;
    this.status = status;
    this.output = output;
    this.errorMessage = errorMessage;
    this.events = events;
    this.toolCalls = toolCalls;
  }

  public String getFlowId() { return flowId; }
  public String getAgentId() { return agentId; }
  public ExecutionStatus getStatus() { return status; }
  public String getOutput() { return output; }
  public String getErrorMessage() { return errorMessage; }
  public List<AgentEvent> getEvents() { return events; }
  public List<ToolCallResult> getToolCalls() { return toolCalls; }

  public boolean isSuccess() {
    return status == ExecutionStatus.SUCCESS;
  }

  public boolean isFailure() {
    return status == ExecutionStatus.FAILURE;
  }

  // Factory methods

  public static ExecutionResult success(
      String flowId, String agentId, String output, List<AgentEvent> events, List<ToolCallResult> toolCalls) {
    return new ExecutionResult(flowId, agentId, ExecutionStatus.SUCCESS, output, null, events, toolCalls);
  }

  public static ExecutionResult failure(
      String flowId, String agentId, String errorMessage, List<AgentEvent> events) {
    return new ExecutionResult(flowId, agentId, ExecutionStatus.FAILURE, null, errorMessage, events, null);
  }

  public static ExecutionResult maxIterations(
      String flowId, String agentId, String message, List<AgentEvent> events, List<ToolCallResult> toolCalls) {
    return new ExecutionResult(flowId, agentId, ExecutionStatus.MAX_ITERATIONS, null, message, events, toolCalls);
  }

  public enum ExecutionStatus {
    SUCCESS,
    FAILURE,
    MAX_ITERATIONS,
    TIMEOUT
  }
}
