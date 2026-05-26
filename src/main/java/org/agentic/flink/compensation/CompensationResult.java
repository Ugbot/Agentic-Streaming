package org.agentic.flink.compensation;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of a compensation operation (saga rollback).
 *
 * <p>Contains the overall result of executing multiple compensation actions,
 * including success/failure counts and individual action results.
 *
 * @author Agentic Flink Team
 */
public class CompensationResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private String flowId;
  private AgentEvent triggerEvent;
  private boolean success;
  private List<CompensationActionResult> actionResults;
  private int successCount;
  private int failureCount;
  private long totalExecutionTimeMs;

  public CompensationResult() {
    this.actionResults = new ArrayList<>();
  }

  public String getFlowId() {
    return flowId;
  }

  public void setFlowId(String flowId) {
    this.flowId = flowId;
  }

  public AgentEvent getTriggerEvent() {
    return triggerEvent;
  }

  public void setTriggerEvent(AgentEvent triggerEvent) {
    this.triggerEvent = triggerEvent;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public List<CompensationActionResult> getActionResults() {
    return actionResults;
  }

  public void setActionResults(List<CompensationActionResult> actionResults) {
    this.actionResults = actionResults;
  }

  public int getSuccessCount() {
    return successCount;
  }

  public void setSuccessCount(int successCount) {
    this.successCount = successCount;
  }

  public int getFailureCount() {
    return failureCount;
  }

  public void setFailureCount(int failureCount) {
    this.failureCount = failureCount;
  }

  public long getTotalExecutionTimeMs() {
    return totalExecutionTimeMs;
  }

  public void setTotalExecutionTimeMs(long totalExecutionTimeMs) {
    this.totalExecutionTimeMs = totalExecutionTimeMs;
  }

  @Override
  public String toString() {
    return String.format("CompensationResult[flow=%s, success=%s, actions=%d, succeeded=%d, failed=%d]",
        flowId, success, actionResults.size(), successCount, failureCount);
  }
}
