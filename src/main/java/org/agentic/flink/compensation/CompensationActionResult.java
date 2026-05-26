package org.agentic.flink.compensation;

import java.io.Serializable;

/**
 * Result of executing a single compensation action.
 *
 * @author Agentic Flink Team
 */
public class CompensationActionResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private String actionName;
  private boolean success;
  private Object result;
  private String errorMessage;
  private long executionTimeMs;

  public String getActionName() {
    return actionName;
  }

  public void setActionName(String actionName) {
    this.actionName = actionName;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
    this.result = result;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public long getExecutionTimeMs() {
    return executionTimeMs;
  }

  public void setExecutionTimeMs(long executionTimeMs) {
    this.executionTimeMs = executionTimeMs;
  }

  @Override
  public String toString() {
    return String.format("CompensationActionResult[action=%s, success=%s, error=%s]",
        actionName, success, errorMessage);
  }
}
