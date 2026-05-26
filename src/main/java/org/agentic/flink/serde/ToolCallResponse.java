package org.agentic.flink.serde;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ToolCallResponse implements Serializable {

  private String requestId;
  private String flowId;
  private String userId;
  private String agentId;

  private String toolId;
  private String toolName;

  private boolean success;
  private Object result;
  private String errorMessage;
  private String errorCode;

  private Long startTime;
  private Long endTime;
  private Long durationMs;

  public ToolCallResponse(String requestId, String flowId, String userId, String agentId) {
    this.requestId = requestId;
    this.flowId = flowId;
    this.userId = userId;
    this.agentId = agentId;
    this.startTime = System.currentTimeMillis();
  }

  public void complete(Object result) {
    this.success = true;
    this.result = result;
    this.endTime = System.currentTimeMillis();
    this.durationMs = this.endTime - this.startTime;
  }

  public void fail(String errorMessage, String errorCode) {
    this.success = false;
    this.errorMessage = errorMessage;
    this.errorCode = errorCode;
    this.endTime = System.currentTimeMillis();
    this.durationMs = this.endTime - this.startTime;
  }
}
