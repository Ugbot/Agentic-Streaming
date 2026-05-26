package org.agentic.flink.serde;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ToolCallRequest implements Serializable {

  private String requestId;
  private String flowId;
  private String userId;
  private String agentId;

  private String toolId;
  private String toolName;
  private Map<String, Object> parameters;

  private Long timestamp;
  private Long timeoutMs;

  // Execution context
  private Integer iterationNumber;
  private String callReason;

  public ToolCallRequest(
      String flowId, String userId, String agentId, String toolId, Map<String, Object> parameters) {
    this.requestId = java.util.UUID.randomUUID().toString();
    this.flowId = flowId;
    this.userId = userId;
    this.agentId = agentId;
    this.toolId = toolId;
    this.parameters = parameters;
    this.timestamp = System.currentTimeMillis();
    this.timeoutMs = 30000L; // 30 seconds default
  }
}
