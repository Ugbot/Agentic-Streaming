package org.agentic.flink.context.compaction;

import org.agentic.flink.context.core.AgentContext;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to compact agent context Sent when context window exceeds threshold
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompactionRequest implements Serializable {

  private String requestId;
  private String flowId;
  private String userId;
  private String agentId;

  private AgentContext context;
  private String originalIntent;

  private Long timestamp;
  private CompactionReason reason;

  public CompactionRequest(AgentContext context, String originalIntent, CompactionReason reason) {
    this.requestId = java.util.UUID.randomUUID().toString();
    this.flowId = context.getFlowId();
    this.userId = context.getUserId();
    this.agentId = context.getAgentId();
    this.context = context;
    this.originalIntent = originalIntent;
    this.reason = reason;
    this.timestamp = System.currentTimeMillis();
  }

  public enum CompactionReason {
    TOKEN_LIMIT_EXCEEDED,
    ITEM_COUNT_EXCEEDED,
    MANUAL_TRIGGER,
    SCHEDULED_COMPACTION
  }
}
