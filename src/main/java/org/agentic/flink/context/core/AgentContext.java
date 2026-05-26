package org.agentic.flink.context.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Complete agent context containing all memory types
 *
 * <p>Manages short-term, long-term, and steering memory
 */
@Data
@NoArgsConstructor  // Required for Jackson deserialization
public class AgentContext implements Serializable {

  private String agentId;
  private String flowId;
  private String userId;

  private ContextWindow contextWindow;

  // Current intent/goal
  private String currentIntent;

  // State tracking
  private Long createdAt;
  private Long lastUpdatedAt;

  // Custom data
  private Map<String, Object> customData;

  public AgentContext(
      String agentId, String flowId, String userId, int maxTokens, int maxItems) {
    this.agentId = agentId;
    this.flowId = flowId;
    this.userId = userId;
    this.contextWindow = new ContextWindow(maxTokens, maxItems, 0.8);
    this.createdAt = System.currentTimeMillis();
    this.lastUpdatedAt = System.currentTimeMillis();
    this.customData = new HashMap<>();
  }

  public void addContext(ContextItem item) {
    contextWindow.addItem(item);
    this.lastUpdatedAt = System.currentTimeMillis();
  }

  public void removeContext(ContextItem item) {
    contextWindow.removeItem(item);
    this.lastUpdatedAt = System.currentTimeMillis();
  }

  public boolean needsCompaction() {
    return contextWindow.needsCompaction();
  }

  public void updateLastAccess() {
    this.lastUpdatedAt = System.currentTimeMillis();
  }

  public void putCustomData(String key, Object value) {
    if (this.customData == null) {
      this.customData = new HashMap<>();
    }
    this.customData.put(key, value);
  }

  public Object getCustomData(String key) {
    return this.customData != null ? this.customData.get(key) : null;
  }

  public long getAgeMs() {
    return System.currentTimeMillis() - this.createdAt;
  }

  @Override
  public String toString() {
    return String.format(
        "AgentContext[agent=%s, flow=%s, %s, age=%dms]",
        agentId, flowId, contextWindow, getAgeMs());
  }
}
