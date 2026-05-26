package org.agentic.flink.context.inverse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Result of inverse RAG operation (storing context to long-term) */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InverseRagResult implements Serializable {

  private String requestId;
  private String flowId;

  private Map<String, String> storedItems; // itemId -> storage_id
  private Map<String, String> failedItems; // itemId -> error_message

  private long timestamp;

  public InverseRagResult(String requestId, String flowId) {
    this.requestId = requestId;
    this.flowId = flowId;
    this.storedItems = new HashMap<>();
    this.failedItems = new HashMap<>();
    this.timestamp = System.currentTimeMillis();
  }

  public void addStoredItem(String itemId, String storageId) {
    storedItems.put(itemId, storageId);
  }

  public void addFailedItem(String itemId, String errorMessage) {
    failedItems.put(itemId, errorMessage);
  }

  public int getStoredCount() {
    return storedItems.size();
  }

  public int getFailedCount() {
    return failedItems.size();
  }

  @Override
  public String toString() {
    return String.format(
        "InverseRagResult[stored=%d, failed=%d]", getStoredCount(), getFailedCount());
  }
}
