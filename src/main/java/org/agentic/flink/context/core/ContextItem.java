package org.agentic.flink.context.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Individual context item with content, metadata, and priority */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ContextItem implements Serializable {

  private String itemId;
  private String content;
  private ContextPriority priority;
  private MemoryType memoryType;

  // Relevancy
  private Double relevancyScore;
  private String intentTag; // What intent this relates to

  // Temporal
  private Long createdAt;
  private Long lastAccessedAt;
  private Integer accessCount;

  // Sizing
  private Integer tokenCount;
  private Integer characterCount;

  // Metadata
  private Map<String, String> metadata;

  public ContextItem(String content, ContextPriority priority, MemoryType memoryType) {
    this.itemId = java.util.UUID.randomUUID().toString();
    this.content = content;
    this.priority = priority;
    this.memoryType = memoryType;
    this.createdAt = System.currentTimeMillis();
    this.lastAccessedAt = System.currentTimeMillis();
    this.accessCount = 0;
    this.tokenCount = estimateTokens(content);
    this.characterCount = content.length();
    this.metadata = new HashMap<>();
  }

  public void access() {
    this.lastAccessedAt = System.currentTimeMillis();
    this.accessCount++;
  }

  public long getAgeMs() {
    return System.currentTimeMillis() - this.createdAt;
  }

  public long getTimeSinceLastAccessMs() {
    return System.currentTimeMillis() - this.lastAccessedAt;
  }

  public boolean isStale(long maxAgeMs) {
    return getTimeSinceLastAccessMs() > maxAgeMs;
  }

  public double getTemporalRelevancy(long currentTime) {
    // Exponential decay based on time since last access
    long timeSinceAccess = currentTime - this.lastAccessedAt;
    double hoursSinceAccess = timeSinceAccess / (1000.0 * 60 * 60);
    return Math.exp(-0.1 * hoursSinceAccess); // Decay factor
  }

  public void addMetadata(String key, String value) {
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.put(key, value);
  }

  public String getMetadata(String key) {
    return this.metadata != null ? this.metadata.get(key) : null;
  }

  private int estimateTokens(String text) {
    // Rough estimation: ~4 characters per token
    return text.length() / 4;
  }

  @Override
  public String toString() {
    return String.format(
        "ContextItem[id=%s, priority=%s, tokens=%d, age=%dms]",
        itemId, priority, tokenCount, getAgeMs());
  }
}
