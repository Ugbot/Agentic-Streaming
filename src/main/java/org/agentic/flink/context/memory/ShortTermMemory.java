package org.agentic.flink.context.memory;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.MemoryType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * Short-term working memory Ephemeral, cleared on timeout, not persisted Used for: - Current
 * conversation - Tool execution results - Temporary context
 */
@Data
public class ShortTermMemory implements Serializable {

  private List<ContextItem> items;
  private int maxItems;
  private long totalTokens;
  private long lastAccessTime;

  public ShortTermMemory(int maxItems) {
    this.maxItems = maxItems;
    this.items = new ArrayList<>();
    this.totalTokens = 0;
    this.lastAccessTime = System.currentTimeMillis();
  }

  public void add(ContextItem item) {
    if (items.size() >= maxItems) {
      throw new IllegalStateException("Short-term memory full, compaction needed");
    }
    items.add(item);
    totalTokens += item.getTokenCount();
    lastAccessTime = System.currentTimeMillis();
  }

  public void remove(ContextItem item) {
    if (items.remove(item)) {
      totalTokens -= item.getTokenCount();
    }
  }

  public void clear() {
    items.clear();
    totalTokens = 0;
    lastAccessTime = System.currentTimeMillis();
  }

  public List<ContextItem> getRecent(int count) {
    int size = items.size();
    int start = Math.max(0, size - count);
    return new ArrayList<>(items.subList(start, size));
  }

  public List<ContextItem> getOlderThan(long ageMs) {
    long now = System.currentTimeMillis();
    return items.stream()
        .filter(item -> (now - item.getLastAccessedAt()) > ageMs)
        .collect(Collectors.toList());
  }

  public void removeOldItems(long maxAgeMs) {
    List<ContextItem> toRemove = getOlderThan(maxAgeMs);
    toRemove.forEach(this::remove);
  }

  public boolean needsCompaction() {
    return items.size() >= (maxItems * 0.8); // 80% full
  }

  public int size() {
    return items.size();
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }

  public long getTimeSinceLastAccessMs() {
    return System.currentTimeMillis() - lastAccessTime;
  }

  @Override
  public String toString() {
    return String.format(
        "ShortTermMemory[items=%d/%d, tokens=%d, lastAccess=%dms]",
        items.size(), maxItems, totalTokens, getTimeSinceLastAccessMs());
  }
}
