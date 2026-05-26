package org.agentic.flink.context.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Context window with size limits and item management */
@Data
@NoArgsConstructor  // Required for Jackson deserialization
public class ContextWindow implements Serializable {

  private int maxTokens;
  private int currentTokens;
  private int maxItems;
  private double compactionThreshold; // 0.8 = compact at 80% full

  private List<ContextItem> items;

  public ContextWindow(int maxTokens, int maxItems, double compactionThreshold) {
    this.maxTokens = maxTokens;
    this.maxItems = maxItems;
    this.compactionThreshold = compactionThreshold;
    this.items = new ArrayList<>();
    this.currentTokens = 0;
  }

  public boolean canFit(ContextItem item) {
    return (currentTokens + item.getTokenCount()) <= maxTokens && items.size() < maxItems;
  }

  public boolean needsCompaction() {
    double usage = (double) currentTokens / maxTokens;
    return usage >= compactionThreshold || items.size() >= maxItems;
  }

  public void addItem(ContextItem item) {
    if (!canFit(item)) {
      throw new IllegalStateException("Context window full, compaction needed");
    }
    items.add(item);
    currentTokens += item.getTokenCount();
  }

  public void removeItem(ContextItem item) {
    if (items.remove(item)) {
      currentTokens -= item.getTokenCount();
    }
  }

  public void clear() {
    items.clear();
    currentTokens = 0;
  }

  public List<ContextItem> getItemsByPriority(ContextPriority priority) {
    return items.stream()
        .filter(item -> item.getPriority() == priority)
        .collect(Collectors.toList());
  }

  public List<ContextItem> getItemsByMemoryType(MemoryType memoryType) {
    return items.stream()
        .filter(item -> item.getMemoryType() == memoryType)
        .collect(Collectors.toList());
  }

  public int size() {
    return items.size();
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }

  public double getUsageRatio() {
    return (double) currentTokens / maxTokens;
  }

  public int getAvailableTokens() {
    return maxTokens - currentTokens;
  }

  public List<ContextItem> getItems() {
    return new ArrayList<>(items);
  }

  @Override
  public String toString() {
    return String.format(
        "ContextWindow[tokens=%d/%d (%.1f%%), items=%d/%d]",
        currentTokens, maxTokens, getUsageRatio() * 100, items.size(), maxItems);
  }
}
