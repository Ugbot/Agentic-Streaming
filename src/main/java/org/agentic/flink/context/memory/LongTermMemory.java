package org.agentic.flink.context.memory;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * Long-term persistent memory Immutable facts, persisted across restarts Used for: - Hard facts
 * about the world - User preferences - Domain knowledge - Historical context
 */
@Data
public class LongTermMemory implements Serializable {

  private Map<String, ContextItem> facts; // key = fact_id
  private long createdAt;
  private long lastUpdatedAt;

  public LongTermMemory() {
    this.facts = new HashMap<>();
    this.createdAt = System.currentTimeMillis();
    this.lastUpdatedAt = System.currentTimeMillis();
  }

  public void addFact(ContextItem fact) {
    // Only MUST priority items should go to long-term memory
    if (fact.getPriority() != ContextPriority.MUST) {
      throw new IllegalArgumentException(
          "Only MUST priority items can be added to long-term memory");
    }
    facts.put(fact.getItemId(), fact);
    lastUpdatedAt = System.currentTimeMillis();
  }

  public ContextItem getFact(String factId) {
    return facts.get(factId);
  }

  public boolean hasFact(String factId) {
    return facts.containsKey(factId);
  }

  public void removeFact(String factId) {
    facts.remove(factId);
    lastUpdatedAt = System.currentTimeMillis();
  }

  public List<ContextItem> getAllFacts() {
    return new ArrayList<>(facts.values());
  }

  public List<ContextItem> getFactsByTag(String intentTag) {
    return facts.values().stream()
        .filter(fact -> intentTag.equals(fact.getIntentTag()))
        .collect(Collectors.toList());
  }

  public List<ContextItem> getRecentFacts(int count) {
    return facts.values().stream()
        .sorted((a, b) -> Long.compare(b.getCreatedAt(), a.getCreatedAt()))
        .limit(count)
        .collect(Collectors.toList());
  }

  public int size() {
    return facts.size();
  }

  public boolean isEmpty() {
    return facts.isEmpty();
  }

  public long getTotalTokens() {
    return facts.values().stream().mapToLong(ContextItem::getTokenCount).sum();
  }

  @Override
  public String toString() {
    return String.format(
        "LongTermMemory[facts=%d, tokens=%d, age=%dms]",
        facts.size(), getTotalTokens(), System.currentTimeMillis() - createdAt);
  }
}
