package org.agentic.flink.storage;

import org.agentic.flink.context.core.ContextItem;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Typed cohort of {@link ContextItem}s sharing a semantic role — borrowed in spirit from Apache
 * Flink Agents' {@code BaseLongTermMemory.MemorySet} / {@code MemorySetItem}.
 *
 * <p>Common sets: {@code "facts"}, {@code "decisions"}, {@code "summaries"}. Sets are persisted
 * through {@link MemorySetAccessor}, which namespaces keys in the underlying
 * {@link LongTermMemoryStore#saveFacts}/{@link LongTermMemoryStore#loadFacts} buckets so a single
 * store can host any number of sets without schema changes.
 */
public final class MemorySet implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final Map<String, ContextItem> items;

  public MemorySet(String name) {
    this(name, new HashMap<>());
  }

  public MemorySet(String name, Map<String, ContextItem> items) {
    this.name = Objects.requireNonNull(name, "name");
    this.items = items == null ? new HashMap<>() : new HashMap<>(items);
  }

  public String getName() {
    return name;
  }

  public Map<String, ContextItem> getItems() {
    return items;
  }

  public Collection<ContextItem> entries() {
    return items.values();
  }

  public int size() {
    return items.size();
  }

  public void add(ContextItem item) {
    Objects.requireNonNull(item, "item");
    items.put(item.getItemId(), item);
  }

  public ContextItem get(String itemId) {
    return items.get(itemId);
  }

  public void remove(String itemId) {
    items.remove(itemId);
  }

  public void clear() {
    items.clear();
  }
}
