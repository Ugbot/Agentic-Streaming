package org.agentic.flink.storage;

import org.agentic.flink.context.core.ContextItem;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Reads and writes {@link MemorySet}s through an underlying {@link LongTermMemoryStore} by
 * namespacing the fact-map keys with {@code "${setName}::${itemId}"}.
 *
 * <p>This is a thin typed view over the existing {@code saveFacts} / {@code loadFacts} surface;
 * no storage-layer change is required. When upstream Apache Flink Agents stabilizes its
 * {@code BaseLongTermMemory.MemorySet} API, this class is the natural bridge — its public
 * surface mirrors the upstream concept vocabulary.
 */
public final class MemorySetAccessor {

  private final LongTermMemoryStore store;

  public MemorySetAccessor(LongTermMemoryStore store) {
    this.store = Objects.requireNonNull(store, "store");
  }

  /** Save (overwriting) the entire set under the given flow id. */
  public void save(String flowId, MemorySet set) throws Exception {
    Objects.requireNonNull(flowId, "flowId");
    Objects.requireNonNull(set, "set");

    // Load the full fact bucket, replace this set's slice, write back.
    Map<String, ContextItem> all = new HashMap<>(store.loadFacts(flowId));
    // Strip any existing entries for this set.
    String prefix = set.getName() + "::";
    all.entrySet().removeIf(e -> e.getKey().startsWith(prefix));
    for (Map.Entry<String, ContextItem> e : set.getItems().entrySet()) {
      all.put(prefix + e.getKey(), e.getValue());
    }
    store.saveFacts(flowId, all);
  }

  /** Load the named set, or an empty {@link MemorySet} if it does not exist. */
  public MemorySet load(String flowId, String setName) throws Exception {
    Objects.requireNonNull(flowId, "flowId");
    Objects.requireNonNull(setName, "setName");

    Map<String, ContextItem> all = store.loadFacts(flowId);
    String prefix = setName + "::";
    Map<String, ContextItem> slice = new LinkedHashMap<>();
    for (Map.Entry<String, ContextItem> e : all.entrySet()) {
      if (e.getKey().startsWith(prefix)) {
        slice.put(e.getKey().substring(prefix.length()), e.getValue());
      }
    }
    return new MemorySet(setName, slice);
  }

  /** Add a single item to a named set without rewriting the whole slice. */
  public void addItem(String flowId, String setName, ContextItem item) throws Exception {
    Objects.requireNonNull(item, "item");
    String prefix = setName + "::";
    store.addFact(flowId, prefix + item.getItemId(), item);
  }

  /** Remove a single item from a named set. */
  public void removeItem(String flowId, String setName, String itemId) throws Exception {
    String prefix = setName + "::";
    store.removeFact(flowId, prefix + itemId);
  }
}
