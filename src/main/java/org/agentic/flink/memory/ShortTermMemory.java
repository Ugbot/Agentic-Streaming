package org.agentic.flink.memory;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import java.util.List;
import java.util.Optional;

/**
 * Per-operator short-term memory abstraction backed by Flink keyed state.
 *
 * <p>Unlike the older {@code ShortTermMemoryStore}, this interface is <b>not</b> a serializable
 * external store. It lives inside a Flink {@code RichFunction}: implementations are constructed
 * from a serializable {@link ShortTermMemorySpec} in {@code open()}, where they obtain access to
 * the runtime's keyed state via {@code RuntimeContext}.
 *
 * <p>All methods operate on the operator's <i>current key</i> — there is no {@code flowId}
 * argument because Flink supplies the key. This is the operational consequence of making Flink
 * state canonical: short-term memory is implicitly scoped to whatever the upstream {@code keyBy}
 * selected.
 *
 * <p>Default implementation: {@link FlinkStateShortTermMemory}.
 */
public interface ShortTermMemory {

  /** Returns the agent context for the current key, or empty if none has been hydrated. */
  Optional<AgentContext> getContext() throws Exception;

  /** Stores the agent context for the current key, replacing any prior value. */
  void putContext(AgentContext context) throws Exception;

  /** Adds or replaces a context item under its {@link ContextItem#getItemId()}. */
  void putItem(ContextItem item) throws Exception;

  /** Returns a single item by id, or empty if absent. */
  Optional<ContextItem> getItem(String itemId) throws Exception;

  /** Removes a single item by id; no-op if absent. */
  void removeItem(String itemId) throws Exception;

  /** Returns every active item for the current key. */
  List<ContextItem> items() throws Exception;

  /** Returns the number of active items for the current key. */
  int size() throws Exception;

  /** Removes every item for the current key. The context value is left untouched. */
  void clearItems() throws Exception;

  /**
   * Sum of {@link ContextItem#getTokenCount()} across all active items. Used by compaction logic
   * to decide when to evict.
   */
  int totalTokens() throws Exception;
}
