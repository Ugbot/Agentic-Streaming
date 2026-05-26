package org.agentic.flink.storage;

import org.agentic.flink.context.core.ContextItem;
import java.util.List;
import java.util.Map;

/**
 * Storage interface for short-term memory (hot tier).
 *
 * <p>Short-term memory stores context items that are actively being used in the current
 * conversation. This is the fastest tier with sub-millisecond access times, typically using
 * in-memory caches or very fast external stores.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Tier: HOT
 *   <li>Latency: &lt;1ms
 *   <li>Scope: Active conversation context (1-50 items typically)
 *   <li>TTL: Minutes to hours
 *   <li>Backends: Caffeine cache, Redis, Hazelcast IMDG
 * </ul>
 *
 * <p>Use cases:
 *
 * <ul>
 *   <li>Current turn context items
 *   <li>Recent tool results
 *   <li>Immediate validation history
 *   <li>Active reasoning chains
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ShortTermMemoryStore store = new InMemoryShortTermStore();
 * store.initialize(config);
 *
 * // Store items for a conversation
 * List<ContextItem> items = Arrays.asList(
 *     new ContextItem("Order lookup result", ContextPriority.MUST),
 *     new ContextItem("User preference", ContextPriority.SHOULD)
 * );
 * store.putItems("flow-001", items);
 *
 * // Retrieve items
 * List<ContextItem> retrieved = store.getItems("flow-001");
 *
 * // Add single item
 * store.addItem("flow-001", new ContextItem("Validation passed", ContextPriority.MUST));
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public interface ShortTermMemoryStore extends StorageProvider<String, List<ContextItem>> {

  /**
   * Store a list of context items for a conversation flow.
   *
   * <p>This replaces any existing items for the given flowId. For incremental updates, use
   * addItem() instead.
   *
   * @param flowId Conversation flow identifier
   * @param items List of context items to store
   * @throws Exception if storage operation fails
   */
  void putItems(String flowId, List<ContextItem> items) throws Exception;

  /**
   * Retrieve all context items for a conversation flow.
   *
   * <p>Returns empty list if no items exist for this flowId.
   *
   * @param flowId Conversation flow identifier
   * @return List of context items, empty if none found
   * @throws Exception if retrieval operation fails
   */
  List<ContextItem> getItems(String flowId) throws Exception;

  /**
   * Add a single context item to an existing conversation.
   *
   * <p>This appends to the existing list of items. If no items exist, creates a new list with this
   * single item.
   *
   * @param flowId Conversation flow identifier
   * @param item Context item to add
   * @throws Exception if storage operation fails
   */
  void addItem(String flowId, ContextItem item) throws Exception;

  /**
   * Remove a specific context item from a conversation.
   *
   * <p>Items are matched by their ID. If the item doesn't exist, this operation is a no-op.
   *
   * @param flowId Conversation flow identifier
   * @param itemId ID of the context item to remove
   * @throws Exception if storage operation fails
   */
  void removeItem(String flowId, String itemId) throws Exception;

  /**
   * Get the count of context items for a conversation.
   *
   * <p>More efficient than retrieving all items when only the count is needed.
   *
   * @param flowId Conversation flow identifier
   * @return Number of context items, 0 if none exist
   * @throws Exception if count operation fails
   */
  int getItemCount(String flowId) throws Exception;

  /**
   * Clear all context items for a conversation.
   *
   * <p>This is more efficient than retrieving and deleting items individually. Used during context
   * compaction or conversation reset.
   *
   * @param flowId Conversation flow identifier
   * @throws Exception if clear operation fails
   */
  void clearItems(String flowId) throws Exception;

  /**
   * Get storage statistics for monitoring.
   *
   * <p>Returns metrics like:
   *
   * <ul>
   *   <li>total_items: Total number of items across all conversations
   *   <li>active_conversations: Number of conversations with items
   *   <li>cache_hit_rate: Hit rate for cache-based implementations (0.0-1.0)
   *   <li>avg_items_per_conversation: Average number of items per conversation
   * </ul>
   *
   * @return Map of metric names to values
   * @throws Exception if statistics retrieval fails
   */
  Map<String, Object> getStatistics() throws Exception;

  /**
   * Set time-to-live for items in a conversation.
   *
   * <p>After the TTL expires, items will be automatically removed. Not all backends support
   * per-conversation TTL; some may use a global TTL.
   *
   * @param flowId Conversation flow identifier
   * @param ttlSeconds Time to live in seconds
   * @throws Exception if TTL setting fails
   */
  void setTTL(String flowId, long ttlSeconds) throws Exception;

  @Override
  default StorageTier getTier() {
    return StorageTier.HOT;
  }

  @Override
  default long getExpectedLatencyMs() {
    return 1; // Sub-millisecond for hot tier
  }
}
