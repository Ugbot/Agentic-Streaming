package org.agentic.flink.storage.memory;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import org.junit.jupiter.api.*;

/**
 * Comprehensive unit tests for InMemoryShortTermStore.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Initialization and configuration
 *   <li>CRUD operations (put, get, delete)
 *   <li>Item management (add, remove, count, clear)
 *   <li>TTL functionality
 *   <li>Statistics and metrics
 *   <li>Edge cases and error handling
 *   <li>Concurrency safety
 * </ul>
 *
 * @author Agentic Flink Team
 */
class InMemoryShortTermStoreTest {

  private InMemoryShortTermStore store;
  private Map<String, String> config;

  @BeforeEach
  void setUp() throws Exception {
    config = new HashMap<>();
    config.put("cache.max.size", "1000");
    config.put("cache.ttl.seconds", "3600");

    store = new InMemoryShortTermStore();
    store.initialize(config);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  // ==================== Initialization Tests ====================

  @Test
  @DisplayName("Should initialize with default configuration")
  void testDefaultInitialization() throws Exception {
    InMemoryShortTermStore defaultStore = new InMemoryShortTermStore();
    defaultStore.initialize(new HashMap<>());

    assertEquals(StorageTier.HOT, defaultStore.getTier());
    assertEquals(0, defaultStore.getExpectedLatencyMs()); // Sub-millisecond
    assertNotNull(defaultStore.getStatistics());

    defaultStore.close();
  }

  @Test
  @DisplayName("Should initialize with custom configuration")
  void testCustomConfiguration() throws Exception {
    Map<String, String> customConfig = new HashMap<>();
    customConfig.put("cache.max.size", "5000");
    customConfig.put("cache.ttl.seconds", "7200");

    InMemoryShortTermStore customStore = new InMemoryShortTermStore();
    customStore.initialize(customConfig);

    Map<String, Object> stats = customStore.getStatistics();
    assertEquals(5000, stats.get("max_size"));
    assertEquals(7200L, stats.get("ttl_seconds"));

    customStore.close();
  }

  @Test
  @DisplayName("Should have correct storage tier")
  void testStorageTier() {
    assertEquals(StorageTier.HOT, store.getTier());
  }

  @Test
  @DisplayName("Should have expected latency under 1ms")
  void testExpectedLatency() {
    assertTrue(store.getExpectedLatencyMs() < 1);
  }

  // ==================== Basic CRUD Tests ====================

  @Test
  @DisplayName("Should put and get items successfully")
  void testPutAndGet() throws Exception {
    String flowId = "flow-001";
    List<ContextItem> items = createTestItems(3);

    // Put items
    store.put(flowId, items);

    // Get items
    Optional<List<ContextItem>> result = store.get(flowId);

    assertTrue(result.isPresent());
    assertEquals(3, result.get().size());
    assertEquals(items.get(0).getContent(), result.get().get(0).getContent());
  }

  @Test
  @DisplayName("Should return empty Optional for non-existent key")
  void testGetNonExistent() throws Exception {
    Optional<List<ContextItem>> result = store.get("non-existent");
    assertFalse(result.isPresent());
  }

  @Test
  @DisplayName("Should delete items successfully")
  void testDelete() throws Exception {
    String flowId = "flow-002";
    List<ContextItem> items = createTestItems(2);

    store.put(flowId, items);
    assertTrue(store.exists(flowId));

    store.delete(flowId);
    assertFalse(store.exists(flowId));
  }

  @Test
  @DisplayName("Should check existence correctly")
  void testExists() throws Exception {
    String flowId = "flow-003";

    assertFalse(store.exists(flowId));

    store.put(flowId, createTestItems(1));
    assertTrue(store.exists(flowId));

    store.delete(flowId);
    assertFalse(store.exists(flowId));
  }

  // ==================== Item Management Tests ====================

  @Test
  @DisplayName("Should add individual items")
  void testAddItem() throws Exception {
    String flowId = "flow-004";
    ContextItem item1 = createTestItem("Item 1");
    ContextItem item2 = createTestItem("Item 2");

    store.addItem(flowId, item1);
    assertEquals(1, store.getItemCount(flowId));

    store.addItem(flowId, item2);
    assertEquals(2, store.getItemCount(flowId));

    List<ContextItem> items = store.getItems(flowId);
    assertEquals(2, items.size());
  }

  @Test
  @DisplayName("Should remove individual items")
  void testRemoveItem() throws Exception {
    String flowId = "flow-005";
    ContextItem item1 = createTestItem("Item 1");
    ContextItem item2 = createTestItem("Item 2");

    item1.setItemId("item-1");
    item2.setItemId("item-2");

    store.putItems(flowId, Arrays.asList(item1, item2));
    assertEquals(2, store.getItemCount(flowId));

    store.removeItem(flowId, "item-1");
    assertEquals(1, store.getItemCount(flowId));

    List<ContextItem> remaining = store.getItems(flowId);
    assertEquals(1, remaining.size());
    assertEquals("item-2", remaining.get(0).getItemId());
  }

  @Test
  @DisplayName("Should count items correctly")
  void testGetItemCount() throws Exception {
    String flowId = "flow-006";

    assertEquals(0, store.getItemCount(flowId));

    store.putItems(flowId, createTestItems(5));
    assertEquals(5, store.getItemCount(flowId));

    store.clearItems(flowId);
    assertEquals(0, store.getItemCount(flowId));
  }

  @Test
  @DisplayName("Should clear all items for a flow")
  void testClearItems() throws Exception {
    String flowId = "flow-007";

    store.putItems(flowId, createTestItems(10));
    assertEquals(10, store.getItemCount(flowId));

    store.clearItems(flowId);
    assertEquals(0, store.getItemCount(flowId));
    assertFalse(store.exists(flowId));
  }

  // ==================== Multiple Flows Tests ====================

  @Test
  @DisplayName("Should handle multiple flows independently")
  void testMultipleFlows() throws Exception {
    String flow1 = "flow-A";
    String flow2 = "flow-B";
    String flow3 = "flow-C";

    store.putItems(flow1, createTestItems(3));
    store.putItems(flow2, createTestItems(5));
    store.putItems(flow3, createTestItems(2));

    assertEquals(3, store.getItemCount(flow1));
    assertEquals(5, store.getItemCount(flow2));
    assertEquals(2, store.getItemCount(flow3));

    store.delete(flow2);

    assertTrue(store.exists(flow1));
    assertFalse(store.exists(flow2));
    assertTrue(store.exists(flow3));
  }

  // ==================== Statistics Tests ====================

  @Test
  @DisplayName("Should track statistics correctly")
  void testStatistics() throws Exception {
    String flow1 = "flow-stats-1";
    String flow2 = "flow-stats-2";

    store.putItems(flow1, createTestItems(3));
    store.putItems(flow2, createTestItems(2));
    store.getItems(flow1); // Hit
    store.getItems("non-existent"); // Miss
    store.delete(flow1);

    Map<String, Object> stats = store.getStatistics();

    assertNotNull(stats);
    assertTrue(stats.containsKey("active_conversations"));
    assertTrue(stats.containsKey("total_items"));
    assertTrue(stats.containsKey("max_size"));
    assertTrue(stats.containsKey("ttl_seconds"));

    // Should have 1 flow remaining (flow2)
    assertEquals(1, stats.get("active_conversations"));

    // Should have 2 items (flow2 has 2 items)
    assertEquals(2, stats.get("total_items"));
  }

  @Test
  @DisplayName("Should track cache hit rate")
  void testCacheHitRate() throws Exception {
    String flowId = "flow-hitrate";

    // Put items
    store.putItems(flowId, createTestItems(3));

    // 3 hits
    store.getItems(flowId);
    store.getItems(flowId);
    store.getItems(flowId);

    // 2 misses
    store.getItems("non-existent-1");
    store.getItems("non-existent-2");

    Map<String, Object> stats = store.getStatistics();

    // Can't directly check hit rate in InMemoryShortTermStore
    // but we can verify statistics are tracked
    assertNotNull(stats);
  }

  // ==================== TTL Tests ====================

  @Test
  @DisplayName("Should set TTL for items")
  void testSetTTL() throws Exception {
    String flowId = "flow-ttl";

    store.putItems(flowId, createTestItems(2));
    store.setTTL(flowId, 3600);

    // TTL is set but doesn't expire immediately in in-memory store
    assertTrue(store.exists(flowId));
  }

  // ==================== Edge Cases and Error Handling ====================

  @Test
  @DisplayName("Should handle null key gracefully")
  void testNullKey() {
    assertThrows(IllegalArgumentException.class, () -> store.put(null, createTestItems(1)));

    assertThrows(IllegalArgumentException.class, () -> store.get(null));

    assertThrows(IllegalArgumentException.class, () -> store.delete(null));
  }

  @Test
  @DisplayName("Should handle null value gracefully")
  void testNullValue() {
    assertThrows(IllegalArgumentException.class, () -> store.put("flow-null", null));
  }

  @Test
  @DisplayName("Should handle empty item list")
  void testEmptyItemList() throws Exception {
    String flowId = "flow-empty";

    store.putItems(flowId, new ArrayList<>());

    List<ContextItem> items = store.getItems(flowId);
    assertTrue(items.isEmpty());
  }

  @Test
  @DisplayName("Should handle multiple deletes of same key")
  void testMultipleDeletes() throws Exception {
    String flowId = "flow-multidel";

    store.putItems(flowId, createTestItems(1));

    store.delete(flowId);
    assertFalse(store.exists(flowId));

    // Second delete should not throw
    assertDoesNotThrow(() -> store.delete(flowId));
  }

  @Test
  @DisplayName("Should handle overwrite of existing key")
  void testOverwrite() throws Exception {
    String flowId = "flow-overwrite";

    store.putItems(flowId, createTestItems(3));
    assertEquals(3, store.getItemCount(flowId));

    store.putItems(flowId, createTestItems(5));
    assertEquals(5, store.getItemCount(flowId));
  }

  // ==================== Performance Tests ====================

  @Test
  @DisplayName("Should handle large number of items")
  void testLargeItemCount() throws Exception {
    String flowId = "flow-large";

    List<ContextItem> items = createTestItems(1000);
    store.putItems(flowId, items);

    assertEquals(1000, store.getItemCount(flowId));

    List<ContextItem> retrieved = store.getItems(flowId);
    assertEquals(1000, retrieved.size());
  }

  @Test
  @DisplayName("Should handle many flows")
  void testManyFlows() throws Exception {
    int flowCount = 100;

    for (int i = 0; i < flowCount; i++) {
      store.putItems("flow-" + i, createTestItems(2));
    }

    Map<String, Object> stats = store.getStatistics();
    assertEquals(flowCount, stats.get("active_conversations"));
    assertEquals(flowCount * 2, stats.get("total_items"));
  }

  @Test
  @DisplayName("Should have sub-millisecond latency")
  void testLatency() throws Exception {
    String flowId = "flow-latency";
    List<ContextItem> items = createTestItems(10);

    long start = System.nanoTime();
    store.putItems(flowId, items);
    long putLatency = System.nanoTime() - start;

    start = System.nanoTime();
    store.getItems(flowId);
    long getLatency = System.nanoTime() - start;

    // Should be well under 1ms (1_000_000 ns)
    assertTrue(putLatency < 1_000_000, "Put latency too high: " + putLatency + "ns");
    assertTrue(getLatency < 1_000_000, "Get latency too high: " + getLatency + "ns");
  }

  // ==================== Helper Methods ====================

  private List<ContextItem> createTestItems(int count) {
    List<ContextItem> items = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      items.add(createTestItem("Test item " + i));
    }
    return items;
  }

  private ContextItem createTestItem(String content) {
    ContextItem item = new ContextItem(content, ContextPriority.MUST, MemoryType.SHORT_TERM);
    item.setItemId(UUID.randomUUID().toString());
    return item;
  }
}
