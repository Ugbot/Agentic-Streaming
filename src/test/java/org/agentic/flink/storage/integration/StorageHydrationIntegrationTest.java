package org.agentic.flink.storage.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.config.StorageConfiguration;
import java.util.*;
import org.junit.jupiter.api.*;

/**
 * Integration test demonstrating complete storage hydration workflow.
 *
 * <p>This test simulates a realistic scenario:
 *
 * <ol>
 *   <li>Process events and build up context in HOT storage
 *   <li>Periodically persist conversations to WARM storage
 *   <li>Simulate job restart (clear HOT storage)
 *   <li>Hydrate context from WARM storage on first access
 *   <li>Continue processing with hydrated context
 * </ol>
 *
 * <p>This validates the conversation resumption pattern that is critical for production deployments
 * where Flink jobs may restart due to failures, upgrades, or scaling events.
 *
 * @author Agentic Flink Team
 */
class StorageHydrationIntegrationTest {

  private ShortTermMemoryStore hotStore;
  private LongTermMemoryStore warmStore;
  private String testFlowId;

  @BeforeEach
  void setUp() throws Exception {
    // Create storage configuration with in-memory backends
    StorageConfiguration config =
        StorageConfiguration.builder()
            .withHotTier("memory", createHotConfig())
            .withWarmTier("memory", createWarmConfig())
            .build();

    this.hotStore = config.createShortTermStore();
    this.warmStore = config.createLongTermStore();
    this.testFlowId = "test-flow-" + UUID.randomUUID();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (hotStore != null) {
      hotStore.close();
    }
    if (warmStore != null) {
      warmStore.close();
    }
  }

  // ==================== Basic Hydration Tests ====================

  @Test
  @DisplayName("Should hydrate context from WARM to HOT storage")
  void testBasicHydration() throws Exception {
    String flowId = testFlowId;

    // 1. Create initial context with items
    List<ContextItem> initialItems = createTestItems(5, "Initial message");

    // 2. Save to WARM storage (simulating persistence)
    AgentContext context = createAgentContext(flowId, "user-001", initialItems);
    warmStore.saveContext(flowId, context);

    // 3. Verify NOT in HOT storage yet
    assertFalse(hotStore.exists(flowId));

    // 4. Simulate hydration (what would happen on first access after restart)
    Optional<AgentContext> warmContext = warmStore.loadContext(flowId);
    assertTrue(warmContext.isPresent());

    List<ContextItem> hydratedItems = warmContext.get().getContextWindow().getItems();
    hotStore.putItems(flowId, hydratedItems);

    // 5. Verify context is now in HOT storage
    assertTrue(hotStore.exists(flowId));
    List<ContextItem> hotItems = hotStore.getItems(flowId);
    assertEquals(5, hotItems.size());
    assertEquals("Initial message 0", hotItems.get(0).getContent());
  }

  @Test
  @DisplayName("Should preserve all context properties during hydration")
  void testCompleteHydration() throws Exception {
    String flowId = testFlowId;
    String userId = "user-002";
    String agentId = "agent-test";

    // 1. Create context with metadata
    AgentContext originalContext = new AgentContext(agentId, flowId, userId, 8000, 50);

    ContextItem item1 = createTestItem("Message 1", ContextPriority.MUST);
    ContextItem item2 = createTestItem("Message 2", ContextPriority.SHOULD);
    ContextItem item3 = createTestItem("Message 3", ContextPriority.COULD);

    originalContext.addContext(item1);
    originalContext.addContext(item2);
    originalContext.addContext(item3);

    originalContext.setCurrentIntent("book_flight");
    originalContext.putCustomData("session_id", "session-123");

    // 2. Save to WARM storage
    warmStore.saveContext(flowId, originalContext);

    // 3. Simulate restart - clear HOT storage
    hotStore.clearItems(flowId);

    // 4. Hydrate from WARM
    Optional<AgentContext> hydratedContext = warmStore.loadContext(flowId);
    assertTrue(hydratedContext.isPresent());

    AgentContext loaded = hydratedContext.get();

    // 5. Verify all properties preserved
    assertEquals(flowId, loaded.getFlowId());
    assertEquals(userId, loaded.getUserId());
    assertEquals(agentId, loaded.getAgentId());
    assertEquals("book_flight", loaded.getCurrentIntent());
    assertEquals("session-123", loaded.getCustomData("session_id"));

    // 6. Verify all items preserved with correct priorities
    assertEquals(3, loaded.getContextWindow().getItems().size());

    List<ContextItem> items = loaded.getContextWindow().getItems();
    assertEquals(ContextPriority.MUST, items.get(0).getPriority());
    assertEquals(ContextPriority.SHOULD, items.get(1).getPriority());
    assertEquals(ContextPriority.COULD, items.get(2).getPriority());
  }

  // ==================== Multi-Tier Workflow Tests ====================

  @Test
  @DisplayName("Should handle complete HOT -> WARM -> HOT workflow")
  void testCompleteTierWorkflow() throws Exception {
    String flowId = testFlowId;

    // Phase 1: Active conversation in HOT storage
    List<ContextItem> activeItems = createTestItems(3, "Active");
    hotStore.putItems(flowId, activeItems);
    assertEquals(3, hotStore.getItemCount(flowId));

    // Phase 2: Persist to WARM storage (checkpoint)
    AgentContext context = createAgentContext(flowId, "user-003", activeItems);
    warmStore.saveContext(flowId, context);

    // Phase 3: Continue adding to HOT storage
    hotStore.addItem(flowId, createTestItem("New message 1", ContextPriority.MUST));
    hotStore.addItem(flowId, createTestItem("New message 2", ContextPriority.MUST));
    assertEquals(5, hotStore.getItemCount(flowId));

    // Phase 4: Another checkpoint to WARM
    List<ContextItem> updatedItems = hotStore.getItems(flowId);
    AgentContext updatedContext = createAgentContext(flowId, "user-003", updatedItems);
    warmStore.saveContext(flowId, updatedContext);

    // Phase 5: Simulate restart - clear HOT
    hotStore.clearItems(flowId);
    assertFalse(hotStore.exists(flowId));

    // Phase 6: Hydrate from WARM
    Optional<AgentContext> restored = warmStore.loadContext(flowId);
    assertTrue(restored.isPresent());

    List<ContextItem> restoredItems = restored.get().getContextWindow().getItems();
    hotStore.putItems(flowId, restoredItems);

    // Phase 7: Verify complete restoration
    assertEquals(5, hotStore.getItemCount(flowId));
  }

  @Test
  @DisplayName("Should handle incremental persistence pattern")
  void testIncrementalPersistence() throws Exception {
    String flowId = testFlowId;

    // Simulate processing messages with periodic persistence
    for (int i = 0; i < 15; i++) {
      // Add message to HOT storage
      ContextItem item = createTestItem("Message " + i, ContextPriority.MUST);
      hotStore.addItem(flowId, item);

      // Every 5 messages, persist to WARM
      if ((i + 1) % 5 == 0) {
        List<ContextItem> currentItems = hotStore.getItems(flowId);
        AgentContext checkpoint = createAgentContext(flowId, "user-004", currentItems);
        warmStore.saveContext(flowId, checkpoint);
      }
    }

    // Verify final state in HOT
    assertEquals(15, hotStore.getItemCount(flowId));

    // Verify final state in WARM
    Optional<AgentContext> warmContext = warmStore.loadContext(flowId);
    assertTrue(warmContext.isPresent());
    assertEquals(15, warmContext.get().getContextWindow().getItems().size());
  }

  // ==================== Multi-User Hydration Tests ====================

  @Test
  @DisplayName("Should hydrate multiple user conversations independently")
  void testMultiUserHydration() throws Exception {
    String flow1 = "flow-user-1";
    String flow2 = "flow-user-2";
    String flow3 = "flow-user-3";

    // Create conversations for 3 users
    AgentContext ctx1 = createAgentContext(flow1, "user-1", createTestItems(3, "User1"));
    AgentContext ctx2 = createAgentContext(flow2, "user-2", createTestItems(5, "User2"));
    AgentContext ctx3 = createAgentContext(flow3, "user-3", createTestItems(2, "User3"));

    warmStore.saveContext(flow1, ctx1);
    warmStore.saveContext(flow2, ctx2);
    warmStore.saveContext(flow3, ctx3);

    // Simulate restart
    hotStore.clearItems(flow1);
    hotStore.clearItems(flow2);
    hotStore.clearItems(flow3);

    // Hydrate user 2's conversation
    Optional<AgentContext> user2Context = warmStore.loadContext(flow2);
    assertTrue(user2Context.isPresent());

    List<ContextItem> user2Items = user2Context.get().getContextWindow().getItems();
    hotStore.putItems(flow2, user2Items);

    // Verify only user 2's conversation is in HOT
    assertFalse(hotStore.exists(flow1));
    assertTrue(hotStore.exists(flow2));
    assertFalse(hotStore.exists(flow3));

    // Verify correct data
    assertEquals(5, hotStore.getItemCount(flow2));
    assertTrue(hotStore.getItems(flow2).get(0).getContent().contains("User2"));
  }

  // ==================== Facts Persistence Tests ====================

  @Test
  @DisplayName("Should persist and hydrate facts alongside context")
  void testFactsPersistence() throws Exception {
    String flowId = testFlowId;

    // Create context with facts
    AgentContext context = new AgentContext("agent-001", flowId, "user-005", 8000, 50);

    // Add regular context items
    context.addContext(createTestItem("Message 1", ContextPriority.MUST));

    // Save context
    warmStore.saveContext(flowId, context);

    // Add facts
    Map<String, ContextItem> facts = new HashMap<>();
    facts.put("fact1", createTestItem("User prefers window seats", ContextPriority.MUST));
    facts.put("fact2", createTestItem("User is a vegetarian", ContextPriority.MUST));
    facts.put("fact3", createTestItem("User speaks English and Spanish", ContextPriority.SHOULD));

    warmStore.saveFacts(flowId, facts);

    // Simulate restart
    hotStore.clearItems(flowId);

    // Hydrate context and facts
    Optional<AgentContext> restoredContext = warmStore.loadContext(flowId);
    Map<String, ContextItem> restoredFacts = warmStore.loadFacts(flowId);

    // Verify restoration
    assertTrue(restoredContext.isPresent());
    assertEquals(3, restoredFacts.size());
    assertTrue(restoredFacts.containsKey("fact1"));
    assertTrue(restoredFacts.containsKey("fact2"));
    assertTrue(restoredFacts.containsKey("fact3"));
  }

  // ==================== Metadata Tests ====================

  @Test
  @DisplayName("Should preserve conversation metadata during hydration")
  void testMetadataPersistence() throws Exception {
    String flowId = testFlowId;
    String userId = "user-006";

    // Create and save context
    AgentContext context = createAgentContext(flowId, userId, createTestItems(2, "Test"));
    warmStore.saveContext(flowId, context);

    // Retrieve metadata
    Map<String, Object> metadata = warmStore.getConversationMetadata(flowId);

    assertNotNull(metadata);
    assertEquals(flowId, metadata.get("flowId"));
    assertEquals(userId, metadata.get("userId"));
    assertTrue(metadata.containsKey("created_at"));
    assertTrue(metadata.containsKey("last_updated_at"));
  }

  // ==================== Error Handling Tests ====================

  @Test
  @DisplayName("Should handle hydration of non-existent conversation")
  void testHydrateNonExistent() throws Exception {
    String nonExistentFlowId = "does-not-exist";

    Optional<AgentContext> result = warmStore.loadContext(nonExistentFlowId);

    assertFalse(result.isPresent());
    assertFalse(hotStore.exists(nonExistentFlowId));
  }

  @Test
  @DisplayName("Should handle empty context hydration")
  void testHydrateEmptyContext() throws Exception {
    String flowId = testFlowId;

    // Create context with no items
    AgentContext emptyContext = new AgentContext("agent-001", flowId, "user-007", 8000, 50);
    warmStore.saveContext(flowId, emptyContext);

    // Hydrate
    Optional<AgentContext> restored = warmStore.loadContext(flowId);

    assertTrue(restored.isPresent());
    assertEquals(0, restored.get().getContextWindow().getItems().size());
  }

  // ==================== Performance Tests ====================

  @Test
  @DisplayName("Should hydrate large context efficiently")
  void testLargeContextHydration() throws Exception {
    String flowId = testFlowId;

    // Create large context (40 items - within max of 50)
    List<ContextItem> largeContext = createTestItems(40, "Large");
    AgentContext context = createAgentContext(flowId, "user-008", largeContext);

    long saveStart = System.currentTimeMillis();
    warmStore.saveContext(flowId, context);
    long saveTime = System.currentTimeMillis() - saveStart;

    long loadStart = System.currentTimeMillis();
    Optional<AgentContext> restored = warmStore.loadContext(flowId);
    long loadTime = System.currentTimeMillis() - loadStart;

    assertTrue(restored.isPresent());
    assertEquals(40, restored.get().getContextWindow().getItems().size());

    // Should be fast (under 100ms for in-memory)
    assertTrue(saveTime < 100, "Save time too slow: " + saveTime + "ms");
    assertTrue(loadTime < 100, "Load time too slow: " + loadTime + "ms");
  }

  // ==================== Helper Methods ====================

  private Map<String, String> createHotConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("cache.max.size", "10000");
    config.put("cache.ttl.seconds", "3600");
    return config;
  }

  private Map<String, String> createWarmConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("cache.max.size", "5000");
    config.put("cache.ttl.seconds", "86400");
    return config;
  }

  private List<ContextItem> createTestItems(int count, String prefix) {
    List<ContextItem> items = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      items.add(createTestItem(prefix + " " + i, ContextPriority.MUST));
    }
    return items;
  }

  private ContextItem createTestItem(String content, ContextPriority priority) {
    ContextItem item = new ContextItem(content, priority, MemoryType.SHORT_TERM);
    item.setItemId(UUID.randomUUID().toString());
    return item;
  }

  private AgentContext createAgentContext(
      String flowId, String userId, List<ContextItem> items) {
    AgentContext context = new AgentContext("test-agent", flowId, userId, 8000, 50);

    for (ContextItem item : items) {
      context.addContext(item);
    }

    return context;
  }
}
