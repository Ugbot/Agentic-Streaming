package org.agentic.flink.storage.postgres;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import org.junit.jupiter.api.*;

/**
 * Unit tests for PostgresConversationStore.
 *
 * <p>These tests use H2 database in PostgreSQL compatibility mode to avoid requiring a running
 * PostgreSQL instance for tests.
 *
 * <p>Test coverage:
 *
 * <ul>
 *   <li>Initialization and schema creation
 *   <li>Context save and load operations
 *   <li>Facts storage and retrieval
 *   <li>Conversation lifecycle (exists, delete)
 *   <li>Multi-user conversation management
 *   <li>Metadata operations
 *   <li>Error handling
 * </ul>
 *
 * @author Agentic Flink Team
 */
class PostgresConversationStoreTest {

  private PostgresConversationStore store;
  private Map<String, String> config;
  private String testFlowId;

  @BeforeEach
  void setUp() throws Exception {
    // Use H2 in PostgreSQL compatibility mode for testing
    // Use unique database name per test to avoid cross-test contamination
    String dbName = "test_db_" + UUID.randomUUID().toString().replace("-", "");
    config = new HashMap<>();
    config.put("postgres.url", "jdbc:h2:mem:" + dbName + ";MODE=PostgreSQL");
    config.put("postgres.user", "sa");
    config.put("postgres.password", "");
    config.put("postgres.pool.max.size", "5");
    config.put("postgres.pool.min.idle", "1");
    config.put("postgres.auto.create.tables", "true");

    store = new PostgresConversationStore();
    store.initialize(config);

    testFlowId = "test-flow-" + UUID.randomUUID();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  // ==================== Initialization Tests ====================

  @Test
  @DisplayName("Should initialize with correct tier and latency")
  void testInitialization() {
    assertEquals(StorageTier.WARM, store.getTier());
    assertEquals(10, store.getExpectedLatencyMs());
    assertEquals("PostgresConversationStore", store.getProviderName());
  }

  @Test
  @DisplayName("Should create tables automatically when enabled")
  void testAutoTableCreation() throws Exception {
    // Tables should be created during initialization
    // Verify by attempting to save a context
    AgentContext context = createTestContext(testFlowId, "user-001");
    assertDoesNotThrow(() -> store.saveContext(testFlowId, context));
  }

  // ==================== Context Save/Load Tests ====================

  @Test
  @DisplayName("Should save and load context successfully")
  void testSaveAndLoadContext() throws Exception {
    AgentContext context = createTestContext(testFlowId, "user-001");
    context.addContext(createTestItem("Message 1", ContextPriority.MUST));
    context.addContext(createTestItem("Message 2", ContextPriority.SHOULD));

    store.saveContext(testFlowId, context);

    Optional<AgentContext> loaded = store.loadContext(testFlowId);
    assertTrue(loaded.isPresent());
    assertEquals(testFlowId, loaded.get().getFlowId());
    assertEquals("user-001", loaded.get().getUserId());
    assertEquals(2, loaded.get().getContextWindow().getItems().size());
  }

  @Test
  @DisplayName("Should return empty optional for non-existent context")
  void testLoadNonExistentContext() throws Exception {
    Optional<AgentContext> loaded = store.loadContext("does-not-exist");
    assertFalse(loaded.isPresent());
  }

  @Test
  @DisplayName("Should update existing context on save")
  void testUpdateContext() throws Exception {
    // Save initial context
    AgentContext context1 = createTestContext(testFlowId, "user-001");
    context1.addContext(createTestItem("Message 1", ContextPriority.MUST));
    store.saveContext(testFlowId, context1);

    // Update with new context
    AgentContext context2 = createTestContext(testFlowId, "user-001");
    context2.addContext(createTestItem("Message 1", ContextPriority.MUST));
    context2.addContext(createTestItem("Message 2", ContextPriority.MUST));
    store.saveContext(testFlowId, context2);

    // Load and verify
    Optional<AgentContext> loaded = store.loadContext(testFlowId);
    assertTrue(loaded.isPresent());
    assertEquals(2, loaded.get().getContextWindow().getItems().size());
  }

  @Test
  @DisplayName("Should support put/get via StorageProvider interface")
  void testPutAndGet() throws Exception {
    AgentContext context = createTestContext(testFlowId, "user-001");
    store.put(testFlowId, context);

    Optional<AgentContext> loaded = store.get(testFlowId);
    assertTrue(loaded.isPresent());
    assertEquals(testFlowId, loaded.get().getFlowId());
  }

  // ==================== Conversation Lifecycle Tests ====================

  @Test
  @DisplayName("Should check conversation existence correctly")
  void testConversationExists() throws Exception {
    assertFalse(store.conversationExists(testFlowId));
    assertFalse(store.exists(testFlowId));

    AgentContext context = createTestContext(testFlowId, "user-001");
    store.saveContext(testFlowId, context);

    assertTrue(store.conversationExists(testFlowId));
    assertTrue(store.exists(testFlowId));
  }

  @Test
  @DisplayName("Should delete conversation and all associated data")
  void testDeleteConversation() throws Exception {
    // Create conversation with context and facts
    AgentContext context = createTestContext(testFlowId, "user-001");
    store.saveContext(testFlowId, context);

    Map<String, ContextItem> facts = new HashMap<>();
    facts.put("fact1", createTestItem("Fact 1", ContextPriority.MUST));
    store.saveFacts(testFlowId, facts);

    assertTrue(store.conversationExists(testFlowId));

    // Delete
    store.deleteConversation(testFlowId);

    // Verify deletion
    assertFalse(store.conversationExists(testFlowId));
    Optional<AgentContext> loaded = store.loadContext(testFlowId);
    assertFalse(loaded.isPresent());
    Map<String, ContextItem> loadedFacts = store.loadFacts(testFlowId);
    assertTrue(loadedFacts.isEmpty());
  }

  @Test
  @DisplayName("Should support delete via StorageProvider interface")
  void testDelete() throws Exception {
    AgentContext context = createTestContext(testFlowId, "user-001");
    store.saveContext(testFlowId, context);

    assertTrue(store.exists(testFlowId));

    store.delete(testFlowId);

    assertFalse(store.exists(testFlowId));
  }

  // ==================== Facts Storage Tests ====================

  @Test
  @DisplayName("Should save and load facts")
  void testSaveAndLoadFacts() throws Exception {
    Map<String, ContextItem> facts = new HashMap<>();
    facts.put("fact1", createTestItem("User prefers window seats", ContextPriority.MUST));
    facts.put("fact2", createTestItem("User is vegetarian", ContextPriority.MUST));
    facts.put("fact3", createTestItem("User speaks English", ContextPriority.SHOULD));

    store.saveFacts(testFlowId, facts);

    Map<String, ContextItem> loaded = store.loadFacts(testFlowId);
    assertEquals(3, loaded.size());
    assertTrue(loaded.containsKey("fact1"));
    assertTrue(loaded.containsKey("fact2"));
    assertTrue(loaded.containsKey("fact3"));
    assertEquals("User prefers window seats", loaded.get("fact1").getContent());
  }

  @Test
  @DisplayName("Should return empty map for non-existent facts")
  void testLoadNonExistentFacts() throws Exception {
    Map<String, ContextItem> facts = store.loadFacts("does-not-exist");
    assertNotNull(facts);
    assertTrue(facts.isEmpty());
  }

  @Test
  @DisplayName("Should add single fact")
  void testAddFact() throws Exception {
    ContextItem fact = createTestItem("User tier: Premium", ContextPriority.MUST);
    store.addFact(testFlowId, "user_tier", fact);

    Map<String, ContextItem> facts = store.loadFacts(testFlowId);
    assertEquals(1, facts.size());
    assertTrue(facts.containsKey("user_tier"));
  }

  @Test
  @DisplayName("Should update existing fact")
  void testUpdateFact() throws Exception {
    // Add initial fact
    ContextItem fact1 = createTestItem("User tier: Basic", ContextPriority.SHOULD);
    store.addFact(testFlowId, "user_tier", fact1);

    // Update fact
    ContextItem fact2 = createTestItem("User tier: Premium", ContextPriority.MUST);
    store.addFact(testFlowId, "user_tier", fact2);

    // Verify update
    Map<String, ContextItem> facts = store.loadFacts(testFlowId);
    assertEquals(1, facts.size());
    assertEquals("User tier: Premium", facts.get("user_tier").getContent());
  }

  @Test
  @DisplayName("Should remove fact")
  void testRemoveFact() throws Exception {
    // Add facts
    store.addFact(testFlowId, "fact1", createTestItem("Fact 1", ContextPriority.MUST));
    store.addFact(testFlowId, "fact2", createTestItem("Fact 2", ContextPriority.MUST));

    assertEquals(2, store.loadFacts(testFlowId).size());

    // Remove one fact
    store.removeFact(testFlowId, "fact1");

    // Verify
    Map<String, ContextItem> facts = store.loadFacts(testFlowId);
    assertEquals(1, facts.size());
    assertFalse(facts.containsKey("fact1"));
    assertTrue(facts.containsKey("fact2"));
  }

  @Test
  @DisplayName("Should handle empty facts gracefully")
  void testSaveEmptyFacts() throws Exception {
    Map<String, ContextItem> emptyFacts = new HashMap<>();
    assertDoesNotThrow(() -> store.saveFacts(testFlowId, emptyFacts));

    Map<String, ContextItem> loaded = store.loadFacts(testFlowId);
    assertTrue(loaded.isEmpty());
  }

  // ==================== Multi-User Tests ====================

  @Test
  @DisplayName("Should list active conversations")
  void testListActiveConversations() throws Exception {
    // Create multiple conversations
    String flow1 = "flow-001";
    String flow2 = "flow-002";
    String flow3 = "flow-003";

    store.saveContext(flow1, createTestContext(flow1, "user-1"));
    store.saveContext(flow2, createTestContext(flow2, "user-2"));
    store.saveContext(flow3, createTestContext(flow3, "user-3"));

    List<String> active = store.listActiveConversations();
    assertEquals(3, active.size());
    assertTrue(active.contains(flow1));
    assertTrue(active.contains(flow2));
    assertTrue(active.contains(flow3));
  }

  @Test
  @DisplayName("Should list conversations for specific user")
  void testListConversationsForUser() throws Exception {
    String user1 = "user-001";
    String user2 = "user-002";

    // Create conversations for different users
    store.saveContext("flow-u1-1", createTestContext("flow-u1-1", user1));
    store.saveContext("flow-u1-2", createTestContext("flow-u1-2", user1));
    store.saveContext("flow-u2-1", createTestContext("flow-u2-1", user2));

    // List for user1
    List<String> user1Conversations = store.listConversationsForUser(user1);
    assertEquals(2, user1Conversations.size());
    assertTrue(user1Conversations.contains("flow-u1-1"));
    assertTrue(user1Conversations.contains("flow-u1-2"));
    assertFalse(user1Conversations.contains("flow-u2-1"));

    // List for user2
    List<String> user2Conversations = store.listConversationsForUser(user2);
    assertEquals(1, user2Conversations.size());
    assertTrue(user2Conversations.contains("flow-u2-1"));
  }

  @Test
  @DisplayName("Should return empty list for user with no conversations")
  void testListConversationsForUserNoResults() throws Exception {
    List<String> conversations = store.listConversationsForUser("unknown-user");
    assertNotNull(conversations);
    assertTrue(conversations.isEmpty());
  }

  // ==================== Metadata Tests ====================

  @Test
  @DisplayName("Should retrieve conversation metadata")
  void testGetConversationMetadata() throws Exception {
    String userId = "user-001";
    String agentId = "agent-test";

    AgentContext context = new AgentContext(agentId, testFlowId, userId, 8000, 50);
    store.saveContext(testFlowId, context);

    Map<String, Object> metadata = store.getConversationMetadata(testFlowId);

    assertNotNull(metadata);
    assertEquals(testFlowId, metadata.get("flowId"));
    assertEquals(userId, metadata.get("userId"));
    assertEquals(agentId, metadata.get("agentId"));
    assertTrue(metadata.containsKey("created_at"));
    assertTrue(metadata.containsKey("last_updated_at"));
  }

  @Test
  @DisplayName("Should return empty metadata for non-existent conversation")
  void testGetMetadataNonExistent() throws Exception {
    Map<String, Object> metadata = store.getConversationMetadata("does-not-exist");
    assertNotNull(metadata);
    assertTrue(metadata.isEmpty());
  }

  @Test
  @DisplayName("Should update metadata on context update")
  void testMetadataUpdate() throws Exception {
    AgentContext context = createTestContext(testFlowId, "user-001");
    store.saveContext(testFlowId, context);

    Map<String, Object> metadata1 = store.getConversationMetadata(testFlowId);
    long firstUpdate = (Long) metadata1.get("last_updated_at");

    // Wait a bit and update
    Thread.sleep(100);
    store.saveContext(testFlowId, context);

    Map<String, Object> metadata2 = store.getConversationMetadata(testFlowId);
    long secondUpdate = (Long) metadata2.get("last_updated_at");

    assertTrue(secondUpdate >= firstUpdate);
  }

  // ==================== TTL Tests ====================

  @Test
  @DisplayName("Should accept setConversationTTL (no-op for PostgreSQL)")
  void testSetConversationTTL() throws Exception {
    AgentContext context = createTestContext(testFlowId, "user-001");
    store.saveContext(testFlowId, context);

    // PostgreSQL doesn't support automatic TTL, so this is a no-op
    assertDoesNotThrow(() -> store.setConversationTTL(testFlowId, 3600));

    // Context should still exist
    assertTrue(store.conversationExists(testFlowId));
  }

  // ==================== Error Handling Tests ====================

  @Test
  @DisplayName("Should throw exception for null flowId in saveContext")
  void testSaveContextNullFlowId() {
    AgentContext context = createTestContext("test", "user-001");
    assertThrows(IllegalArgumentException.class, () -> store.saveContext(null, context));
  }

  @Test
  @DisplayName("Should throw exception for null context in saveContext")
  void testSaveContextNullContext() {
    assertThrows(IllegalArgumentException.class, () -> store.saveContext(testFlowId, null));
  }

  @Test
  @DisplayName("Should throw exception for null flowId in loadContext")
  void testLoadContextNullFlowId() {
    assertThrows(IllegalArgumentException.class, () -> store.loadContext(null));
  }

  @Test
  @DisplayName("Should return false for null flowId in conversationExists")
  void testConversationExistsNullFlowId() throws Exception {
    assertFalse(store.conversationExists(null));
  }

  @Test
  @DisplayName("Should throw exception for null parameters in saveFacts")
  void testSaveFactsNullParams() {
    Map<String, ContextItem> facts = new HashMap<>();
    assertThrows(IllegalArgumentException.class, () -> store.saveFacts(null, facts));
    assertThrows(IllegalArgumentException.class, () -> store.saveFacts(testFlowId, null));
  }

  @Test
  @DisplayName("Should throw exception for null parameters in addFact")
  void testAddFactNullParams() {
    ContextItem fact = createTestItem("Test", ContextPriority.MUST);
    assertThrows(IllegalArgumentException.class, () -> store.addFact(null, "fact1", fact));
    assertThrows(IllegalArgumentException.class, () -> store.addFact(testFlowId, null, fact));
    assertThrows(IllegalArgumentException.class, () -> store.addFact(testFlowId, "fact1", null));
  }

  @Test
  @DisplayName("Should throw exception for null userId in listConversationsForUser")
  void testListConversationsNullUser() {
    assertThrows(IllegalArgumentException.class, () -> store.listConversationsForUser(null));
  }

  // ==================== Integration Tests ====================

  @Test
  @DisplayName("Should handle complete workflow: save, load, update, delete")
  void testCompleteWorkflow() throws Exception {
    // 1. Create and save context
    AgentContext context = createTestContext(testFlowId, "user-001");
    context.addContext(createTestItem("Message 1", ContextPriority.MUST));
    store.saveContext(testFlowId, context);

    // 2. Add facts
    Map<String, ContextItem> facts = new HashMap<>();
    facts.put("pref1", createTestItem("Preference 1", ContextPriority.MUST));
    store.saveFacts(testFlowId, facts);

    // 3. Verify existence
    assertTrue(store.conversationExists(testFlowId));

    // 4. Load and verify
    Optional<AgentContext> loaded = store.loadContext(testFlowId);
    assertTrue(loaded.isPresent());
    assertEquals(1, loaded.get().getContextWindow().getItems().size());

    Map<String, ContextItem> loadedFacts = store.loadFacts(testFlowId);
    assertEquals(1, loadedFacts.size());

    // 5. Update context
    context.addContext(createTestItem("Message 2", ContextPriority.MUST));
    store.saveContext(testFlowId, context);

    // 6. Verify update
    Optional<AgentContext> updated = store.loadContext(testFlowId);
    assertTrue(updated.isPresent());
    assertEquals(2, updated.get().getContextWindow().getItems().size());

    // 7. Delete conversation
    store.deleteConversation(testFlowId);

    // 8. Verify deletion
    assertFalse(store.conversationExists(testFlowId));
  }

  @Test
  @DisplayName("Should handle multiple concurrent conversations")
  void testMultipleConversations() throws Exception {
    int numConversations = 10;
    List<String> flowIds = new ArrayList<>();

    // Create multiple conversations
    for (int i = 0; i < numConversations; i++) {
      String flowId = "flow-" + i;
      flowIds.add(flowId);

      AgentContext context = createTestContext(flowId, "user-" + (i % 3));
      context.addContext(createTestItem("Message " + i, ContextPriority.MUST));
      store.saveContext(flowId, context);
    }

    // Verify all exist
    for (String flowId : flowIds) {
      assertTrue(store.conversationExists(flowId));
    }

    // Verify count
    List<String> active = store.listActiveConversations();
    assertEquals(numConversations, active.size());
  }

  // ==================== Helper Methods ====================

  private AgentContext createTestContext(String flowId, String userId) {
    return new AgentContext("test-agent", flowId, userId, 8000, 50);
  }

  private ContextItem createTestItem(String content, ContextPriority priority) {
    ContextItem item = new ContextItem(content, priority, MemoryType.SHORT_TERM);
    item.setItemId(UUID.randomUUID().toString());
    return item;
  }
}
