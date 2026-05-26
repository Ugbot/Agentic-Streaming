package org.agentic.flink.storage.memory;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import org.junit.jupiter.api.*;

/**
 * Comprehensive unit tests for {@link InMemoryLongTermStore}.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Initialization and configuration
 *   <li>saveContext / loadContext round-trip
 *   <li>saveFacts / loadFacts / addFact / removeFact
 *   <li>Conversation lifecycle (exists, delete, list, metadata)
 *   <li>Multi-user isolation
 *   <li>Error conditions (null inputs)
 * </ul>
 *
 * @author Agentic Flink Team
 */
class InMemoryLongTermStoreTest {

  private InMemoryLongTermStore store;

  @BeforeEach
  void setUp() throws Exception {
    store = new InMemoryLongTermStore();
    Map<String, String> config = new HashMap<>();
    config.put("cache.max.size", "1000");
    config.put("cache.ttl.seconds", "3600");
    store.initialize(config);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  // ==================== Initialization ====================

  @Test
  @DisplayName("Should initialize with correct tier and latency")
  void testInitialization() {
    assertEquals(StorageTier.WARM, store.getTier());
    assertTrue(store.getExpectedLatencyMs() < 1);
    assertEquals("InMemoryLongTermStore", store.getProviderName());
  }

  @Test
  @DisplayName("Should initialize with null config (uses defaults)")
  void testInitializeWithNullConfig() throws Exception {
    InMemoryLongTermStore nullConfigStore = new InMemoryLongTermStore();
    assertDoesNotThrow(() -> nullConfigStore.initialize(null));
    nullConfigStore.close();
  }

  @Test
  @DisplayName("Should initialize with empty config (uses defaults)")
  void testInitializeWithEmptyConfig() throws Exception {
    InMemoryLongTermStore emptyConfigStore = new InMemoryLongTermStore();
    emptyConfigStore.initialize(new HashMap<>());
    // Should be usable immediately
    assertDoesNotThrow(emptyConfigStore::listActiveConversations);
    emptyConfigStore.close();
  }

  // ==================== saveContext / loadContext ====================

  @Test
  @DisplayName("Should save and load a context successfully")
  void testSaveAndLoadContext() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    AgentContext ctx = createContext("agent-1", flowId, "user-1");

    store.saveContext(flowId, ctx);

    Optional<AgentContext> loaded = store.loadContext(flowId);
    assertTrue(loaded.isPresent());
    assertEquals("agent-1", loaded.get().getAgentId());
    assertEquals(flowId, loaded.get().getFlowId());
    assertEquals("user-1", loaded.get().getUserId());
  }

  @Test
  @DisplayName("Should return empty Optional for non-existent flow")
  void testLoadNonExistentContext() throws Exception {
    Optional<AgentContext> loaded = store.loadContext("does-not-exist-" + UUID.randomUUID());
    assertFalse(loaded.isPresent());
  }

  @Test
  @DisplayName("Should overwrite context on second save")
  void testOverwriteContext() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    AgentContext ctx1 = createContext("agent-v1", flowId, "user-1");
    AgentContext ctx2 = createContext("agent-v2", flowId, "user-1");

    store.saveContext(flowId, ctx1);
    store.saveContext(flowId, ctx2);

    Optional<AgentContext> loaded = store.loadContext(flowId);
    assertTrue(loaded.isPresent());
    assertEquals("agent-v2", loaded.get().getAgentId());
  }

  @Test
  @DisplayName("put/get should delegate to saveContext/loadContext")
  void testPutGetDelegation() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    AgentContext ctx = createContext("agent-1", flowId, "user-1");

    store.put(flowId, ctx);

    Optional<AgentContext> loaded = store.get(flowId);
    assertTrue(loaded.isPresent());
    assertEquals("agent-1", loaded.get().getAgentId());
  }

  // ==================== conversationExists / deleteConversation ====================

  @Test
  @DisplayName("conversationExists returns false before save and true after")
  void testConversationExists() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();

    assertFalse(store.conversationExists(flowId));

    store.saveContext(flowId, createContext("a", flowId, "u"));

    assertTrue(store.conversationExists(flowId));
  }

  @Test
  @DisplayName("deleteConversation removes context, facts, and metadata")
  void testDeleteConversation() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.saveContext(flowId, createContext("a", flowId, "u"));
    store.addFact(flowId, "fact-1", createFact("some fact"));

    assertTrue(store.conversationExists(flowId));
    assertFalse(store.loadFacts(flowId).isEmpty());

    store.deleteConversation(flowId);

    assertFalse(store.conversationExists(flowId));
    assertTrue(store.loadFacts(flowId).isEmpty());
    assertTrue(store.getConversationMetadata(flowId).isEmpty());
  }

  @Test
  @DisplayName("delete delegates to deleteConversation")
  void testDeleteDelegation() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.saveContext(flowId, createContext("a", flowId, "u"));

    assertTrue(store.exists(flowId));

    store.delete(flowId);

    assertFalse(store.exists(flowId));
  }

  // ==================== saveFacts / loadFacts ====================

  @Test
  @DisplayName("Should save and load facts for a flow")
  void testSaveAndLoadFacts() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    Map<String, ContextItem> factsMap = new HashMap<>();
    factsMap.put("user_tier", createFact("premium"));
    factsMap.put("language", createFact("en"));

    store.saveFacts(flowId, factsMap);

    Map<String, ContextItem> loaded = store.loadFacts(flowId);
    assertEquals(2, loaded.size());
    assertEquals("premium", loaded.get("user_tier").getContent());
    assertEquals("en", loaded.get("language").getContent());
  }

  @Test
  @DisplayName("loadFacts should return empty map for non-existent flow")
  void testLoadFactsNonExistent() throws Exception {
    Map<String, ContextItem> facts = store.loadFacts("no-such-flow-" + UUID.randomUUID());
    assertNotNull(facts);
    assertTrue(facts.isEmpty());
  }

  @Test
  @DisplayName("saveFacts should replace existing facts entirely")
  void testSaveFactsReplaces() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    Map<String, ContextItem> original = new HashMap<>();
    original.put("key-a", createFact("value-a"));
    original.put("key-b", createFact("value-b"));
    store.saveFacts(flowId, original);

    Map<String, ContextItem> replacement = new HashMap<>();
    replacement.put("key-c", createFact("value-c"));
    store.saveFacts(flowId, replacement);

    Map<String, ContextItem> loaded = store.loadFacts(flowId);
    assertEquals(1, loaded.size());
    assertTrue(loaded.containsKey("key-c"));
    assertFalse(loaded.containsKey("key-a"));
  }

  // ==================== addFact / removeFact ====================

  @Test
  @DisplayName("addFact should add a single fact without replacing others")
  void testAddFact() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.addFact(flowId, "fact-1", createFact("first"));
    store.addFact(flowId, "fact-2", createFact("second"));

    Map<String, ContextItem> loaded = store.loadFacts(flowId);
    assertEquals(2, loaded.size());
    assertEquals("first", loaded.get("fact-1").getContent());
    assertEquals("second", loaded.get("fact-2").getContent());
  }

  @Test
  @DisplayName("addFact should overwrite an existing fact with the same id")
  void testAddFactOverwrite() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.addFact(flowId, "fact-1", createFact("original"));
    store.addFact(flowId, "fact-1", createFact("updated"));

    Map<String, ContextItem> loaded = store.loadFacts(flowId);
    assertEquals(1, loaded.size());
    assertEquals("updated", loaded.get("fact-1").getContent());
  }

  @Test
  @DisplayName("removeFact should remove only the targeted fact")
  void testRemoveFact() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.addFact(flowId, "keep", createFact("kept"));
    store.addFact(flowId, "remove", createFact("removed"));

    store.removeFact(flowId, "remove");

    Map<String, ContextItem> loaded = store.loadFacts(flowId);
    assertEquals(1, loaded.size());
    assertTrue(loaded.containsKey("keep"));
    assertFalse(loaded.containsKey("remove"));
  }

  @Test
  @DisplayName("removeFact on non-existent fact should not throw")
  void testRemoveNonExistentFact() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    // No facts exist yet -- should be a no-op
    assertDoesNotThrow(() -> store.removeFact(flowId, "ghost"));
  }

  // ==================== Multi-User Isolation ====================

  @Test
  @DisplayName("Conversations for different users should be isolated")
  void testMultiUserIsolation() throws Exception {
    String flowA = "flow-" + UUID.randomUUID();
    String flowB = "flow-" + UUID.randomUUID();
    String userA = "user-alice-" + UUID.randomUUID();
    String userB = "user-bob-" + UUID.randomUUID();

    store.saveContext(flowA, createContext("agent", flowA, userA));
    store.saveContext(flowB, createContext("agent", flowB, userB));

    // Each user should only see their own conversations
    List<String> aliceConvos = store.listConversationsForUser(userA);
    List<String> bobConvos = store.listConversationsForUser(userB);

    assertEquals(1, aliceConvos.size());
    assertTrue(aliceConvos.contains(flowA));
    assertFalse(aliceConvos.contains(flowB));

    assertEquals(1, bobConvos.size());
    assertTrue(bobConvos.contains(flowB));
    assertFalse(bobConvos.contains(flowA));
  }

  @Test
  @DisplayName("Deleting one user's conversation should not affect another user")
  void testDeleteIsolation() throws Exception {
    String flowA = "flow-" + UUID.randomUUID();
    String flowB = "flow-" + UUID.randomUUID();
    String userA = "user-" + UUID.randomUUID();
    String userB = "user-" + UUID.randomUUID();

    store.saveContext(flowA, createContext("agent", flowA, userA));
    store.saveContext(flowB, createContext("agent", flowB, userB));

    store.deleteConversation(flowA);

    assertFalse(store.conversationExists(flowA));
    assertTrue(store.conversationExists(flowB));

    Optional<AgentContext> bobCtx = store.loadContext(flowB);
    assertTrue(bobCtx.isPresent());
    assertEquals(userB, bobCtx.get().getUserId());
  }

  @Test
  @DisplayName("Facts for different flows should be isolated")
  void testFactIsolation() throws Exception {
    String flowA = "flow-" + UUID.randomUUID();
    String flowB = "flow-" + UUID.randomUUID();

    store.addFact(flowA, "tier", createFact("premium"));
    store.addFact(flowB, "tier", createFact("free"));

    assertEquals("premium", store.loadFacts(flowA).get("tier").getContent());
    assertEquals("free", store.loadFacts(flowB).get("tier").getContent());
  }

  // ==================== Listing ====================

  @Test
  @DisplayName("listActiveConversations should track all saved conversations")
  void testListActiveConversations() throws Exception {
    String flow1 = "flow-" + UUID.randomUUID();
    String flow2 = "flow-" + UUID.randomUUID();

    store.saveContext(flow1, createContext("a", flow1, "u1"));
    store.saveContext(flow2, createContext("a", flow2, "u2"));

    List<String> active = store.listActiveConversations();
    assertTrue(active.contains(flow1));
    assertTrue(active.contains(flow2));
    assertTrue(active.size() >= 2);
  }

  @Test
  @DisplayName("listConversationsForUser should return empty list for unknown user")
  void testListConversationsForUnknownUser() throws Exception {
    List<String> convos = store.listConversationsForUser("nonexistent-" + UUID.randomUUID());
    assertNotNull(convos);
    assertTrue(convos.isEmpty());
  }

  // ==================== Metadata ====================

  @Test
  @DisplayName("getConversationMetadata should return metadata after save")
  void testGetConversationMetadata() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.saveContext(flowId, createContext("my-agent", flowId, "my-user"));

    Map<String, Object> meta = store.getConversationMetadata(flowId);
    assertFalse(meta.isEmpty());
    assertEquals(flowId, meta.get("flowId"));
    assertEquals("my-user", meta.get("userId"));
    assertEquals("my-agent", meta.get("agentId"));
    assertNotNull(meta.get("created_at"));
    assertNotNull(meta.get("last_updated_at"));
  }

  @Test
  @DisplayName("getConversationMetadata should return empty map for unknown flow")
  void testGetMetadataUnknownFlow() throws Exception {
    Map<String, Object> meta =
        store.getConversationMetadata("nonexistent-" + UUID.randomUUID());
    assertNotNull(meta);
    assertTrue(meta.isEmpty());
  }

  // ==================== Error Conditions (null inputs) ====================

  @Test
  @DisplayName("saveContext should reject null flowId")
  void testSaveContextNullFlowId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> store.saveContext(null, createContext("a", "f", "u")));
  }

  @Test
  @DisplayName("saveContext should reject null context")
  void testSaveContextNullContext() {
    assertThrows(IllegalArgumentException.class, () -> store.saveContext("flow-1", null));
  }

  @Test
  @DisplayName("loadContext should reject null flowId")
  void testLoadContextNullFlowId() {
    assertThrows(IllegalArgumentException.class, () -> store.loadContext(null));
  }

  @Test
  @DisplayName("deleteConversation should reject null flowId")
  void testDeleteNullFlowId() {
    assertThrows(IllegalArgumentException.class, () -> store.deleteConversation(null));
  }

  @Test
  @DisplayName("conversationExists should return false for null flowId")
  void testConversationExistsNull() throws Exception {
    assertFalse(store.conversationExists(null));
  }

  @Test
  @DisplayName("saveFacts should reject null flowId")
  void testSaveFactsNullFlowId() {
    assertThrows(IllegalArgumentException.class, () -> store.saveFacts(null, new HashMap<>()));
  }

  @Test
  @DisplayName("saveFacts should reject null facts map")
  void testSaveFactsNullMap() {
    assertThrows(IllegalArgumentException.class, () -> store.saveFacts("flow-1", null));
  }

  @Test
  @DisplayName("loadFacts should reject null flowId")
  void testLoadFactsNullFlowId() {
    assertThrows(IllegalArgumentException.class, () -> store.loadFacts(null));
  }

  @Test
  @DisplayName("addFact should reject null arguments")
  void testAddFactNullArgs() {
    assertThrows(
        IllegalArgumentException.class, () -> store.addFact(null, "id", createFact("v")));
    assertThrows(
        IllegalArgumentException.class, () -> store.addFact("flow", null, createFact("v")));
    assertThrows(IllegalArgumentException.class, () -> store.addFact("flow", "id", null));
  }

  @Test
  @DisplayName("removeFact should reject null arguments")
  void testRemoveFactNullArgs() {
    assertThrows(IllegalArgumentException.class, () -> store.removeFact(null, "id"));
    assertThrows(IllegalArgumentException.class, () -> store.removeFact("flow", null));
  }

  @Test
  @DisplayName("listConversationsForUser should reject null userId")
  void testListConversationsNullUser() {
    assertThrows(IllegalArgumentException.class, () -> store.listConversationsForUser(null));
  }

  @Test
  @DisplayName("getConversationMetadata should reject null flowId")
  void testGetMetadataNullFlowId() {
    assertThrows(IllegalArgumentException.class, () -> store.getConversationMetadata(null));
  }

  // ==================== Close / Resource Management ====================

  @Test
  @DisplayName("close should clear all data and be idempotent")
  void testClose() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    store.saveContext(flowId, createContext("a", flowId, "u"));

    store.close();

    // Second close should not throw
    assertDoesNotThrow(() -> store.close());
  }

  // ==================== Helpers ====================

  private AgentContext createContext(String agentId, String flowId, String userId) {
    return new AgentContext(agentId, flowId, userId, 4096, 100);
  }

  private ContextItem createFact(String content) {
    return new ContextItem(content, ContextPriority.MUST, MemoryType.LONG_TERM);
  }
}
