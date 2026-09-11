package org.agentic.flink.storage.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.FlinkSerializationHarness;
import org.agentic.flink.storage.StorageTier;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link PostgresConversationStore} against a real PostgreSQL (Testcontainers on Podman). Covers
 * the PostgreSQL {@code INSERT ... ON CONFLICT DO UPDATE} upserts, concurrent writers on the same
 * conversation, and use of the store after Flink-style serialization.
 */
@Tag("integration")
class PostgresConversationStoreTest {

  private static PostgresTestDatabase database;

  private PostgresConversationStore store;
  private String flowId;

  @BeforeAll
  static void startDatabase() {
    database = PostgresTestDatabase.start();
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) {
      database.close();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    store = new PostgresConversationStore();
    store.initialize(database.storeConfig());
    flowId = "flow-" + UUID.randomUUID();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.close();
  }

  @Test
  @DisplayName("reports tier, latency and provider name")
  void metadata() {
    assertEquals(StorageTier.WARM, store.getTier());
    assertEquals(10, store.getExpectedLatencyMs());
    assertEquals("PostgresConversationStore", store.getProviderName());
  }

  @Test
  @DisplayName("save then save again upserts the same row instead of failing on the primary key")
  void contextUpsertReplacesRow() throws Exception {
    String userA = "user-" + UUID.randomUUID();
    String userB = "user-" + UUID.randomUUID();
    AgentContext first = context(flowId, userA);
    first.addContext(item("first-" + UUID.randomUUID()));
    store.saveContext(flowId, first);

    AgentContext second = context(flowId, userB);
    String marker = "second-" + UUID.randomUUID();
    second.addContext(item(marker));
    store.saveContext(flowId, second);

    Optional<AgentContext> loaded = store.loadContext(flowId);
    assertTrue(loaded.isPresent());
    assertEquals(userB, loaded.get().getUserId());
    assertEquals(1, loaded.get().getContextWindow().getItems().size());
    assertEquals(marker, loaded.get().getContextWindow().getItems().get(0).getContent());
    assertEquals(1, countRows("agent_contexts", flowId));
    assertEquals(List.of(flowId), store.listConversationsForUser(userB));
    assertTrue(store.listConversationsForUser(userA).isEmpty());
  }

  @Test
  @DisplayName("facts upsert by (flow_id, fact_id) and saveFacts replaces the set")
  void factUpsert() throws Exception {
    String factId = "fact-" + UUID.randomUUID();
    store.addFact(flowId, factId, item("v1-" + UUID.randomUUID()));
    String latest = "v2-" + UUID.randomUUID();
    store.addFact(flowId, factId, item(latest));

    Map<String, ContextItem> facts = store.loadFacts(flowId);
    assertEquals(1, facts.size());
    assertEquals(latest, facts.get(factId).getContent());

    Map<String, ContextItem> replacement = new HashMap<>();
    int n = 2 + ThreadLocalRandom.current().nextInt(5);
    for (int i = 0; i < n; i++) {
      replacement.put("fact-" + UUID.randomUUID(), item("r-" + UUID.randomUUID()));
    }
    store.saveFacts(flowId, replacement);
    Map<String, ContextItem> reloaded = store.loadFacts(flowId);
    assertEquals(replacement.keySet(), reloaded.keySet());
    assertEquals(n, countRows("agent_facts", flowId));
  }

  @Test
  @DisplayName("concurrent writers on the same conversation all succeed and leave one consistent row")
  void concurrentWritersSameConversation() throws Exception {
    int writers = 8 + ThreadLocalRandom.current().nextInt(8);
    int writesPerWriter = 10 + ThreadLocalRandom.current().nextInt(10);
    String userId = "user-" + UUID.randomUUID();
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<List<String>>> results = new ArrayList<>();
    try {
      for (int w = 0; w < writers; w++) {
        int writerIndex = w;
        results.add(
            pool.submit(
                (Callable<List<String>>)
                    () -> {
                      List<String> written = new ArrayList<>();
                      start.await();
                      for (int i = 0; i < writesPerWriter; i++) {
                        String content = "w" + writerIndex + "-" + i + "-" + UUID.randomUUID();
                        AgentContext ctx = context(flowId, userId);
                        ctx.addContext(item(content));
                        store.saveContext(flowId, ctx);
                        store.addFact(flowId, "writer-" + writerIndex, item(content));
                        written.add(content);
                      }
                      return written;
                    }));
      }
      start.countDown();
      List<String> allWritten = new ArrayList<>();
      for (Future<List<String>> f : results) {
        allWritten.addAll(f.get(2, TimeUnit.MINUTES));
      }
      assertEquals(writers * writesPerWriter, allWritten.size());
    } finally {
      pool.shutdownNow();
    }

    assertEquals(1, countRows("agent_contexts", flowId));
    Optional<AgentContext> loaded = store.loadContext(flowId);
    assertTrue(loaded.isPresent());
    assertEquals(1, loaded.get().getContextWindow().getItems().size());
    assertTrue(
        loaded.get().getContextWindow().getItems().get(0).getContent().matches("w\\d+-\\d+-.*"),
        "final row must be one of the writers' payloads");

    Map<String, ContextItem> facts = store.loadFacts(flowId);
    assertEquals(writers, facts.size());
    for (int w = 0; w < writers; w++) {
      ContextItem fact = facts.get("writer-" + w);
      assertNotNull(fact, "fact for writer " + w);
      assertTrue(fact.getContent().startsWith("w" + w + "-"), fact.getContent());
    }
  }

  @Test
  @DisplayName("store serialized with Flink's InstantiationUtil reopens its pool and keeps working")
  void usableAfterFlinkSerialization() throws Exception {
    String userId = "user-" + UUID.randomUUID();
    store.saveContext(flowId, context(flowId, userId));

    byte[] bytes = InstantiationUtil.serializeObject(store);
    PostgresConversationStore copy =
        InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
    try {
      assertTrue(copy.conversationExists(flowId));
      String other = "flow-" + UUID.randomUUID();
      copy.saveContext(other, context(other, userId));
      assertTrue(store.conversationExists(other));
    } finally {
      copy.close();
    }
  }

  @Test
  @DisplayName("store shipped inside a Flink job graph writes to the database from the task")
  void usableInsideFlinkJob() throws Exception {
    String userId = "user-" + UUID.randomUUID();
    String taskFlow = "flow-" + UUID.randomUUID();
    String input = "in-" + UUID.randomUUID();
    List<String> out =
        FlinkSerializationHarness.viaJobGraph(
            store,
            s -> {
              try {
                s.saveContext(taskFlow, context(taskFlow, userId));
                s.addFact(taskFlow, "origin", item("task"));
                return s.loadContext(taskFlow).orElseThrow().getUserId();
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            },
            List.of(input));
    assertEquals(List.of(input + "=" + userId), out);
    assertTrue(store.conversationExists(taskFlow));
    assertEquals("task", store.loadFacts(taskFlow).get("origin").getContent());
  }

  @Test
  @DisplayName("an unreachable configured PostgreSQL fails initialize loudly")
  void unreachableDatabaseFailsLoudly() {
    Map<String, String> config = database.storeConfig();
    config.put("postgres.url", "jdbc:postgresql://127.0.0.1:1/" + UUID.randomUUID());
    PostgresConversationStore broken = new PostgresConversationStore();
    assertThrows(Exception.class, () -> broken.initialize(config));
  }

  @Test
  @DisplayName("delete removes context, facts and user index")
  void deleteConversation() throws Exception {
    String userId = "user-" + UUID.randomUUID();
    store.saveContext(flowId, context(flowId, userId));
    store.addFact(flowId, "f", item("x-" + UUID.randomUUID()));
    store.deleteConversation(flowId);
    assertFalse(store.conversationExists(flowId));
    assertTrue(store.loadFacts(flowId).isEmpty());
    assertEquals(0, countRows("agent_contexts", flowId));
    assertEquals(0, countRows("agent_facts", flowId));
  }

  private static int countRows(String table, String flowId) throws Exception {
    Map<String, String> cfg = database.storeConfig();
    try (Connection c =
            DriverManager.getConnection(
                cfg.get("postgres.url"), cfg.get("postgres.user"), cfg.get("postgres.password"));
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT count(*) FROM " + table + " WHERE flow_id = '" + flowId + "'")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  private static AgentContext context(String flowId, String userId) {
    return new AgentContext("agent-" + UUID.randomUUID(), flowId, userId, 8000, 50);
  }

  private static ContextItem item(String content) {
    ContextItem item = new ContextItem(content, ContextPriority.SHOULD, MemoryType.SHORT_TERM);
    item.setItemId(UUID.randomUUID().toString());
    return item;
  }
}
