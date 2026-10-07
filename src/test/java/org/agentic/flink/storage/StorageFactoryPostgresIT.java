package org.agentic.flink.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.postgres.PostgresConversationStore;
import org.agentic.flink.storage.postgres.PostgresTestDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link StorageFactory}'s PostgreSQL long-term path against a real PostgreSQL (Testcontainers on
 * Podman). The factory hands the config to {@link PostgresConversationStore#initialize(Map)}, which
 * creates the schema and opens the pool; both backend aliases must yield a store that round-trips a
 * conversation through the database.
 */
@Tag("integration")
class StorageFactoryPostgresIT {

  private static PostgresTestDatabase database;

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

  @Test
  @DisplayName(
      "'postgresql' creates a WARM PostgresConversationStore that persists to the database")
  void postgresqlBackendRoundTrips() throws Exception {
    LongTermMemoryStore store =
        StorageFactory.createLongTermStore("postgresql", database.storeConfig());
    try {
      assertInstanceOf(PostgresConversationStore.class, store);
      assertEquals(StorageTier.WARM, store.getTier());
      assertEquals(10, store.getExpectedLatencyMs());
      assertTrue(store.getProviderName().contains("Postgres"), store.getProviderName());

      String flowId = "flow-" + UUID.randomUUID();
      String userId = "user-" + UUID.randomUUID();
      String content = "content-" + UUID.randomUUID();
      AgentContext ctx = new AgentContext("agent-" + UUID.randomUUID(), flowId, userId, 8000, 50);
      ContextItem item = new ContextItem(content, ContextPriority.SHOULD, MemoryType.SHORT_TERM);
      item.setItemId(UUID.randomUUID().toString());
      ctx.addContext(item);

      assertFalse(store.conversationExists(flowId));
      store.saveContext(flowId, ctx);
      assertTrue(store.conversationExists(flowId));

      Optional<AgentContext> loaded = store.loadContext(flowId);
      assertTrue(loaded.isPresent());
      assertEquals(userId, loaded.get().getUserId());
      assertEquals(
          List.of(content),
          loaded.get().getContextWindow().getItems().stream()
              .map(ContextItem::getContent)
              .toList());
    } finally {
      store.close();
    }
  }

  @Test
  @DisplayName("'postgres' is an alias for the same backend and sees rows written through it")
  void postgresAliasSharesTheDatabase() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    String userId = "user-" + UUID.randomUUID();
    LongTermMemoryStore writer =
        StorageFactory.createLongTermStore("postgres", database.storeConfig());
    try {
      assertInstanceOf(PostgresConversationStore.class, writer);
      assertEquals(StorageTier.WARM, writer.getTier());
      writer.saveContext(
          flowId, new AgentContext("agent-" + UUID.randomUUID(), flowId, userId, 8000, 50));
    } finally {
      writer.close();
    }

    LongTermMemoryStore reader =
        StorageFactory.createLongTermStore("postgresql", database.storeConfig());
    try {
      assertTrue(reader.conversationExists(flowId));
      assertEquals(List.of(flowId), reader.listConversationsForUser(userId));
    } finally {
      reader.close();
    }
  }
}
