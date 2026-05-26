package org.agentic.flink.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.storage.memory.InMemoryShortTermStore;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * Unit tests for StorageFactory.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Factory method creation for different backends
 *   <li>Configuration handling
 *   <li>Error handling for invalid backends
 *   <li>Backend discovery and registration
 * </ul>
 *
 * @author Agentic Flink Team
 */
class StorageFactoryTest {

  private Map<String, String> config;

  @BeforeEach
  void setUp() {
    config = new HashMap<>();
  }

  // ==================== Short-Term Memory Store Tests ====================

  @Test
  @DisplayName("Should create in-memory short-term store")
  void testCreateInMemoryShortTermStore() throws Exception {
    config.put("cache.max.size", "1000");
    config.put("cache.ttl.seconds", "3600");

    ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", config);

    assertNotNull(store);
    assertTrue(store instanceof InMemoryShortTermStore);
    assertEquals(StorageTier.HOT, store.getTier());

    store.close();
  }

  @Test
  @DisplayName("Should create in-memory short-term store with default config")
  void testCreateInMemoryShortTermStoreDefaultConfig() throws Exception {
    ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", new HashMap<>());

    assertNotNull(store);
    assertTrue(store instanceof InMemoryShortTermStore);

    store.close();
  }

  @Test
  @DisplayName("Should reject Redis short-term backend (Flink state is the HOT tier)")
  void testRejectRedisShortTermStore() {
    // Redis-as-HOT was removed when Flink keyed state became the canonical short-term
    // memory. Users wanting a non-state HOT cache must implement their own
    // ShortTermMemorySpec — the factory no longer ships one.
    Map<String, String> redisCfg = new HashMap<>();
    redisCfg.put("redis.host", "localhost");
    redisCfg.put("redis.port", "6379");
    assertThrows(
        IllegalArgumentException.class,
        () -> StorageFactory.createShortTermStore("redis", redisCfg));
  }

  // ==================== Long-Term Memory Store Tests ====================

  @Test
  @DisplayName("Should create in-memory long-term store")
  void testCreateInMemoryLongTermStore() throws Exception {
    config.put("cache.max.size", "5000");
    config.put("cache.ttl.seconds", "86400");

    LongTermMemoryStore store = StorageFactory.createLongTermStore("memory", config);

    assertNotNull(store);
    assertEquals(StorageTier.WARM, store.getTier());

    store.close();
  }

  @Test
  @DisplayName("Should create Redis long-term store when configured")
  void testCreateRedisLongTermStore() throws Exception {
    config.put("redis.host", "localhost");
    config.put("redis.port", "6379");
    config.put("redis.database", "1");

    LongTermMemoryStore store = StorageFactory.createLongTermStore("redis", config);

    assertNotNull(store);
    assertEquals(StorageTier.WARM, store.getTier());
    assertTrue(store.getProviderName().contains("Redis"));

    store.close();
  }

  @Test
  @DisplayName("Should create PostgreSQL long-term store when configured")
  void testCreatePostgresLongTermStore() throws Exception {
    config.put("postgres.url", "jdbc:h2:mem:testdb;MODE=PostgreSQL");
    config.put("postgres.user", "sa");
    config.put("postgres.password", "");
    config.put("postgres.auto.create.tables", "true");

    LongTermMemoryStore store = StorageFactory.createLongTermStore("postgresql", config);

    assertNotNull(store);
    assertEquals(StorageTier.WARM, store.getTier());
    assertEquals(10, store.getExpectedLatencyMs());
    assertTrue(store.getProviderName().contains("Postgres"));

    store.close();
  }

  @Test
  @DisplayName("Should create PostgreSQL store with 'postgres' alias")
  void testCreatePostgresLongTermStoreAlias() throws Exception {
    config.put("postgres.url", "jdbc:h2:mem:testdb2;MODE=PostgreSQL");
    config.put("postgres.user", "sa");
    config.put("postgres.password", "");

    // Both "postgresql" and "postgres" should work
    LongTermMemoryStore store = StorageFactory.createLongTermStore("postgres", config);

    assertNotNull(store);
    assertEquals(StorageTier.WARM, store.getTier());

    store.close();
  }

  // ==================== Error Handling Tests ====================

  @Test
  @DisplayName("Should throw exception for unknown backend type")
  void testUnknownBackend() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StorageFactory.createShortTermStore("unknown-backend", config));
  }

  @Test
  @DisplayName("Should throw exception for null backend type")
  void testNullBackend() {
    assertThrows(
        IllegalArgumentException.class, () -> StorageFactory.createShortTermStore(null, config));
  }

  @Test
  @DisplayName("Should throw exception for empty backend type")
  void testEmptyBackend() {
    assertThrows(
        IllegalArgumentException.class, () -> StorageFactory.createShortTermStore("", config));
  }

  @Test
  @DisplayName("Should throw exception for null config")
  void testNullConfig() {
    assertThrows(
        IllegalArgumentException.class, () -> StorageFactory.createShortTermStore("memory", null));
  }

  // ==================== Backend Type Tests ====================

  @Test
  @DisplayName("Should be case-insensitive for backend type")
  void testCaseInsensitiveBackend() throws Exception {
    ShortTermMemoryStore store1 = StorageFactory.createShortTermStore("memory", config);
    ShortTermMemoryStore store2 = StorageFactory.createShortTermStore("MEMORY", config);
    ShortTermMemoryStore store3 = StorageFactory.createShortTermStore("Memory", config);

    assertNotNull(store1);
    assertNotNull(store2);
    assertNotNull(store3);

    store1.close();
    store2.close();
    store3.close();
  }

  // ==================== Configuration Propagation Tests ====================

  @Test
  @DisplayName("Should propagate configuration to created store")
  void testConfigurationPropagation() throws Exception {
    config.put("cache.max.size", "2500");
    config.put("cache.ttl.seconds", "7200");

    ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", config);

    Map<String, Object> stats = store.getStatistics();
    assertEquals(2500, stats.get("max_size"));
    assertEquals(7200L, stats.get("ttl_seconds"));

    store.close();
  }

  // ==================== Multiple Instance Tests ====================

  @Test
  @DisplayName("Should create multiple independent instances")
  void testMultipleInstances() throws Exception {
    Map<String, String> config1 = new HashMap<>();
    config1.put("cache.max.size", "1000");

    Map<String, String> config2 = new HashMap<>();
    config2.put("cache.max.size", "2000");

    ShortTermMemoryStore store1 = StorageFactory.createShortTermStore("memory", config1);
    ShortTermMemoryStore store2 = StorageFactory.createShortTermStore("memory", config2);

    assertNotNull(store1);
    assertNotNull(store2);
    assertNotSame(store1, store2);

    // Verify they have different configurations
    assertEquals(1000, store1.getStatistics().get("max_size"));
    assertEquals(2000, store2.getStatistics().get("max_size"));

    store1.close();
    store2.close();
  }

  // ==================== Provider Name Tests ====================

  @Test
  @DisplayName("Should have meaningful provider names")
  void testProviderNames() throws Exception {
    ShortTermMemoryStore memoryStore = StorageFactory.createShortTermStore("memory", config);
    assertTrue(memoryStore.getProviderName().toLowerCase().contains("memory"));

    memoryStore.close();
  }

  // ==================== Initialization Tests ====================

  @Test
  @DisplayName("Should initialize store on creation")
  void testStoreInitialization() throws Exception {
    ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", config);

    // Store should be ready to use immediately
    assertDoesNotThrow(() -> store.putItems("test", java.util.Collections.emptyList()));
    assertDoesNotThrow(() -> store.getItems("test"));

    store.close();
  }

  // ==================== Resource Management Tests ====================

  @Test
  @DisplayName("Should allow closing stores created by factory")
  void testCloseCreatedStore() throws Exception {
    ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", config);

    assertNotNull(store);
    assertDoesNotThrow(() -> store.close());
  }

  @Test
  @DisplayName("Should create stores that can be closed multiple times")
  void testMultipleClose() throws Exception {
    ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", config);

    store.close();
    // Second close should not throw
    assertDoesNotThrow(() -> store.close());
  }
}
