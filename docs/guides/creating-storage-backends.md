# Creating Storage Backends

This guide covers how to implement custom storage backends for the Agentic Flink multi-tier memory architecture. The storage system is organized into tiers with different latency profiles, and each tier defines an interface you can implement against any backing store.

## Storage Architecture

Agentic Flink uses a tiered storage model defined by the `StorageTier` enum:

| Tier | Latency | Purpose | Existing Backends |
|---|---|---|---|
| HOT | <1ms | Active conversation context, recent tool results | `InMemoryShortTermStore`, `RedisShortTermStore` |
| WARM | 1-10ms | Conversation persistence, long-term facts | `InMemoryLongTermStore`, `RedisConversationStore`, `PostgresConversationStore` |
| COLD | 10-100ms | Historical data, analytics | (planned: S3, ClickHouse) |
| VECTOR | 5-50ms | Semantic search over embeddings | (planned: Qdrant, Pinecone, Weaviate, pgvector) |
| CHECKPOINT | <1ms local | Flink-managed fault tolerance | Managed by Flink (RocksDB, HashMapStateBackend) |

All storage providers implement `StorageProvider<K, V>`, the base interface. The HOT and WARM tiers have specialized sub-interfaces (`ShortTermMemoryStore` and `LongTermMemoryStore`) that add tier-specific operations.

### StorageProvider Base Interface

Every storage backend implements this contract:

```java
public interface StorageProvider<K, V> extends Serializable {
    void initialize(Map<String, String> config) throws Exception;
    void put(K key, V value) throws Exception;
    Optional<V> get(K key) throws Exception;
    void delete(K key) throws Exception;
    boolean exists(K key) throws Exception;
    void close() throws Exception;
    StorageTier getTier();
    long getExpectedLatencyMs();
    default String getProviderName() {
        return this.getClass().getSimpleName();
    }
}
```

Key requirements:

- **Serializable**: Flink serializes operators across the cluster. Use `transient` for non-serializable resources (connections, thread pools) and reinitialize them in `initialize()`.
- **initialize()**: Called once after construction to establish connections and configure the backend. The `config` map carries backend-specific key-value pairs.
- **close()**: Must be idempotent. Release all resources (connections, clients, threads).
- **delete()**: Must be idempotent. Deleting a nonexistent key must not throw.

## Implementing ShortTermMemoryStore (HOT Tier)

The `ShortTermMemoryStore` interface stores `List<ContextItem>` keyed by a flow ID string. It extends `StorageProvider<String, List<ContextItem>>` and adds methods for item-level operations, statistics, and TTL.

### Interface Methods

Beyond the base `StorageProvider` methods, you must implement:

| Method | Purpose |
|---|---|
| `putItems(flowId, items)` | Replace all items for a conversation |
| `getItems(flowId)` | Retrieve all items (returns empty list if none) |
| `addItem(flowId, item)` | Append a single item |
| `removeItem(flowId, itemId)` | Remove a specific item by ID |
| `getItemCount(flowId)` | Return item count without fetching all data |
| `clearItems(flowId)` | Remove all items for a conversation |
| `getStatistics()` | Return monitoring metrics (total_items, active_conversations, cache_hit_rate, avg_items_per_conversation) |
| `setTTL(flowId, ttlSeconds)` | Set per-conversation expiration |

The interface provides default implementations for `getTier()` (returns `StorageTier.HOT`) and `getExpectedLatencyMs()` (returns 1).

### Example: Caffeine-backed ShortTermMemoryStore

```java
package org.agentic.flink.storage.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CaffeineShortTermStore implements ShortTermMemoryStore {

    private static final Logger LOG = LoggerFactory.getLogger(CaffeineShortTermStore.class);

    private transient Cache<String, List<ContextItem>> cache;
    private long ttlSeconds = 3600;
    private int maxSize = 10000;

    @Override
    public void initialize(Map<String, String> config) throws Exception {
        this.ttlSeconds = Long.parseLong(config.getOrDefault("cache.ttl.seconds", "3600"));
        this.maxSize = Integer.parseInt(config.getOrDefault("cache.max.size", "10000"));

        this.cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterAccess(ttlSeconds, TimeUnit.SECONDS)
            .recordStats()
            .build();

        LOG.info("CaffeineShortTermStore initialized: maxSize={}, ttl={}s", maxSize, ttlSeconds);
    }

    @Override
    public void put(String key, List<ContextItem> value) throws Exception {
        putItems(key, value);
    }

    @Override
    public Optional<List<ContextItem>> get(String key) throws Exception {
        List<ContextItem> items = getItems(key);
        return items.isEmpty() ? Optional.empty() : Optional.of(items);
    }

    @Override
    public void putItems(String flowId, List<ContextItem> items) throws Exception {
        if (flowId == null || items == null) {
            throw new IllegalArgumentException("flowId and items cannot be null");
        }
        cache.put(flowId, new ArrayList<>(items));
    }

    @Override
    public List<ContextItem> getItems(String flowId) throws Exception {
        if (flowId == null) {
            throw new IllegalArgumentException("flowId cannot be null");
        }
        List<ContextItem> items = cache.getIfPresent(flowId);
        return items != null ? new ArrayList<>(items) : new ArrayList<>();
    }

    @Override
    public void addItem(String flowId, ContextItem item) throws Exception {
        if (flowId == null || item == null) {
            throw new IllegalArgumentException("flowId and item cannot be null");
        }
        List<ContextItem> items = cache.getIfPresent(flowId);
        if (items == null) {
            items = new ArrayList<>();
        }
        items.add(item);
        cache.put(flowId, items);
    }

    @Override
    public void removeItem(String flowId, String itemId) throws Exception {
        if (flowId == null || itemId == null) {
            throw new IllegalArgumentException("flowId and itemId cannot be null");
        }
        List<ContextItem> items = cache.getIfPresent(flowId);
        if (items != null) {
            items.removeIf(item -> itemId.equals(item.getItemId()));
            cache.put(flowId, items);
        }
    }

    @Override
    public int getItemCount(String flowId) throws Exception {
        if (flowId == null) {
            throw new IllegalArgumentException("flowId cannot be null");
        }
        List<ContextItem> items = cache.getIfPresent(flowId);
        return items != null ? items.size() : 0;
    }

    @Override
    public void clearItems(String flowId) throws Exception {
        if (flowId == null) {
            throw new IllegalArgumentException("flowId cannot be null");
        }
        cache.invalidate(flowId);
    }

    @Override
    public Map<String, Object> getStatistics() throws Exception {
        Map<String, Object> stats = new HashMap<>();
        stats.put("active_conversations", cache.estimatedSize());
        stats.put("cache_hit_rate", cache.stats().hitRate());
        stats.put("hit_count", cache.stats().hitCount());
        stats.put("miss_count", cache.stats().missCount());
        stats.put("max_size", maxSize);
        stats.put("ttl_seconds", ttlSeconds);
        return stats;
    }

    @Override
    public void setTTL(String flowId, long ttlSeconds) throws Exception {
        // Caffeine does not support per-entry TTL; this is a no-op.
        // The global TTL configured at initialization applies to all entries.
        LOG.debug("Per-entry TTL not supported by Caffeine; global TTL is {}s", this.ttlSeconds);
    }

    @Override
    public void delete(String key) throws Exception {
        clearItems(key);
    }

    @Override
    public boolean exists(String key) throws Exception {
        return key != null && cache.getIfPresent(key) != null;
    }

    @Override
    public void close() throws Exception {
        if (cache != null) {
            cache.invalidateAll();
            cache.cleanUp();
        }
        LOG.info("CaffeineShortTermStore closed");
    }

    @Override
    public StorageTier getTier() {
        return StorageTier.HOT;
    }

    @Override
    public long getExpectedLatencyMs() {
        return 0; // Sub-millisecond
    }
}
```

## Implementing LongTermMemoryStore (WARM Tier)

The `LongTermMemoryStore` interface stores `AgentContext` objects and supports conversation management (facts, metadata, TTL, archival). It extends `StorageProvider<String, AgentContext>`.

### Interface Methods

Beyond the base methods, you must implement:

| Method | Purpose |
|---|---|
| `saveContext(flowId, context)` | Persist complete agent context |
| `loadContext(flowId)` | Load context (returns `Optional.empty()` if not found or expired) |
| `conversationExists(flowId)` | Check existence without loading full context |
| `deleteConversation(flowId)` | Remove conversation and all associated data |
| `saveFacts(flowId, facts)` | Replace all long-term facts for a conversation |
| `loadFacts(flowId)` | Load facts (returns empty map if none) |
| `addFact(flowId, factId, fact)` | Add or update a single fact |
| `removeFact(flowId, factId)` | Remove a specific fact |
| `listActiveConversations()` | List all non-expired conversation IDs |
| `listConversationsForUser(userId)` | List conversations for a user |
| `getConversationMetadata(flowId)` | Get lightweight metadata (created_at, last_updated_at, user_id, agent_id, turn_count) |
| `setConversationTTL(flowId, ttlSeconds)` | Set conversation expiration |
| `archiveConversation(flowId, coldStore)` | Move conversation to cold tier |

The interface provides default implementations for `getTier()` (returns `StorageTier.WARM`) and `getExpectedLatencyMs()` (returns 5).

### Implementation Pattern

Here is the skeleton for a DynamoDB-backed long-term store, showing the structure you would follow for any new backend:

```java
package org.agentic.flink.storage.dynamodb;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.StorageProvider;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDbLongTermStore implements LongTermMemoryStore {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbLongTermStore.class);

    // Mark non-serializable resources as transient
    private transient Object dynamoClient; // Replace with actual DynamoDB client type
    private String tableName;
    private String region;

    @Override
    public void initialize(Map<String, String> config) throws Exception {
        this.tableName = config.getOrDefault("dynamodb.table", "agent_conversations");
        this.region = config.getOrDefault("dynamodb.region", "us-east-1");

        // Initialize the DynamoDB client
        // this.dynamoClient = DynamoDbClient.builder()
        //     .region(Region.of(region))
        //     .build();

        LOG.info("DynamoDbLongTermStore initialized: table={}, region={}", tableName, region);
    }

    @Override
    public void put(String key, AgentContext value) throws Exception {
        saveContext(key, value);
    }

    @Override
    public Optional<AgentContext> get(String key) throws Exception {
        return loadContext(key);
    }

    @Override
    public void saveContext(String flowId, AgentContext context) throws Exception {
        // Serialize AgentContext and write to DynamoDB
        // Include metadata: userId, agentId, timestamps
        throw new UnsupportedOperationException("Implement DynamoDB put logic");
    }

    @Override
    public Optional<AgentContext> loadContext(String flowId) throws Exception {
        // Read from DynamoDB and deserialize
        throw new UnsupportedOperationException("Implement DynamoDB get logic");
    }

    @Override
    public boolean conversationExists(String flowId) throws Exception {
        // Use a lightweight query (project only the key)
        throw new UnsupportedOperationException("Implement DynamoDB exists check");
    }

    @Override
    public void deleteConversation(String flowId) throws Exception {
        // Delete the conversation item and all associated fact items
        throw new UnsupportedOperationException("Implement DynamoDB delete logic");
    }

    @Override
    public void saveFacts(String flowId, Map<String, ContextItem> facts) throws Exception {
        // Store facts as a nested map or separate items
        throw new UnsupportedOperationException("Implement facts storage");
    }

    @Override
    public Map<String, ContextItem> loadFacts(String flowId) throws Exception {
        throw new UnsupportedOperationException("Implement facts retrieval");
    }

    @Override
    public void addFact(String flowId, String factId, ContextItem fact) throws Exception {
        throw new UnsupportedOperationException("Implement single fact update");
    }

    @Override
    public void removeFact(String flowId, String factId) throws Exception {
        throw new UnsupportedOperationException("Implement fact removal");
    }

    @Override
    public List<String> listActiveConversations() throws Exception {
        // Scan or query with a GSI for active conversations
        throw new UnsupportedOperationException("Implement conversation listing");
    }

    @Override
    public List<String> listConversationsForUser(String userId) throws Exception {
        // Query a GSI on userId
        throw new UnsupportedOperationException("Implement user conversation listing");
    }

    @Override
    public Map<String, Object> getConversationMetadata(String flowId) throws Exception {
        // Project only metadata attributes
        throw new UnsupportedOperationException("Implement metadata retrieval");
    }

    @Override
    public void setConversationTTL(String flowId, long ttlSeconds) throws Exception {
        // Update the TTL attribute for DynamoDB TTL feature
        throw new UnsupportedOperationException("Implement TTL update");
    }

    @Override
    public void archiveConversation(
            String flowId, StorageProvider<String, AgentContext> coldStore) throws Exception {
        Optional<AgentContext> context = loadContext(flowId);
        if (context.isPresent()) {
            coldStore.put(flowId, context.get());
            deleteConversation(flowId);
            LOG.info("Archived conversation {} to cold storage", flowId);
        }
    }

    @Override
    public void delete(String key) throws Exception {
        deleteConversation(key);
    }

    @Override
    public boolean exists(String key) throws Exception {
        return conversationExists(key);
    }

    @Override
    public void close() throws Exception {
        // Close the DynamoDB client
        // if (dynamoClient != null) { dynamoClient.close(); }
        LOG.info("DynamoDbLongTermStore closed");
    }

    @Override
    public StorageTier getTier() {
        return StorageTier.WARM;
    }

    @Override
    public long getExpectedLatencyMs() {
        return 5;
    }
}
```

## Registering with StorageFactory

After implementing your backend, register it in `StorageFactory` so it can be created by name.

### Step 1: Add the case to the factory switch

Edit `src/main/java/org/agentic/flink/storage/StorageFactory.java`. For a new short-term backend, add a case to `createShortTermStore()`:

```java
case "caffeine":
    store = new CaffeineShortTermStore();
    break;
```

For a new long-term backend, add a case to `createLongTermStore()`:

```java
case "dynamodb":
    store = new DynamoDbLongTermStore();
    break;
```

### Step 2: Update the available backends list

In `getAvailableBackends()`, add your backend to the returned array for the corresponding tier:

```java
case HOT:
    return new String[] {"memory", "redis", "caffeine"};

case WARM:
    return new String[] {"memory", "redis", "postgresql", "dynamodb"};
```

### Step 3: Use via StorageFactory

Once registered, your backend can be created by name:

```java
Map<String, String> config = new HashMap<>();
config.put("cache.max.size", "50000");
config.put("cache.ttl.seconds", "1800");

ShortTermMemoryStore store = StorageFactory.createShortTermStore("caffeine", config);
```

### Using StorageConfiguration

You can also configure tiers programmatically via `StorageConfiguration`:

```java
StorageConfiguration storageConfig = StorageConfiguration.builder()
    .withHotTier("caffeine", Map.of(
        "cache.max.size", "50000",
        "cache.ttl.seconds", "1800"))
    .withWarmTier("dynamodb", Map.of(
        "dynamodb.table", "agent_conversations",
        "dynamodb.region", "us-east-1"))
    .build();

ShortTermMemoryStore hotStore = storageConfig.createShortTermStore();
LongTermMemoryStore warmStore = storageConfig.createLongTermStore();
```

## Testing Storage Backends

### Unit Test Pattern

Follow the pattern established in `StorageFactoryTest`. Test creation, configuration propagation, CRUD operations, and error handling:

```java
package org.agentic.flink.storage.caffeine;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.ShortTermMemoryStore;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.StorageTier;
import java.util.*;
import org.junit.jupiter.api.*;

class CaffeineShortTermStoreTest {

    private ShortTermMemoryStore store;

    @BeforeEach
    void setUp() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("cache.max.size", "1000");
        config.put("cache.ttl.seconds", "60");
        store = StorageFactory.createShortTermStore("caffeine", config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void tierIsHot() {
        assertEquals(StorageTier.HOT, store.getTier());
    }

    @Test
    void putAndGetItems() throws Exception {
        String flowId = UUID.randomUUID().toString();
        List<ContextItem> items = List.of(
            new ContextItem("item-1", "First item"),
            new ContextItem("item-2", "Second item")
        );

        store.putItems(flowId, items);
        List<ContextItem> retrieved = store.getItems(flowId);

        assertEquals(2, retrieved.size());
    }

    @Test
    void getItemsReturnsEmptyListForUnknownFlow() throws Exception {
        List<ContextItem> items = store.getItems("nonexistent-flow");
        assertNotNull(items);
        assertTrue(items.isEmpty());
    }

    @Test
    void addItemAppendsToExistingList() throws Exception {
        String flowId = UUID.randomUUID().toString();
        store.putItems(flowId, new ArrayList<>(List.of(
            new ContextItem("item-1", "First")
        )));

        store.addItem(flowId, new ContextItem("item-2", "Second"));

        assertEquals(2, store.getItemCount(flowId));
    }

    @Test
    void clearItemsRemovesAllItems() throws Exception {
        String flowId = UUID.randomUUID().toString();
        store.putItems(flowId, List.of(new ContextItem("item-1", "Data")));

        store.clearItems(flowId);

        assertEquals(0, store.getItemCount(flowId));
        assertFalse(store.exists(flowId));
    }

    @Test
    void statisticsReturnsMetrics() throws Exception {
        Map<String, Object> stats = store.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.containsKey("max_size"));
        assertTrue(stats.containsKey("ttl_seconds"));
    }

    @Test
    void closeIsIdempotent() throws Exception {
        store.close();
        assertDoesNotThrow(() -> store.close());
    }

    @Test
    void nullFlowIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> store.putItems(null, List.of()));
        assertThrows(IllegalArgumentException.class, () -> store.getItems(null));
    }
}
```

### Testing Long-Term Store Implementations

For `LongTermMemoryStore`, test the full lifecycle of conversations and facts:

```java
@Test
void saveAndLoadContext() throws Exception {
    String flowId = UUID.randomUUID().toString();
    AgentContext context = createTestContext(flowId);

    store.saveContext(flowId, context);
    Optional<AgentContext> loaded = store.loadContext(flowId);

    assertTrue(loaded.isPresent());
    assertEquals(flowId, loaded.get().getFlowId());
}

@Test
void deleteConversationRemovesContextAndFacts() throws Exception {
    String flowId = UUID.randomUUID().toString();
    store.saveContext(flowId, createTestContext(flowId));
    store.saveFacts(flowId, Map.of("key", new ContextItem("fact-1", "value")));

    store.deleteConversation(flowId);

    assertFalse(store.conversationExists(flowId));
    assertTrue(store.loadFacts(flowId).isEmpty());
}

@Test
void addFactDoesNotOverwriteOtherFacts() throws Exception {
    String flowId = UUID.randomUUID().toString();
    store.saveFacts(flowId, Map.of("tier", new ContextItem("f1", "premium")));
    store.addFact(flowId, "language", new ContextItem("f2", "en"));

    Map<String, ContextItem> facts = store.loadFacts(flowId);
    assertEquals(2, facts.size());
    assertNotNull(facts.get("tier"));
    assertNotNull(facts.get("language"));
}
```

### Integration Testing with Testcontainers

For backends that require external services (Redis, PostgreSQL, DynamoDB), use Testcontainers:

```java
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RedisShortTermStoreIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
        .withExposedPorts(6379);

    private ShortTermMemoryStore store;

    @BeforeEach
    void setUp() throws Exception {
        Map<String, String> config = Map.of(
            "redis.host", redis.getHost(),
            "redis.port", String.valueOf(redis.getMappedPort(6379))
        );
        store = StorageFactory.createShortTermStore("redis", config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) store.close();
    }

    // Same test methods as the unit tests above -- the interface contract
    // is identical regardless of backend.
}
```

Run integration tests with:

```
mvn test -P integration-tests
```

## File Locations

- `StorageProvider` base interface: `src/main/java/org/agentic/flink/storage/StorageProvider.java`
- `ShortTermMemoryStore`: `src/main/java/org/agentic/flink/storage/ShortTermMemoryStore.java`
- `LongTermMemoryStore`: `src/main/java/org/agentic/flink/storage/LongTermMemoryStore.java`
- `StorageTier` enum: `src/main/java/org/agentic/flink/storage/StorageTier.java`
- `StorageFactory`: `src/main/java/org/agentic/flink/storage/StorageFactory.java`
- `StorageConfiguration`: `src/main/java/org/agentic/flink/storage/config/StorageConfiguration.java`
- In-memory reference implementations: `src/main/java/org/agentic/flink/storage/memory/`
- Redis implementations: `src/main/java/org/agentic/flink/storage/redis/`
- PostgreSQL implementation: `src/main/java/org/agentic/flink/storage/postgres/`
- Factory tests: `src/test/java/org/agentic/flink/storage/StorageFactoryTest.java`
