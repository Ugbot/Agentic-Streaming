# Creating Storage Backends

This guide explains how to add a storage backend to the Flink framework module and how it becomes visible to `StorageFactory`. The registration mechanism is `java.util.ServiceLoader` plus a `META-INF/services` file; `StorageFactory` is not edited when a backend is added. Read [Storage Architecture](../reference/storage-architecture.md) first for what ships today and which factory names resolve.

The procedure below is executed by `src/test/java/org/agentic/flink/storage/toy/ToyBackendRegistrationTest.java`, which registers the toy backend listed in this guide through a test-scope service file and resolves it through the factory. Run it with:

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw test -Dtest=ToyBackendRegistrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Java snippets in this guide that start with a `package` line are compiled by `docs/tools/check_java_snippets.py` (run through `pytest docs/tools/test_docs.py`). Snippets without a `package` line are fragments.

## Extension points

| Interface | Tier | Registration | Resolved by |
|---|---|---|---|
| `org.agentic.flink.memory.ShortTermMemorySpec` | HOT (Flink keyed state) | Passed as an object; no ServiceLoader | `spec.bind(getRuntimeContext())` in `RichFunction.open()` |
| `org.agentic.flink.storage.LongTermMemoryStore` | WARM | `META-INF/services/org.agentic.flink.storage.LongTermMemoryStore` | `StorageFactory.createLongTermStore(name, config)` |
| `org.agentic.flink.storage.VectorStore` | VECTOR | `META-INF/services/org.agentic.flink.storage.VectorStore` | `StorageFactory.createVectorStore(name, config)` |
| `org.agentic.flink.memory.conversation.ConversationStore` | shared transcript | `META-INF/services/org.agentic.flink.memory.conversation.ConversationStore` | `ConversationStores.discover()` |

`StorageFactory.createShortTermStore` accepts only `"memory"` (the legacy `InMemoryShortTermStore`). A new HOT tier backend is therefore not a `ShortTermMemoryStore` registered anywhere; it is a `ShortTermMemorySpec` whose `bind` method returns a `ShortTermMemory`. The shipped implementation is `FlinkStateShortTermMemory`, and a job that needs a different short-term behaviour passes its own spec to the operator that binds it.

## Requirements common to every provider

`StorageProvider<K, V>` is the base interface for the WARM and VECTOR tiers:

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

- The class must have a public no-argument constructor. `ServiceLoader` instantiates it, then `StorageFactory` calls `initialize(config)`.
- The class must be `Serializable`. Flink ships operator fields to task managers, so connections, clients, and thread pools are `transient` and are reopened from the retained configuration. Extend `org.agentic.flink.storage.ReopenableStore` to get this for free: it keeps the `config` map, calls your `open(config)` from `initialize`, and reopens lazily through `ensureOpen()` after deserialization.
- `close()` and `delete()` must be idempotent.
- `getProviderName()` is the short name users pass to the factory. Without an override it is the simple class name.

## Implementing a LongTermMemoryStore

`LongTermMemoryStore extends StorageProvider<String, AgentContext>` and adds the conversation and fact operations listed in its Javadoc (`saveContext`, `loadContext`, `conversationExists`, `deleteConversation`, `saveFacts`, `loadFacts`, `addFact`, `removeFact`, `listActiveConversations`, `listConversationsForUser`, `getConversationMetadata`, `setConversationTTL`, `archiveConversation`). `getTier()` defaults to `StorageTier.WARM` and `getExpectedLatencyMs()` to 5.

The following complete class is the toy backend used by `ToyBackendRegistrationTest`. It keeps everything in transient maps so the guide can be verified without an external service; a real backend replaces the maps with a client and keeps the same shape.

```java
package org.agentic.flink.storage.toy;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.storage.ReopenableStore;
import org.agentic.flink.storage.StorageProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class ToyLongTermStore extends ReopenableStore implements LongTermMemoryStore {

  private static final long serialVersionUID = 1L;

  public static final String PROVIDER_NAME = "toy";
  public static final String NAMESPACE_KEY = "toy.namespace";

  private String namespace;
  private transient Map<String, AgentContext> contexts;
  private transient Map<String, Map<String, ContextItem>> facts;
  private transient Map<String, Long> ttlSeconds;

  @Override
  protected void open(Map<String, String> config) {
    this.namespace = config.getOrDefault(NAMESPACE_KEY, "default");
    this.contexts = new HashMap<>();
    this.facts = new HashMap<>();
    this.ttlSeconds = new HashMap<>();
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public String getProviderName() {
    return PROVIDER_NAME;
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
  public void delete(String key) throws Exception {
    deleteConversation(key);
  }

  @Override
  public boolean exists(String key) throws Exception {
    return conversationExists(key);
  }

  @Override
  public void close() {
    markClosed();
  }

  @Override
  public void saveContext(String flowId, AgentContext context) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(context, "context");
    contexts.put(flowId, context);
  }

  @Override
  public Optional<AgentContext> loadContext(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    return Optional.ofNullable(contexts.get(flowId));
  }

  @Override
  public boolean conversationExists(String flowId) {
    ensureOpen();
    return flowId != null && contexts.containsKey(flowId);
  }

  @Override
  public void deleteConversation(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    contexts.remove(flowId);
    facts.remove(flowId);
    ttlSeconds.remove(flowId);
  }

  @Override
  public void saveFacts(String flowId, Map<String, ContextItem> newFacts) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(newFacts, "facts");
    facts.put(flowId, new HashMap<>(newFacts));
  }

  @Override
  public Map<String, ContextItem> loadFacts(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    Map<String, ContextItem> stored = facts.get(flowId);
    return stored == null ? new HashMap<>() : new HashMap<>(stored);
  }

  @Override
  public void addFact(String flowId, String factId, ContextItem fact) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(factId, "factId");
    requireNonNull(fact, "fact");
    facts.computeIfAbsent(flowId, k -> new HashMap<>()).put(factId, fact);
  }

  @Override
  public void removeFact(String flowId, String factId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(factId, "factId");
    Map<String, ContextItem> stored = facts.get(flowId);
    if (stored != null) {
      stored.remove(factId);
    }
  }

  @Override
  public List<String> listActiveConversations() {
    ensureOpen();
    return new ArrayList<>(contexts.keySet());
  }

  @Override
  public List<String> listConversationsForUser(String userId) {
    ensureOpen();
    requireNonNull(userId, "userId");
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, AgentContext> e : contexts.entrySet()) {
      if (userId.equals(e.getValue().getUserId())) {
        out.add(e.getKey());
      }
    }
    return out;
  }

  @Override
  public Map<String, Object> getConversationMetadata(String flowId) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    Map<String, Object> meta = new HashMap<>();
    AgentContext context = contexts.get(flowId);
    if (context != null) {
      meta.put("flowId", flowId);
      meta.put("userId", context.getUserId());
      meta.put("agentId", context.getAgentId());
      meta.put("created_at", context.getCreatedAt());
      meta.put("last_updated_at", context.getLastUpdatedAt());
      meta.put("ttl_seconds", ttlSeconds.get(flowId));
    }
    return meta;
  }

  @Override
  public void setConversationTTL(String flowId, long seconds) {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    ttlSeconds.put(flowId, seconds);
  }

  @Override
  public void archiveConversation(String flowId, StorageProvider<String, AgentContext> coldStore)
      throws Exception {
    ensureOpen();
    requireNonNull(flowId, "flowId");
    requireNonNull(coldStore, "coldStore");
    Optional<AgentContext> context = loadContext(flowId);
    if (context.isPresent()) {
      coldStore.put(flowId, context.get());
      deleteConversation(flowId);
    }
  }

  private static void requireNonNull(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
  }
}
```

`PostgresConversationStore` in `src/main/java/org/agentic/flink/storage/postgres/` is the production example of the same shape with a JDBC connection held in a `transient` field.

## Registering the backend

### Step 1: list the class in a service file

Create (or append to) a file named after the interface, one fully qualified implementation class per line:

```text
src/main/resources/META-INF/services/org.agentic.flink.storage.LongTermMemoryStore
```

```text
org.agentic.flink.storage.toy.ToyLongTermStore
```

For a backend that lives in a separate jar, the file goes into that jar's `META-INF/services/`. `ServiceLoader` merges service files from every jar on the class path, so nothing in `agentic-flink` changes. The toy backend uses the test-scope copy of this file under `src/test/resources/`, which is why it is visible to `./mvnw test` but not to production code.

For a `VectorStore` the file name is `org.agentic.flink.storage.VectorStore`; for a `ConversationStore` it is `org.agentic.flink.memory.conversation.ConversationStore`.

### Step 2: resolve it through StorageFactory

`createLongTermStore` checks the built-in names `memory`, `postgres`, and `postgresql` first and then iterates the discovered providers. A provider matches when the requested name equals, ignoring case, its `getProviderName()`, its simple class name, or its fully qualified class name, or when the request is a substring of the simple class name. Any of the following resolve the toy backend:

```java
Map<String, String> config = Map.of("toy.namespace", "orders");

LongTermMemoryStore a = StorageFactory.createLongTermStore("toy", config);
LongTermMemoryStore b = StorageFactory.createLongTermStore("ToyLongTermStore", config);
LongTermMemoryStore c =
    StorageFactory.createLongTermStore("org.agentic.flink.storage.toy.ToyLongTermStore", config);
```

A provider whose constructor throws (for example because an optional client library is absent) is skipped with a debug log and does not prevent other providers from resolving.

### Step 3: know what `getAvailableBackends` reports

`StorageFactory.getAvailableBackends(StorageTier.WARM)` returns `memory`, `postgres`, `postgresql`, the simple class name of every discovered provider, and the aliases `redis`, `postgres`, `dynamodb`, and `cassandra` when a class name contains that word. It does not include `getProviderName()`. Consequently:

- `isBackendAvailable(WARM, "ToyLongTermStore")` is `true`;
- `isBackendAvailable(WARM, "toy")` is `false`, even though `createLongTermStore("toy", ...)` works.

`StorageConfiguration.validate()` uses `isBackendAvailable` and throws `IllegalStateException` for a name it does not find, so a `StorageConfiguration` must name the class:

```java
StorageConfiguration storage = StorageConfiguration.builder()
    .withWarmTier("ToyLongTermStore", Map.of("toy.namespace", "orders"))
    .build();
LongTermMemoryStore store = storage.createLongTermStore();
```

`StorageConfiguration.fromYamlFile` and `fromResource` are not implemented; they log a warning and return an empty configuration.

## Implementing a VectorStore

`VectorStore extends StorageProvider<String, float[]>` and adds `storeEmbedding`, `storeEmbeddingsBatch`, `searchSimilar`, `searchSimilarWithFilter`, `storeContextItem`, `searchContextItems`, `getEmbedding`, `getMetadata`, `deleteEmbedding`, `deleteByFlowId`, `getEmbeddingDimension`, `getSimilarityMetric`, `getStatistics`, and `createCollection`. Registration is the same service-file mechanism with `org.agentic.flink.storage.VectorStore` as the file name. `createVectorStore` has no built-in names and matches only `getProviderName()`, the simple class name, or the fully qualified class name (no substring alias). `getAvailableBackends(VECTOR)` returns simple class names only.

Four of the shipped implementations (`InMemoryVectorStore`, `QdrantVectorStore`, `MilvusVectorStore`, `FlussVectorStore`) read the dimension from `vector.dimension`; `PgVectorStore` reads `postgres.dimension`. Prefer `vector.dimension` in a new backend so callers can switch between the majority of backends without changing keys. `VectorStoreDiscoveryTest` in `src/test/java/org/agentic/flink/storage/vector/` shows the discovery assertions to copy.

## Testing a backend

`ToyBackendRegistrationTest` covers the registration contract; copy its structure for a new backend:

```java
@Test
void factoryResolvesProviderName() throws Exception {
  String namespace = UUID.randomUUID().toString();
  Map<String, String> config = new HashMap<>();
  config.put(ToyLongTermStore.NAMESPACE_KEY, namespace);

  LongTermMemoryStore store = StorageFactory.createLongTermStore("toy", config);

  ToyLongTermStore toy = assertInstanceOf(ToyLongTermStore.class, store);
  assertEquals(namespace, toy.getNamespace());
  assertEquals(StorageTier.WARM, toy.getTier());
}
```

Behavioural tests build `ContextItem` values with the explicit constructor `ContextItem(String content, ContextPriority priority, MemoryType memoryType)`; `ContextItem` has no `(String, String)` constructor.

```java
String flowId = UUID.randomUUID().toString();
AgentContext context = new AgentContext("agent", flowId, "user-" + flowId, 4096, 50);
ContextItem fact = new ContextItem("plan=premium", ContextPriority.MUST, MemoryType.LONG_TERM);

store.saveContext(flowId, context);
store.addFact(flowId, "plan", fact);

assertTrue(store.loadContext(flowId).isPresent());
assertEquals("plan=premium", store.loadFacts(flowId).get("plan").getContent());
```

Also serialize the store with `ObjectOutputStream` and use the deserialized copy; `StorageProviderFlinkSerializationTest` shows the Flink-side check (Kryo, Java serialization, `InstantiationUtil`) applied to the shipped stores.

Backends that need an external service are tagged `@Tag("integration")` and excluded from the default `./mvnw test` run. `PostgresConversationStoreTest` uses Testcontainers; run it with:

```bash
systemctl --user start podman.socket
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
./mvnw test -P integration-tests
```

## File locations

- `StorageProvider`: `src/main/java/org/agentic/flink/storage/StorageProvider.java`
- `ReopenableStore`: `src/main/java/org/agentic/flink/storage/ReopenableStore.java`
- `LongTermMemoryStore`: `src/main/java/org/agentic/flink/storage/LongTermMemoryStore.java`
- `VectorStore`: `src/main/java/org/agentic/flink/storage/VectorStore.java`
- `ShortTermMemorySpec`, `ShortTermMemory`, `FlinkStateShortTermMemory`: `src/main/java/org/agentic/flink/memory/`
- `StorageFactory`: `src/main/java/org/agentic/flink/storage/StorageFactory.java`
- `StorageConfiguration`: `src/main/java/org/agentic/flink/storage/config/StorageConfiguration.java`
- Service files: `src/main/resources/META-INF/services/`
- Toy backend and registration test: `src/test/java/org/agentic/flink/storage/toy/`, `src/test/resources/META-INF/services/org.agentic.flink.storage.LongTermMemoryStore`
- Shipped implementations: `src/main/java/org/agentic/flink/storage/{memory,postgres,redis,vector}/`
