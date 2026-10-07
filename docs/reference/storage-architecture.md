# Storage Architecture

This page describes the storage layer of the root Flink module (`org.jagentic:agentic-flink`) as it exists in the source tree. Every class, method, and service file named here can be found under `src/main/java/org/agentic/flink` and `src/main/resources/META-INF/services`. If a backend is not listed here it does not ship.

## Summary

- Short-term memory is Flink keyed state. `FlinkStateShortTermMemory` is the default and is bound inside `RichFunction.open()` through a serializable `ShortTermMemorySpec`. No external cache sits in front of it.
- Long-term memory (`LongTermMemoryStore`) and vector storage (`VectorStore`) are pluggable. Implementations are discovered with `java.util.ServiceLoader` from `META-INF/services` files. The built-in providers ship in the same jar.
- `StorageFactory` is the name-based entry point for long-term and vector stores. For short-term memory it accepts only `"memory"` and points callers at `FlinkStateShortTermMemory`.
- `StorageConfiguration` is a serializable holder of per-tier backend names and settings. Its YAML loading methods are not implemented.

## Interfaces

All interfaces live in `org.agentic.flink.storage` unless noted.

| Interface | Package | Role |
|-----------|---------|------|
| `StorageProvider<K, V>` | `storage` | Base contract: `initialize(Map)`, `put`, `get`, `delete`, `exists`, `close`, `getTier()`, `getExpectedLatencyMs()`, `getProviderName()`. Extends `Serializable`. |
| `ShortTermMemoryStore` | `storage` | Legacy flow-id keyed store (`putItems`, `getItems`, `addItem`, `removeItem`, `getItemCount`, `clearItems`, `setTTL`, `getStatistics`). Only `InMemoryShortTermStore` implements it. |
| `LongTermMemoryStore` | `storage` | Conversation persistence: `saveContext`, `loadContext`, `conversationExists`, `deleteConversation`, `saveFacts`, `loadFacts`, `addFact`, `removeFact`, `listActiveConversations`, `listConversationsForUser`, `getConversationMetadata`, `setConversationTTL`, `archiveConversation`. |
| `VectorStore` | `storage` | Embedding storage and similarity search: `searchSimilar`, `storeContextItem`, `searchContextItems`, `getEmbedding`, `getMetadata`, `deleteEmbedding`, `deleteByFlowId`, `getEmbeddingDimension`, `getSimilarityMetric`, `getStatistics`. |
| `ShortTermMemory` | `memory` | Operator-scoped short-term memory used by the agent operators: `getContext`, `putContext`, `putItem`, `getItem`, `removeItem`, `items`, `size`, `clearItems`, `totalTokens`. The current key is supplied by Flink, so there is no `flowId` argument. |
| `ShortTermMemorySpec` | `memory` | Serializable factory for `ShortTermMemory`. `bind(RuntimeContext)` is called per task inside `open()`. Optional `ttl()` and `providerName()`. |
| `ConversationStore` | `memory.conversation` | Conversation history store used by `AgentBuilder.withConversationStore(...)`. |

`StorageTier` is an enum with the values `HOT`, `WARM`, `COLD`, `VECTOR`, and `CHECKPOINT`. `getTier()` on each store reports where it belongs.

`ReopenableStore` is the abstract base class used by every backend that holds a network connection. It keeps the configuration map in a serializable field and reopens the transient connection lazily after Flink deserializes the store on a task manager. Subclasses implement `open(Map<String, String>)` and call `ensureOpen()` before each operation.

## Short-term memory is Flink state

The production path for short-term memory does not go through `StorageFactory`. An agent function declares a `ShortTermMemorySpec` when the job graph is built and binds it to real state handles in `open()`:

```java
// job graph time: serializable
ShortTermMemorySpec spec = FlinkStateShortTermMemory.spec(Duration.ofMinutes(30));

// inside RichFunction.open(): per task, per key
ShortTermMemory memory = spec.bind(getRuntimeContext());
```

`FlinkStateShortTermMemory` keeps two pieces of keyed state per key:

- `ValueState<AgentContext>` named `shortterm.context`
- `MapState<String, ContextItem>` named `shortterm.items`

When the spec carries a non-zero TTL, both descriptors get a `StateTtlConfig` with `OnCreateAndWrite` updates, `ReturnExpiredIfNotCleanedUp` visibility, and incremental cleanup of ten entries per access. `FlinkStateShortTermMemory.spec()` with no argument means no TTL.

`AgentBuilder.withShortTermTtl(Duration)` and `AgentBuilder.withShortTermMemory(ShortTermMemorySpec)` record a spec on the built `Agent`, but no operator in this repository reads those fields yet; treat them as unwired. The operator that binds a spec today is `ContextManagementActionWithStorage` under `plugins/flintagents`, which receives the spec through its constructor and calls `bind` in `open()`. That package is excluded from the default build and compiles only with `-P flink-agents`.

There is no `RedisShortTermStore`, no `CaffeineShortTermStore`, and no other short-term backend. `StorageFactory.createShortTermStore(backend, config)` accepts only `"memory"` and returns an `InMemoryShortTermStore`; for any other name it throws `IllegalArgumentException` with a message that directs the caller to `FlinkStateShortTermMemory.spec()`. `InMemoryShortTermStore` is kept for tests and for the legacy `StorageIntegratedFlinkJob` example.

## Service registration

Backends are discovered with `ServiceLoader`. The root module registers its providers in three files under `src/main/resources/META-INF/services/`:

`org.agentic.flink.storage.LongTermMemoryStore`

```text
org.agentic.flink.storage.memory.InMemoryLongTermStore
org.agentic.flink.storage.postgres.PostgresConversationStore
org.agentic.flink.storage.redis.RedisConversationStore
```

`org.agentic.flink.storage.VectorStore`

```text
org.agentic.flink.storage.vector.InMemoryVectorStore
org.agentic.flink.storage.vector.PgVectorStore
org.agentic.flink.storage.vector.QdrantVectorStore
org.agentic.flink.storage.vector.MilvusVectorStore
org.agentic.flink.storage.vector.FlussVectorStore
```

`org.agentic.flink.memory.conversation.ConversationStore`

```text
org.agentic.flink.memory.conversation.redis.RedisConversationStore
org.agentic.flink.memory.conversation.fluss.FlussConversationStore
```

Nothing is registered for `ShortTermMemorySpec`. Short-term specs are passed programmatically, for example through `StorageConfiguration.Builder.withHotTier(ShortTermMemorySpec)` or an operator constructor.

A third-party jar adds a backend by shipping the same service file name with its own implementation class listed. No change to `StorageFactory` is needed. The step by step procedure, including a test that registers a toy backend, is in [Creating Storage Backends](../guides/creating-storage-backends.md).

## Shipped backends

| Backend | Class | Interface | Provider name | Registration | Client dependency |
|---------|-------|-----------|---------------|--------------|-------------------|
| In-memory long-term | `storage.memory.InMemoryLongTermStore` | `LongTermMemoryStore` | `InMemoryLongTermStore` | ServiceLoader and built-in name `memory` | none |
| PostgreSQL long-term | `storage.postgres.PostgresConversationStore` | `LongTermMemoryStore` | `PostgresConversationStore` | ServiceLoader and built-in names `postgres`, `postgresql` | `org.postgresql:postgresql`, `com.zaxxer:HikariCP` |
| Redis long-term | `storage.redis.RedisConversationStore` | `LongTermMemoryStore` | `RedisConversationStore` | ServiceLoader (alias `redis`) | `redis.clients:jedis` (optional) |
| In-memory vector | `storage.vector.InMemoryVectorStore` | `VectorStore` | `in-memory` | ServiceLoader | none |
| pgvector | `storage.vector.PgVectorStore` | `VectorStore` | `pgvector` | ServiceLoader | `org.postgresql:postgresql` |
| Qdrant | `storage.vector.QdrantVectorStore` | `VectorStore` | `qdrant` | ServiceLoader | `io.qdrant:client` (optional) |
| Milvus | `storage.vector.MilvusVectorStore` | `VectorStore` | `milvus` | ServiceLoader | `io.milvus:milvus-sdk-java` (optional) |
| Fluss | `storage.vector.FlussVectorStore` | `VectorStore` | `fluss` | ServiceLoader | `com.alibaba.fluss:fluss-client` (optional) |
| Redis conversation | `memory.conversation.redis.RedisConversationStore` | `ConversationStore` | see class | ServiceLoader | `redis.clients:jedis` (optional) |
| Fluss conversation | `memory.conversation.fluss.FlussConversationStore` | `ConversationStore` | see class | ServiceLoader | `com.alibaba.fluss:fluss-client` (optional) |

Package names in the table are relative to `org.agentic.flink`. Dependencies marked optional are declared `<optional>true</optional>` in the root `pom.xml`; a job that selects one of those backends must add the client library to its own classpath.

`PostgresConversationStore` creates the tables `agent_contexts` and `agent_facts` itself when `postgres.auto.create.tables` is `true`. The file `sql/schema.sql` in the repository root defines a different, wider schema (`conversations`, `context_items`, `messages`, `tool_executions`, `validation_results`) and is not read by the store.

Test coverage that backs this table: `StorageFactoryTest`, `VectorStoreDiscoveryTest`, `InMemoryLongTermStoreTest`, `InMemoryShortTermStoreTest`, `InMemoryVectorStoreTest`, `StorageProviderFlinkSerializationTest`, `MemorySpecSerializationTest`, and the Testcontainers-backed `PostgresConversationStoreTest`, `RedisConversationStoreIT`, and `FlussConversationStoreIT` (tagged `integration`, run with `./mvnw test -Pintegration-tests` and a Podman socket).

## StorageFactory

`org.agentic.flink.storage.StorageFactory` resolves a backend name to an initialized store. All three methods require a non-null, non-empty backend and a non-null config map and call `initialize(config)` before returning.

| Method | Accepted names | Resolution |
|--------|----------------|------------|
| `createShortTermStore(String, Map)` | `memory` only | Returns `InMemoryShortTermStore`. Any other name throws `IllegalArgumentException`. |
| `createLongTermStore(String, Map)` | `memory`, `postgres`, `postgresql`, then any ServiceLoader provider | Built-in names are matched first. Otherwise each `ServiceLoader<LongTermMemoryStore>` provider is matched on `getProviderName()`, simple class name, fully qualified class name, and finally a case-insensitive substring of the simple class name (this is how `redis` resolves to `RedisConversationStore`). |
| `createVectorStore(String, Map)` | any ServiceLoader provider | Matched on `getProviderName()`, simple class name, or fully qualified class name. No built-in names. |
| `getAvailableBackends(StorageTier)` | any tier | `HOT` returns `{"memory"}`. `WARM` returns `memory`, `postgres`, `postgresql`, the simple class name of every discovered `LongTermMemoryStore` provider, and the aliases `redis`, `postgres`, `dynamodb`, or `cassandra` when a provider class name contains that word. `VECTOR` returns the simple class name of every discovered `VectorStore` provider (for example `QdrantVectorStore`, not `qdrant`). `CHECKPOINT` returns `rocksdb` and `hashmap`. `COLD` returns an empty array. |
| `isBackendAvailable(StorageTier, String)` | as above | Case-insensitive membership test on `getAvailableBackends`. Because the `VECTOR` list holds class names, `isBackendAvailable(VECTOR, "qdrant")` is `false` while `createVectorStore("qdrant", ...)` succeeds; use the class name where availability is checked (see `StorageConfiguration.validate()` below). |

Unknown names fail loudly. A store whose client library is missing at runtime fails when `initialize` opens the connection, not silently.

Example:

```java
Map<String, String> pg = new HashMap<>();
pg.put("postgres.url", "jdbc:postgresql://localhost:5432/agentic");
pg.put("postgres.user", "agentic");
pg.put("postgres.password", "secret");
pg.put("postgres.auto.create.tables", "true");
LongTermMemoryStore longTerm = StorageFactory.createLongTermStore("postgres", pg);

Map<String, String> qdrant = new HashMap<>();
qdrant.put("qdrant.host", "localhost");
qdrant.put("qdrant.port", "6334");
qdrant.put("qdrant.collection", "agent_memory");
qdrant.put("vector.dimension", "768");
VectorStore vectors = StorageFactory.createVectorStore("qdrant", qdrant);
```

The keys each backend reads are documented in the class Javadoc of the backend. `QdrantVectorStore` reads `qdrant.host` (default `localhost`), `qdrant.port` (the gRPC port, default `6334`), `qdrant.api.key`, `qdrant.collection` (default `agentic_flink`), `qdrant.use.tls` (default `false`), the required `vector.dimension`, and `vector.similarity` (default `cosine`). The application-level `AGENTIC_FLINK_QDRANT_HOST` and `AGENTIC_FLINK_QDRANT_PORT` variables read by `AgenticFlinkConfig` default to `localhost` and `6333` (the REST port); a job that forwards them to `QdrantVectorStore` must pass the gRPC port. See [Configuration](../configuration.md).

## StorageConfiguration

`org.agentic.flink.storage.config.StorageConfiguration` is a `Serializable` bag of `TierConfiguration` entries plus an optional `ShortTermMemorySpec` and optional memory channels. It is built with `StorageConfiguration.builder()`:

```java
StorageConfiguration storage = StorageConfiguration.builder()
    .withHotTier(FlinkStateShortTermMemory.spec(Duration.ofHours(1)))
    .withWarmTier("postgres", pg)
    .withVectorTier("QdrantVectorStore", qdrant)
    .build();
```

`build()` calls `validate()`, which checks that every configured tier names a backend that `StorageFactory.isBackendAvailable` accepts. For the `VECTOR` tier that means the simple class name (`QdrantVectorStore`), because `getAvailableBackends(VECTOR)` lists class names only; the provider name `qdrant` is accepted by `createVectorStore` but rejected by `validate()`. For a ServiceLoader-discovered `LongTermMemoryStore` the same applies unless its class name contains `redis`, `postgres`, `dynamo`, or `cassandra`. The `withHotTier(ShortTermMemorySpec)` overload records the spec and a sentinel backend name `spec` that `validate()` recognizes. The string overload `withHotTier("memory", config)` is retained for the in-memory legacy store.

`createShortTermStore()`, `createLongTermStore()`, and `createVectorStore()` on the built configuration delegate to `StorageFactory` with the recorded backend name and settings.

`StorageConfiguration.fromYamlFile(String)` and `fromResource(String)` exist but are not implemented: they log a warning and return an empty configuration. Do not rely on YAML loading.

## Long-term hydration and write-behind

`AgentBuilder.withLongTermStore(...)` stores a `LongTermMemoryStore` on the `Agent` that no operator in the default build reads, so setting it has no runtime effect today. The hydration and write-behind logic exists in `ContextManagementActionWithStorage` (`plugins/flintagents`, opt-in with `-P flink-agents`): it binds the short-term spec in `open()`, creates the WARM store from its `StorageConfiguration` when `isTierConfigured(WARM)` is true, loads context and facts for a key from that store on first access, and syncs back on an event-count interval or after a successful compaction. `StorageHydrationIntegrationTest` in the default build exercises the store-level round trip (save to `InMemoryLongTermStore`, load, copy into `InMemoryShortTermStore`) without a Flink operator.

## Metrics

`org.agentic.flink.storage.metrics.StorageMetrics` is a serializable per-tier counter set with `recordGet`, `recordPut`, `recordDelete`, `recordError`, and getters such as `getTotalOperations`, `getHitRate`, `getErrorRate`, and `getAverageLatencyMs`. Functions expose it to Flink through `getRuntimeContext().getMetricGroup()` gauges. `StorageMetricsTest` covers the arithmetic.

## What does not exist

The following names have appeared in earlier drafts of this documentation and do not exist in the code base: `RedisShortTermStore`, `CaffeineShortTermStore`, `DynamoDbLongTermStore`, `S3ColdStore`, `ExternalStateStore`, `RedisStateStore`, `DynamoDBStateStore`, `PostgreSQLStateStore`, and a working YAML loader in `StorageConfiguration`. No shipped class implements a `COLD` tier store; `LongTermMemoryStore.archiveConversation` takes a caller-supplied `StorageProvider<String, AgentContext>` as the archive target. `getAvailableBackends(CHECKPOINT)` names the Flink state backends `rocksdb` and `hashmap`; there is no `StorageProvider` for them because checkpointing is Flink's own mechanism.

## Related pages

- [Memory](../memory.md) for the agent-level view of short-term, long-term, and vector memory.
- [Creating Storage Backends](../guides/creating-storage-backends.md) for implementing and registering a provider.
- [Storage Quickstart](../guides/storage-quickstart.md) for running the `StorageIntegratedFlinkJob` example.
- [Configuration](../configuration.md) for the `AGENTIC_FLINK_*` environment variables.
