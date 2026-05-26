# Pluggable Multi-Tier Storage Architecture

**Status:** Interfaces complete, implementations partial
**Last Updated:** 2025-10-17
**Purpose:** Detailed documentation of the pluggable storage system

## Overview

This codebase implements a multi-tier storage architecture where every storage layer is pluggable, allowing you to choose the best backend for your use case. Storage is organized into five tiers based on latency and access patterns.

## Storage Tiers

### 1. HOT Tier (Short-Term Memory)
**Latency:** <1ms
**Scope:** Active conversation context
**TTL:** Minutes to hours
**Use Cases:**
- Current turn context items
- Recent tool results
- Immediate validation history
- Active reasoning chains

**Available Backends:**
- `memory` - In-memory ConcurrentHashMap (implemented)
- `redis` - Redis with Jedis client (implemented, requires dependency)
- `hazelcast` - Hazelcast IMDG (planned)

### 2. WARM Tier (Long-Term Memory)
**Latency:** 1-10ms
**Scope:** Recent conversations for resumption
**TTL:** Hours to days
**Use Cases:**
- Conversation resumption after job restart
- Long-term facts (user preferences, business rules)
- Cross-job context sharing
- Recent conversation history

**Available Backends:**
- `redis` - Redis with conversation persistence (implemented, requires dependency)
- `dynamodb` - AWS DynamoDB (planned)
- `cassandra` - Apache Cassandra (planned)
- `mongodb` - MongoDB (planned)

### 3. COLD Tier (Historical Storage)
**Latency:** 10-100ms
**Scope:** Historical data and analytics
**TTL:** Days to months
**Use Cases:**
- Long-term conversation history
- Analytics and reporting
- Compliance and audit logs
- Batch processing of historical data

**Planned Backends:**
- `postgresql` - PostgreSQL with JSONB
- `s3` - AWS S3 with Parquet format
- `clickhouse` - ClickHouse for analytics

### 4. VECTOR Tier (Embeddings)
**Latency:** 5-50ms (varies with index size)
**Scope:** Semantic search over embeddings
**TTL:** Indefinite or application-specific
**Use Cases:**
- Retrieval Augmented Generation (RAG)
- Semantic search over conversation history
- Knowledge base queries
- Similar conversation retrieval

**Planned Backends:**
- `qdrant` - Qdrant vector database
- `pinecone` - Pinecone
- `weaviate` - Weaviate
- `pgvector` - PostgreSQL with pgvector extension

### 5. CHECKPOINT Tier (Flink-Managed)
**Latency:** <1ms (local) to 1-5ms (remote)
**Scope:** Flink fault tolerance state
**TTL:** Checkpoint retention policy
**Note:** This tier is managed by Flink's checkpointing mechanism, not by the storage factory.

**Backends:**
- `rocksdb` - Embedded RocksDB (Flink managed)
- `hashmap` - In-memory HashMap (Flink managed)
- `s3` - S3 for checkpoint storage (Flink managed)

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
│  (ContextManagementAction, AgentLoopProcessFunction)        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Storage Interfaces                         │
│  • ShortTermMemoryStore  (HOT tier)                         │
│  • LongTermMemoryStore   (WARM tier)                        │
│  • SteeringStateStore    (WARM tier)                        │
│  • VectorStore           (VECTOR tier)                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    StorageFactory                            │
│  • createShortTermStore(backend, config)                    │
│  • createLongTermStore(backend, config)                     │
│  • createVectorStore(backend, config)                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                Concrete Implementations                      │
│                                                              │
│  HOT:    InMemoryShortTermStore                             │
│          RedisShortTermStore                                │
│                                                              │
│  WARM:   RedisConversationStore                             │
│          (DynamoDB, Cassandra, MongoDB - planned)           │
│                                                              │
│  COLD:   (PostgreSQL, S3, ClickHouse - planned)            │
│                                                              │
│  VECTOR: (Qdrant, Pinecone, Weaviate, pgvector - planned)  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Storage Backends                           │
│  Memory  │  Redis  │  DynamoDB  │  PostgreSQL  │  Qdrant    │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Status

### Completed ✅
- Base `StorageProvider` interface
- `StorageTier` enum with tier classifications
- `ShortTermMemoryStore` interface (HOT tier)
- `LongTermMemoryStore` interface (WARM tier)
- `SteeringStateStore` interface (WARM tier)
- `VectorStore` interface (VECTOR tier)
- `InMemoryShortTermStore` implementation (fully working)
- `RedisShortTermStore` implementation (requires Jedis dependency)
- `RedisConversationStore` implementation (requires Jedis dependency)
- `StorageFactory` with tier-specific factory methods
- `StorageConfiguration` with YAML support (requires Jackson dependency)

### In Progress 🚧
- Integration with `ContextManagementAction`
- Storage metrics and monitoring

### Planned 📋
- Additional backend implementations (DynamoDB, Cassandra, MongoDB, PostgreSQL, S3, ClickHouse)
- Vector store implementations (Qdrant, Pinecone, Weaviate, pgvector)
- Steering store implementations
- Migration utilities for moving data between backends
- Performance benchmarking suite

## Usage Examples

### Programmatic Configuration

```java
// Configure storage programmatically
Map<String, String> redisConfig = new HashMap<>();
redisConfig.put("redis.host", "localhost");
redisConfig.put("redis.port", "6379");
redisConfig.put("redis.ttl.seconds", "3600");

StorageConfiguration config = StorageConfiguration.builder()
    .withHotTier("redis", redisConfig)
    .withWarmTier("redis", redisWarmConfig)
    .build();

// Create storage providers
ShortTermMemoryStore hotStore = config.createShortTermStore();
LongTermMemoryStore warmStore = config.createLongTermStore();

// Use in processing
List<ContextItem> items = Arrays.asList(
    new ContextItem("Order lookup result", ContextPriority.MUST, MemoryType.SHORT_TERM)
);
hotStore.putItems("flow-001", items);

AgentContext context = new AgentContext();
warmStore.saveContext("flow-001", context);
```

### YAML Configuration

```yaml
# storage-config.yaml
storage:
  hot:
    backend: redis
    config:
      redis.host: localhost
      redis.port: 6379
      redis.ttl.seconds: 3600

  warm:
    backend: redis
    config:
      redis.host: localhost
      redis.port: 6379
      redis.database: 1
      redis.ttl.seconds: 86400

  vector:
    backend: qdrant
    config:
      qdrant.host: localhost
      qdrant.port: 6333
      qdrant.collection: agent_vectors
```

Load configuration:
```java
StorageConfiguration config = StorageConfiguration.fromYamlFile("storage-config.yaml");
ShortTermMemoryStore hotStore = config.createShortTermStore();
```

### Factory Pattern Usage

```java
// Direct factory usage
Map<String, String> memoryConfig = new HashMap<>();
memoryConfig.put("cache.max.size", "10000");
memoryConfig.put("cache.ttl.seconds", "3600");

ShortTermMemoryStore store = StorageFactory.createShortTermStore("memory", memoryConfig);
store.putItems("flow-001", contextItems);
```

### In Flink Job

```java
public class AgenticFlinkJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    // Load storage configuration
    StorageConfiguration storageConfig =
        StorageConfiguration.fromResource("config/storage.yaml");

    // Create data stream
    DataStream<Event> events = env.addSource(new KafkaSource(...));

    // Apply stateful processing with pluggable storage
    DataStream<Event> processed = events
        .keyBy(event -> event.getAttr("flowId"))
        .process(new ContextManagementActionWithStorage(storageConfig));

    env.execute("Agentic Flink Job");
  }
}
```

## Backend Comparison

| Backend | Tier | Latency | Capacity | Persistence | Distribution | Cost | Status |
|---------|------|---------|----------|-------------|--------------|------|--------|
| **In-Memory** | HOT | <1ms | JVM heap | None | Local | Free | Implemented |
| **Redis** | HOT/WARM | <1ms | RAM | Optional | Shared | Low | Implemented (requires Jedis) |
| **Hazelcast** | HOT | <1ms | RAM | Optional | Shared | Low | Planned |
| **DynamoDB** | WARM | 1-10ms | Unlimited | Yes | AWS-managed | Variable | Planned |
| **Cassandra** | WARM | 1-10ms | Very Large | Yes | Self-managed | Medium | Planned |
| **MongoDB** | WARM | 1-10ms | Large | Yes | Self/Cloud | Medium | Planned |
| **PostgreSQL** | COLD | 10-100ms | Large | Yes | Self-managed | Low | Planned |
| **S3** | COLD | 50-200ms | Unlimited | Yes | AWS-managed | Very Low | Planned |
| **ClickHouse** | COLD | 10-50ms | Very Large | Yes | Self-managed | Medium | Planned |
| **Qdrant** | VECTOR | 5-50ms | Large | Yes | Self-managed | Low | Planned |
| **Pinecone** | VECTOR | 10-100ms | Large | Yes | Cloud | Medium | Planned |
| **Weaviate** | VECTOR | 10-50ms | Large | Yes | Self/Cloud | Medium | Planned |
| **pgvector** | VECTOR | 10-100ms | Medium | Yes | Self-managed | Low | Planned |

## Dependencies

### Currently Required
```xml
<!-- Core Flink -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.17.2</version>
</dependency>

<!-- Logging -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.36</version>
</dependency>
```

### Optional (for specific backends)

#### Redis Support
```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>5.1.0</version>
</dependency>
```

#### YAML Configuration
```xml
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-yaml</artifactId>
    <version>2.15.2</version>
</dependency>
```

#### In-Memory Cache Enhancement
```xml
<dependency>
    <groupId>com.github.ben-manes.caffeine</groupId>
    <artifactId>caffeine</artifactId>
    <version>3.1.8</version>
</dependency>
```

## Configuration Reference

### InMemoryShortTermStore

| Key | Description | Default |
|-----|-------------|---------|
| `cache.max.size` | Maximum number of conversations to cache | 10000 |
| `cache.ttl.seconds` | Time to live in seconds | 3600 |
| `cache.expire.after.access` | Expire after last access | true |

### RedisShortTermStore

| Key | Description | Default |
|-----|-------------|---------|
| `redis.host` | Redis host | localhost |
| `redis.port` | Redis port | 6379 |
| `redis.password` | Redis password (optional) | - |
| `redis.database` | Redis database number | 0 |
| `redis.ttl.seconds` | Time to live in seconds | 3600 |
| `redis.pool.max.total` | Connection pool max size | 50 |
| `redis.pool.max.idle` | Connection pool max idle | 10 |
| `redis.timeout.ms` | Operation timeout | 2000 |

### RedisConversationStore

| Key | Description | Default |
|-----|-------------|---------|
| `redis.host` | Redis host | localhost |
| `redis.port` | Redis port | 6379 |
| `redis.password` | Redis password (optional) | - |
| `redis.database` | Redis database number | 0 |
| `redis.ttl.seconds` | Time to live in seconds | 86400 |
| `redis.pool.max.total` | Connection pool max size | 50 |
| `redis.pool.max.idle` | Connection pool max idle | 10 |

## Production Considerations

### Capacity Planning

**HOT Tier:**
- Estimate: 1-50 context items per active conversation
- Memory: ~10KB per conversation (average)
- For 10,000 active conversations: ~100MB memory

**WARM Tier:**
- Estimate: Full context + facts per conversation
- Memory: ~50KB per conversation (average)
- For 100,000 recent conversations: ~5GB storage

**COLD Tier:**
- Estimate: Historical conversations with full details
- Storage: ~100KB per conversation (compressed)
- For 10M historical conversations: ~1TB storage

### High Availability

**Single-JVM Deployment:**
- Use in-memory for HOT tier
- Use Redis persistence (AOF) for WARM tier
- Use PostgreSQL for COLD tier

**Multi-JVM Deployment:**
- Use Redis for HOT tier (shared across task managers)
- Use Redis/DynamoDB for WARM tier
- Use distributed database for COLD tier

**Cloud-Native Deployment:**
- Use DynamoDB for WARM tier (AWS-managed)
- Use S3 for COLD tier archival
- Use Pinecone for VECTOR tier (managed)

### Performance Tuning

**HOT Tier:**
- Keep cache size reasonable (<100k conversations)
- Use local in-memory when possible
- Set aggressive TTL (minutes to hours)

**WARM Tier:**
- Use Redis for low-latency resumption
- Implement connection pooling
- Consider read replicas for high read loads

**COLD Tier:**
- Use batch operations for bulk queries
- Implement data partitioning by date
- Consider columnar storage (Parquet) for analytics

## Next Steps

1. Add Jedis dependency to enable Redis backends
2. Integrate pluggable storage into `ContextManagementAction`
3. Implement additional backends (DynamoDB, PostgreSQL, Qdrant)
4. Add storage metrics and monitoring
5. Create migration utilities
6. Performance benchmark suite

## Migration Strategy

When moving from template ExternalStateStore to pluggable architecture:

1. **Phase 1:** Replace ExternalStateStore usage with LongTermMemoryStore
2. **Phase 2:** Add ShortTermMemoryStore for active context
3. **Phase 3:** Integrate VectorStore for RAG functionality
4. **Phase 4:** Add SteeringStateStore for system configuration

---

**Status Summary:** Core interfaces and factory pattern implemented. Redis backends ready to use (requires Jedis dependency). Additional backends planned based on requirements.
