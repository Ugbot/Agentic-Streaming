# Pluggable Storage Quick Start Guide

This guide shows how to run the complete storage-integrated Flink job with different backends.

## Overview

The `StorageIntegratedFlinkJob` demonstrates:
- ✅ Multi-tier storage (HOT + WARM)
- ✅ Configurable backends (in-memory, Redis, or PostgreSQL)
- ✅ Context hydration from persistent storage
- ✅ Metrics collection and reporting
- ✅ Multi-user conversation handling
- ✅ Realistic event processing

## Quick Start

### Option 1: In-Memory Storage (No Dependencies)

**Works immediately - no setup required**

```bash
# Build the project
mvn clean package -DskipTests

# Run with in-memory storage
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="memory"
```

**Expected Output:**
```
[flow-alice] User alice: Hello, I need help with my order (Context size: 1)
[flow-bob] User bob: Hi there (Context size: 1)
[flow-alice] User alice: My order number is 12345 (Context size: 2)
...
=== Storage Metrics Report ===
HOT Storage (0ms latency):
  - Operations: 42 (100.0% success)
  - Latency: avg=0ms, p95=0ms, p99=0ms
WARM Storage (1ms latency):
  - Operations: 8 (100.0% success)
  - Latency: avg=1ms, p95=1ms, p99=1ms
=============================
```

### Option 2: Redis Storage (Distributed)

**Requires Redis running**

```bash
# Start Redis with Docker
docker run -d --name redis-storage -p 6379:6379 redis:latest

# Verify Redis is running
docker ps | grep redis-storage

# Run with Redis storage
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="redis"
```

**What Happens:**
1. Job connects to Redis on `localhost:6379`
2. Uses database 0 for HOT tier (short-term context)
3. Uses database 1 for WARM tier (conversation persistence)
4. All conversations survive restarts
5. Metrics show Redis latency (~1-5ms)

**Inspect Redis Data:**
```bash
# Connect to Redis
docker exec -it redis-storage redis-cli

# List HOT tier keys (database 0)
SELECT 0
KEYS agent:shortterm:*

# List WARM tier keys (database 1)
SELECT 1
KEYS agent:context:*
KEYS agent:facts:*
KEYS agent:metadata:*

# View a conversation
SELECT 1
GET agent:context:flow-alice

# View metadata
HGETALL agent:metadata:flow-alice

# View active conversations
SMEMBERS agent:conversations:active

# View conversations for a user
SMEMBERS agent:user:alice
```

**Test Conversation Persistence:**
```bash
# Run the job
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="redis"

# Stop it with Ctrl+C after a few messages

# Run it again - conversations will be hydrated from Redis
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="redis"
```

You should see log messages like:
```
Context not in HOT storage, hydrating from WARM for flow=flow-alice
Hydrated 5 items from WARM to HOT storage
```

### Option 3: PostgreSQL Storage (Persistent Database)

**Requires PostgreSQL running**

```bash
# Start PostgreSQL with Docker
docker run -d --name postgres-storage \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=flink_agents \
  -p 5432:5432 \
  postgres:15-alpine

# Verify PostgreSQL is running
docker ps | grep postgres-storage

# Run with PostgreSQL storage
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="postgresql"
```

**What Happens:**
1. Job connects to PostgreSQL on `localhost:5432`
2. Auto-creates tables: `agent_contexts` and `agent_facts`
3. Uses HikariCP connection pooling for efficiency
4. All conversations stored in relational database
5. Full ACID transaction support
6. Metrics show PostgreSQL latency (~5-15ms)

**Inspect PostgreSQL Data:**
```bash
# Connect to PostgreSQL
docker exec -it postgres-storage psql -U postgres -d flink_agents

# List tables
\dt

# View schema
\d agent_contexts
\d agent_facts

# Query conversations
SELECT flow_id, user_id, agent_id,
       created_at, last_updated_at
FROM agent_contexts;

# View a specific conversation
SELECT context_json
FROM agent_contexts
WHERE flow_id = 'flow-alice';

# Query facts
SELECT flow_id, fact_key, fact_value
FROM agent_facts
WHERE flow_id = 'flow-alice';

# Count active conversations
SELECT COUNT(*) FROM agent_contexts;

# List all conversations by user
SELECT flow_id, created_at, last_updated_at
FROM agent_contexts
WHERE user_id = 'alice'
ORDER BY last_updated_at DESC;
```

**Configuration Options:**

The job uses default PostgreSQL configuration:
```java
config.put("postgres.url", "jdbc:postgresql://localhost:5432/flink_agents");
config.put("postgres.user", "postgres");
config.put("postgres.password", "postgres");
config.put("postgres.auto.create.tables", "true");
config.put("postgres.pool.max.size", "10");
config.put("postgres.pool.min.idle", "2");
```

**Test Conversation Persistence:**
```bash
# Run the job
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="postgresql"

# Stop it with Ctrl+C after a few messages

# Restart PostgreSQL (simulates database restart)
docker restart postgres-storage

# Run job again - conversations will be hydrated from PostgreSQL
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StorageIntegratedFlinkJob" \
  -Dexec.args="postgresql"
```

**Advantages:**
- ✅ Full ACID transactions
- ✅ Relational data model with SQL queries
- ✅ Long-term persistence (survives container restarts)
- ✅ Connection pooling for production workloads
- ✅ Industry-standard database (PostgreSQL)
- ✅ Easy to integrate with existing data infrastructure

**Use Cases:**
- Long-term conversation storage
- Compliance and audit requirements
- Complex queries across conversations
- Integration with existing PostgreSQL infrastructure
- Production deployments requiring ACID guarantees

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────┐
│              Flink Streaming Job                     │
│                                                       │
│  ┌──────────────────────────────────────────────┐  │
│  │  StorageAwareConversationProcessor           │  │
│  │                                               │  │
│  │  1. Check HOT storage for active context     │  │
│  │  2. If not found, hydrate from WARM          │  │
│  │  3. Process event, update context            │  │
│  │  4. Save to HOT storage                      │  │
│  │  5. Periodically persist to WARM             │  │
│  │  6. Report metrics                           │  │
│  └──────────────────────────────────────────────┘  │
│                 │                │                   │
│                 ▼                ▼                   │
│         ┌───────────┐    ┌───────────┐             │
│         │ HOT Tier  │    │ WARM Tier │             │
│         │ <1ms      │    │ 1-10ms    │             │
│         │ Active    │    │ Persist   │             │
│         └───────────┘    └───────────┘             │
│              │                 │                     │
└──────────────┼─────────────────┼─────────────────────┘
               │                 │
               ▼                 ▼
      ┌─────────────────────────────┐
      │   Pluggable Backend         │
      │                             │
      │   • In-Memory (ConcurrentMap)
      │   • Redis (Jedis)           │
      │   • PostgreSQL (JDBC + HikariCP) ✅
      │   • DynamoDB (planned)      │
      └─────────────────────────────┘
```

### Data Flow

1. **Event Arrives**: User message comes in
2. **HOT Check**: Look for active context in HOT storage (fast)
3. **WARM Hydration**: If not in HOT, load from WARM storage (persistence)
4. **Processing**: Add new context item, maintain conversation
5. **HOT Update**: Save updated context to HOT storage
6. **WARM Persistence**: Every N messages, save to WARM storage
7. **Metrics**: Track latency, success rate, operation counts

### Storage Keys

**HOT Tier (Active Context):**
- `agent:shortterm:{flowId}` → List of recent ContextItems (JSON)

**WARM Tier (Conversation Persistence):**
- `agent:context:{flowId}` → Complete AgentContext (JSON)
- `agent:facts:{flowId}` → Hash map of learned facts
- `agent:metadata:{flowId}` → Hash of metadata (userId, timestamps, etc.)
- `agent:conversations:active` → Set of active flow IDs
- `agent:user:{userId}` → Set of flow IDs for this user

## Configuration

### In-Memory Configuration

```java
Map<String, String> config = new HashMap<>();
config.put("cache.max.size", "10000");      // Max items
config.put("cache.ttl.seconds", "3600");    // 1 hour TTL
```

### Redis Configuration

```java
Map<String, String> config = new HashMap<>();
config.put("redis.host", "localhost");
config.put("redis.port", "6379");
config.put("redis.database", "0");
config.put("redis.ttl.seconds", "3600");
config.put("redis.pool.max.total", "20");   // Connection pool
config.put("redis.pool.max.idle", "5");
```

### PostgreSQL Configuration

```java
Map<String, String> config = new HashMap<>();
config.put("postgres.url", "jdbc:postgresql://localhost:5432/flink_agents");
config.put("postgres.user", "postgres");
config.put("postgres.password", "postgres");
config.put("postgres.auto.create.tables", "true");  // Auto-create schema
config.put("postgres.pool.max.size", "10");         // HikariCP max connections
config.put("postgres.pool.min.idle", "2");          // HikariCP min idle
config.put("postgres.connection.timeout", "30000"); // 30 seconds
config.put("postgres.idle.timeout", "600000");      // 10 minutes
```

**Environment Variables (Recommended for Production):**
```bash
export POSTGRES_URL="jdbc:postgresql://prod-db.example.com:5432/flink_agents"
export POSTGRES_USER="flink_app"
export POSTGRES_PASSWORD="secure-password-here"
```

## Monitoring

### Metrics Reported

Every 30 seconds, the job reports:

**HOT Storage:**
- Total operations
- Success rate (%)
- Average latency (ms)
- P95 latency (ms)
- P99 latency (ms)

**WARM Storage:**
- Same metrics as HOT

### Expected Latencies

| Backend    | HOT Tier | WARM Tier | Notes                    |
|------------|----------|-----------|--------------------------|
| In-Memory  | <1ms     | <1ms      | Single JVM only          |
| Redis      | 1-2ms    | 2-5ms     | Local Redis              |
| Redis      | 5-10ms   | 10-20ms   | Remote Redis (network)   |
| PostgreSQL | N/A      | 5-15ms    | Local PostgreSQL         |
| PostgreSQL | N/A      | 15-50ms   | Remote PostgreSQL        |

## Troubleshooting

### Redis Connection Failed

```
Error: Failed to connect to Redis at localhost:6379
```

**Solution:**
```bash
# Check if Redis is running
docker ps | grep redis

# If not, start it
docker run -d -p 6379:6379 redis:latest

# Check Redis logs
docker logs redis-storage
```

### PostgreSQL Connection Failed

```
Error: Failed to connect to PostgreSQL at localhost:5432
PSQLException: Connection refused
```

**Solution:**
```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# If not, start it
docker run -d --name postgres-storage \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=flink_agents \
  -p 5432:5432 \
  postgres:15-alpine

# Check PostgreSQL logs
docker logs postgres-storage

# Test connection
docker exec -it postgres-storage psql -U postgres -c "SELECT version();"
```

### PostgreSQL Schema Issues

```
PSQLException: relation "agent_contexts" does not exist
```

**Solution:**
Ensure auto-create is enabled in configuration:
```java
config.put("postgres.auto.create.tables", "true");
```

Or create tables manually:
```sql
CREATE TABLE IF NOT EXISTS agent_contexts (
    flow_id VARCHAR(255) PRIMARY KEY,
    context_json TEXT NOT NULL,
    user_id VARCHAR(255),
    agent_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agent_facts (
    flow_id VARCHAR(255) NOT NULL,
    fact_key VARCHAR(255) NOT NULL,
    fact_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (flow_id, fact_key)
);

CREATE INDEX idx_user_id ON agent_contexts(user_id);
CREATE INDEX idx_agent_id ON agent_contexts(agent_id);
CREATE INDEX idx_last_updated ON agent_contexts(last_updated_at);
```

### Out of Memory (In-Memory Backend)

```
OutOfMemoryError: Java heap space
```

**Solution:**
```bash
# Increase JVM heap size
export MAVEN_OPTS="-Xmx2g"
mvn exec:java ...

# Or reduce cache size
config.put("cache.max.size", "1000");
```

### Slow Performance

**Check Metrics:**
- If HOT tier >10ms → Check network to Redis
- If WARM tier >100ms → Check Redis memory/CPU
- If success rate <100% → Check Redis connection pool size

## Next Steps

### 1. Add Your Own Events

Replace `ConversationEventSource` with a real Kafka source:

```java
DataStream<AgentEvent> events = env
    .addSource(new FlinkKafkaConsumer<>("events", deserializer, kafkaProps))
    .assignTimestampsAndWatermarks(...);
```

### 2. Customize Storage

Create your own backend by implementing:
- `ShortTermMemoryStore` for HOT tier
- `LongTermMemoryStore` for WARM tier

See `STORAGE_ARCHITECTURE.md` for details.

### 3. Add More Tiers

Extend to 5-tier architecture:
- HOT: Redis (<1ms)
- WARM: Redis (1-10ms)
- COLD: PostgreSQL (10-100ms)
- VECTOR: Qdrant (5-50ms)
- CHECKPOINT: Flink RocksDB (Flink-managed)

### 4. Production Deployment

For production:
1. Use Redis Cluster for HA
2. Configure proper TTLs
3. Enable Redis persistence (AOF)
4. Monitor metrics with Prometheus
5. Set up alerting
6. Configure proper connection pool sizes
7. Enable TLS for Redis connections

## Additional Examples

See also:
- `PluggableStorageExample.java` - Basic API usage
- `STORAGE_ARCHITECTURE.md` - Complete architecture guide
- `DELIVERY_SUMMARY.md` - Implementation summary

## Performance Tips

### In-Memory Backend
- ✅ Fastest possible (<1ms)
- ❌ No persistence across restarts
- ❌ Limited to single JVM
- ✅ Perfect for development/testing

### Redis Backend
- ✅ Distributed - shared across all Flink tasks
- ✅ Persistence with RDB/AOF
- ✅ Scales horizontally with Redis Cluster
- ✅ Fast enough for production (1-10ms)
- ⚠️  Requires Redis infrastructure

### PostgreSQL Backend
- ✅ Full ACID transactions and data integrity
- ✅ Relational model - complex queries with SQL
- ✅ Industry-standard database with broad tooling support
- ✅ Long-term persistence and archival
- ✅ Connection pooling via HikariCP
- ⚠️  Slower than Redis (10-50ms) but acceptable for WARM tier
- ⚠️  Requires PostgreSQL infrastructure
- ✅ Best for: compliance, audit trails, long-term storage

### Backend Comparison

| Feature | In-Memory | Redis | PostgreSQL |
|---------|-----------|-------|------------|
| **Setup Complexity** | None | Medium | Medium |
| **HOT Tier Latency** | <1ms | 1-5ms | N/A (WARM only) |
| **WARM Tier Latency** | <1ms | 2-10ms | 5-50ms |
| **Persistence** | ❌ | ✅ (RDB/AOF) | ✅ (ACID) |
| **Distributed** | ❌ | ✅ | ✅ |
| **SQL Queries** | ❌ | ❌ | ✅ |
| **Transactions** | ❌ | ⚠️ (limited) | ✅ (full ACID) |
| **Best For** | Dev/Test | Production HOT+WARM | Production WARM/archive |
| **Cost** | Free | Infrastructure | Infrastructure |
| **Scalability** | Single JVM | Horizontal (cluster) | Vertical + replication |

**Recommendation:**
- **Development:** In-Memory (fastest, no dependencies)
- **Production HOT tier:** Redis (fast + distributed)
- **Production WARM tier:** Redis or PostgreSQL (choose based on query needs)
- **Long-term archive:** PostgreSQL (SQL queries, compliance, audit)

### Best Practices

1. **Use HOT tier for active context** (frequently accessed)
2. **Use WARM tier for persistence** (survive restarts)
3. **Monitor metrics** to catch performance issues
4. **Set appropriate TTLs** to prevent memory bloat
5. **Use connection pooling** for Redis and PostgreSQL
6. **Test hydration** to ensure conversations survive restarts
7. **Choose PostgreSQL for** compliance, audit trails, complex queries
8. **Choose Redis for** low latency, high throughput, simple key-value
