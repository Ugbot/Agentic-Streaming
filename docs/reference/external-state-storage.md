# External State Storage for Conversation Persistence

**Status:** Interface defined, template implementations provided
**Production Ready:** No - requires actual storage client integration
**Last Updated:** 2025-10-17

## Overview

This module provides a pluggable interface for persisting agent conversations to external storage systems. This enables conversations to be resumed across job restarts and accessed from multiple systems.

## Implementation Status

### Completed
- `ExternalStateStore` interface (fully documented)
- Method signatures for all required operations
- Three template implementations showing integration patterns

### Template Implementations (Not Production-Ready)
- `RedisStateStore` - Shows Redis integration pattern (requires Jedis dependency)
- `DynamoDBStateStore` - Shows DynamoDB pattern (requires AWS SDK)
- `PostgreSQLStateStore` - Shows RDBMS pattern (requires JDBC)

**Note:** Template implementations have placeholder logic and require actual storage client code to function.

## Architecture

```
Flink State Backend                External State Store
(Active Processing)                (Long-Term Persistence)

ValueState<AgentContext> ---------> saveContext(flowId, context)
                                    │
ListState<ContextItem>   ---------> saveContextItems(flowId, items)
                                    │
MapState<String, Item>   ---------> saveLongTermFacts(flowId, facts)
                                    │
                                    ↓
                        [Redis / DynamoDB / PostgreSQL]
                                    │
                                    ↑
                        loadContext(flowId) ---------> Resume conversation
```

## Interface Methods

### Conversation Operations
```java
void saveContext(String flowId, AgentContext context)
Optional<AgentContext> loadContext(String flowId)
boolean conversationExists(String flowId)
void deleteConversation(String flowId)
```

### Context Item Operations
```java
void saveContextItems(String flowId, List<ContextItem> items)
List<ContextItem> loadContextItems(String flowId)
void saveLongTermFacts(String flowId, Map<String, ContextItem> facts)
Map<String, ContextItem> loadLongTermFacts(String flowId)
```

### Metadata Operations
```java
List<String> listActiveConversations()
List<String> listConversationsForUser(String userId)
Map<String, Object> getConversationMetadata(String flowId)
```

## Redis Template Implementation

File: `RedisStateStore.java`

**Data Structure:**
```
agent:context:{flowId}     -> Hash (AgentContext JSON)
agent:items:{flowId}       -> List (ContextItem JSON)
agent:facts:{flowId}       -> Hash (itemId -> ContextItem)
agent:metadata:{flowId}    -> Hash (metadata fields)
```

**Required Dependencies:**
```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>4.4.0</version>
</dependency>
```

**Configuration:**
```java
Map<String, String> config = new HashMap<>();
config.put("redis.host", "localhost");
config.put("redis.port", "6379");
config.put("redis.ttl.seconds", "86400");  // 24 hours
```

**Status:** Template only - requires uncommenting Jedis client code

## DynamoDB Template Implementation

File: `DynamoDBStateStore.java`

**Table Schema:**
```
Table: agent_conversations
  Partition Key: flowId (String)
  Sort Key: itemType (String)

GSI: userId-index
  Partition Key: userId
  Sort Key: lastUpdatedAt
```

**Required Dependencies:**
```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>dynamodb</artifactId>
    <version>2.20.0</version>
</dependency>
```

**Status:** Template only - requires AWS SDK integration

## PostgreSQL Template Implementation

File: `PostgreSQLStateStore.java`

**Schema:**
```sql
CREATE TABLE agent_contexts (
  flow_id VARCHAR(255) PRIMARY KEY,
  agent_id VARCHAR(255),
  user_id VARCHAR(255),
  context_data JSONB,
  created_at TIMESTAMP,
  last_updated_at TIMESTAMP
);

CREATE TABLE agent_context_items (
  id SERIAL PRIMARY KEY,
  flow_id VARCHAR(255),
  item_data JSONB,
  FOREIGN KEY (flow_id) REFERENCES agent_contexts(flow_id)
);
```

**Required Dependencies:**
```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.6.0</version>
</dependency>
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>5.0.1</version>
</dependency>
```

**Status:** Template only - requires JDBC connection pool integration

## Integration with ContextManagementAction

The external state store integrates with the stateful processing function:

```java
public class ContextManagementAction extends KeyedProcessFunction<String, Event, Event> {

    private transient ExternalStateStore externalStore;
    private ValueState<AgentContext> contextState;  // Flink state

    @Override
    public void open(Configuration config) {
        // Initialize Flink state
        contextState = getRuntimeContext().getState(...);

        // Initialize external store
        externalStore = new RedisStateStore();
        externalStore.initialize(storeConfig);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) {
        String flowId = event.getAttr("flowId");

        // Try to load from Flink state first
        AgentContext context = contextState.value();

        // If not in Flink state, hydrate from external store
        if (context == null) {
            Optional<AgentContext> loaded = externalStore.loadContext(flowId);
            context = loaded.orElse(createNewContext(event));
            contextState.update(context);  // Populate Flink state
        }

        // Process event...

        // Persist to external store periodically
        if (shouldPersist()) {
            externalStore.saveContextAsync(flowId, context);
        }
    }
}
```

**Status:** Integration pattern defined - actual implementation pending

## Use Cases

### Resume Conversation After Job Restart
1. Job fails or is redeployed
2. New job instance starts
3. First event for flowId arrives
4. ContextManagementAction checks Flink state (empty)
5. Loads context from external store
6. Populates Flink state
7. Processing continues

### Cross-Job Context Sharing
1. Job A processes user requests
2. Periodically saves to external store
3. Job B (analytics) reads from same store
4. Job C (notifications) queries conversation metadata

### Long-Term Conversation History
1. Flink checkpoints expire after retention period
2. External store retains data longer (configurable TTL)
3. Historical analysis possible
4. User conversation history accessible

## Production Considerations

### Required Work for Production
1. Implement actual storage client code (Redis/DynamoDB/PostgreSQL)
2. Add error handling and retry logic
3. Implement connection pooling
4. Add monitoring and metrics
5. Test failover scenarios
6. Tune TTL and retention policies

### Performance Characteristics

| Store | Read Latency | Write Latency | Scalability | Cost |
|-------|--------------|---------------|-------------|------|
| Redis | <1ms | <1ms | Horizontal | Medium |
| DynamoDB | 1-10ms | 1-10ms | Automatic | Variable |
| PostgreSQL | 1-5ms | 1-5ms | Vertical | Low |

### Recommended Architecture

```
Hot Path (Active Processing):
  Flink State Backend (RocksDB)
  - Sub-millisecond access
  - Checkpointed for fault tolerance

Warm Path (Recent Conversations):
  Redis or DynamoDB
  - TTL: 24-72 hours
  - Fast resume capability

Cold Path (Historical Data):
  PostgreSQL or S3
  - TTL: 30-90 days
  - Analytics and compliance
```

## Configuration Example

```java
// Configure external store
Map<String, String> storeConfig = new HashMap<>();
storeConfig.put("redis.host", "prod-redis.example.com");
storeConfig.put("redis.port", "6379");
storeConfig.put("redis.password", System.getenv("REDIS_PASSWORD"));
storeConfig.put("redis.ttl.seconds", "86400");  // 24 hours

// Initialize in ContextManagementAction
ExternalStateStore store = new RedisStateStore();
store.initialize(storeConfig);
```

## Testing

Template implementations include logging but no actual storage operations. To test:

1. Implement actual storage client code
2. Set up local storage instance (Docker recommended)
3. Run integration tests
4. Verify data persists across restarts

## Limitations

**Current Implementation:**
- Interface and templates only
- No actual storage operations
- No production testing
- No error handling beyond logging

**Production Requirements:**
- Storage client implementation
- Connection management
- Error handling and retries
- Monitoring and alerting
- Performance tuning
- Security (encryption at rest/transit)

## Next Steps

1. Choose storage backend (Redis recommended for MVP)
2. Add storage client dependency to pom.xml
3. Implement actual storage operations in chosen template
4. Add integration tests
5. Deploy and validate with real workload
6. Monitor performance and tune as needed

---

**Summary:** External state storage interface is defined with clear method signatures and integration patterns. Template implementations provide scaffolding but require actual storage client code for production use. This architecture enables conversation resumption and cross-job sharing when fully implemented.
