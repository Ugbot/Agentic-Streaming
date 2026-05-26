# Context Window Management for Agentic Framework

A sophisticated context management system using MoSCoW prioritization, intelligent compaction, and inverse RAG for long-term memory.

## Overview

The context window management system provides:
- **Size-limited context windows** with automatic overflow handling
- **MoSCoW prioritization** (MUST, SHOULD, COULD, WONT)
- **Intelligent compaction** with relevancy scoring
- **Memory hierarchy** (Short-term, Long-term, Steering)
- **Inverse RAG** - pushing context to long-term storage
- **Temporal relevancy** with access pattern tracking

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                  Context Window Management                        │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Agent Event ──> Context Check ──┬──> Space Available ──> Process │
│                                   │                               │
│                                   └──> Overflow ──> Compact       │
│                                                        │          │
│                                                        v          │
│                            Compaction Service                     │
│                            ├─> Priority Filter (MoSCoW)           │
│                            ├─> Relevancy Scorer                   │
│                            ├─> Temporal Analysis                  │
│                            └─> Summarization                      │
│                                     │                             │
│                       ┌─────────────┴──────────────┐              │
│                       v                            v              │
│               Long-Term Storage              Discard              │
│               (Inverse RAG)                  (Forget)             │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

## MoSCoW Prioritization

### Priority Levels

**MUST (Retention Score: 1.0)**
- Hard facts, never discarded
- Immutable domain knowledge
- User preferences and constraints
- Critical context for task completion

**SHOULD (Retention Score: 0.7)**
- Important but compressible
- Can be summarized if space needed
- Recently accessed information
- Relevant tool execution results

**COULD (Retention Score: 0.5)**
- Nice to have, easily discarded
- Supplementary information
- Low-relevancy context
- Older, rarely accessed items

**WONT (Retention Score: 0.0)**
- Not relevant, discard immediately
- Explicitly marked as irrelevant
- Contradictory or outdated info
- Unrelated context

### Example Usage

```java
// Create context items with priorities
ContextItem mustKeep = new ContextItem(
    "User prefers detailed technical explanations",
    ContextPriority.MUST,
    MemoryType.LONG_TERM
);

ContextItem shouldKeep = new ContextItem(
    "Apache Flink provides exactly-once guarantees",
    ContextPriority.SHOULD,
    MemoryType.SHORT_TERM
);

ContextItem couldKeep = new ContextItem(
    "RocksDB backend supports incremental checkpoints",
    ContextPriority.COULD,
    MemoryType.SHORT_TERM
);

ContextItem wontKeep = new ContextItem(
    "Docker containers provide isolation",  // Unrelated
    ContextPriority.WONT,
    MemoryType.SHORT_TERM
);
```

## Memory Hierarchy

### Short-Term Memory

Ephemeral working memory, cleared on timeout.

**Use Cases:**
- Current conversation
- Tool execution results
- Temporary context
- Working state

**Configuration:**
```java
ShortTermMemory shortTerm = new ShortTermMemory(50);  // Max 50 items
shortTerm.add(contextItem);
shortTerm.removeOldItems(Duration.ofHours(1).toMillis());
```

### Long-Term Memory

Persistent facts that survive agent restarts.

**Use Cases:**
- Hard facts about the world
- User preferences
- Domain knowledge
- Historical context

**Configuration:**
```java
LongTermMemory longTerm = new LongTermMemory();
// Only MUST priority items
longTerm.addFact(mustItem);
```

### Steering State

MoSCoW rules and constraints that guide agent behavior.

**Use Cases:**
- Hard constraints (MUST)
- Strong preferences (SHOULD)
- Nice-to-haves (COULD)
- Explicit avoidance (WONT)

**Configuration:**
```java
SteeringState steering = new SteeringState();
steering.addMust("rule-001", "Accuracy required", "must be factually correct");
steering.addShould("rule-002", "Be concise", "should be brief");
steering.addCould("rule-003", "Add examples", "could include examples");
steering.addWont("rule-004", "No speculation", "don't guess");
```

## Compaction Process

### Phases

**Phase 1: Remove WONT Items**
```
Immediately discard all WONT priority items
- No relevancy check needed
- Always removed first
```

**Phase 2: Score Relevancy**
```
Score all remaining items for relevancy:
- Semantic similarity to current intent (50%)
- Temporal relevancy (20%)
- Access frequency (15%)
- Priority boost (15%)
```

**Phase 3: Remove Low-Relevancy COULD Items**
```
Remove COULD items with relevancy < threshold (e.g., 0.5)
- Keeps high-relevancy COULD items
- Discards irrelevant supplementary info
```

**Phase 4: Compress SHOULD Items**
```
If still over limit:
- Sort SHOULD items by relevancy
- Remove bottom 50%
- Keep top items
```

**Phase 5: Promote to Long-Term**
```
Identify items for long-term storage:
- Relevancy score >= 0.7
- Priority == MUST
- Push to vector store (Inverse RAG)
```

### Compaction Configuration

```java
ContextWindowManager.ContextWindowConfig config =
    new ContextWindowManager.ContextWindowConfig();
config.setMaxTokens(4000);
config.setMaxItems(50);
config.setCompactionThreshold(0.8);  // Trigger at 80% full
config.setRelevancyThreshold(0.5);   // Keep items above 0.5
config.setMustKeepThreshold(1.0);
config.setShouldKeepThreshold(0.7);
config.setCouldKeepThreshold(0.5);
```

## Relevancy Scoring

### Components

1. **Semantic Similarity** (50% weight)
   - Compare context to current intent
   - Uses keyword matching or embeddings
   - Range: 0.0 - 1.0

2. **Temporal Relevancy** (20% weight)
   - Exponential decay: e^(-0.1 * hours_since_access)
   - Recent items score higher
   - Range: 0.0 - 1.0

3. **Access Frequency** (15% weight)
   - Normalize by 10 accesses
   - Frequently accessed = more relevant
   - Range: 0.0 - 1.0

4. **Priority Boost** (15% weight)
   - Direct from MoSCoW score
   - MUST=1.0, SHOULD=0.7, COULD=0.5, WONT=0.0

### Example Calculation

```
Item: "Flink provides stateful processing"
Intent: "Understand Flink state management"

Semantic:  0.9  (high keyword match)
Temporal:  0.7  (accessed 2 hours ago)
Access:    0.3  (3 accesses)
Priority:  0.7  (SHOULD)

Final = (0.9 * 0.5) + (0.7 * 0.2) + (0.3 * 0.15) + (0.7 * 0.15)
      = 0.45 + 0.14 + 0.045 + 0.105
      = 0.74  ✓ Keep (above 0.5 threshold)
```

## Inverse RAG

Traditional RAG retrieves context. Inverse RAG **stores** context.

### Process

1. **Identify High-Value Items**
   - Relevancy score >= 0.7
   - Priority == MUST
   - Accessed multiple times

2. **Chunk and Embed**
   - Split into manageable chunks
   - Generate embeddings
   - Add metadata (agent_id, intent, timestamp)

3. **Store in Vector DB**
   - Push to Qdrant collection
   - Use separate collection: "agent-long-term-memory"
   - Tag for future retrieval

4. **Future Retrieval**
   - When agent restarts, can retrieve relevant history
   - Semantic search on past context
   - Hydrate agent with prior knowledge

### Configuration

```java
Map<String, String> config = new HashMap<>();
config.put("host", "localhost");
config.put("port", "6333");
config.put("collectionName", "agent-long-term-memory");

// Metadata for retrieval
metadata.put("agent_id", "agent-001");
metadata.put("flow_id", "flow-123");
metadata.put("intent_tag", "state-management");
metadata.put("priority", "MUST");
metadata.put("created_at", String.valueOf(timestamp));
```

## Integration with Agent Framework

### Context-Aware Agent Processing

```java
// Before processing agent event
if (contextManager.needsCompaction(agentContext)) {
    // Route to compaction
    CompactionRequest request = contextManager.createCompactionRequest(
        agentContext,
        currentIntent
    );
    ctx.output(COMPACTION_TAG, request);
    return;  // Wait for compaction
}

// Process normally
agentFunction.process(event, agentContext);
```

### Compaction Pipeline

```java
// Main agent stream
DataStream<AgentEvent> agentEvents = ...;

// Check context, route overflows to compaction
SingleOutputStreamOperator<AgentEvent> processedEvents =
    agentEvents
        .keyBy(AgentEvent::getFlowId)
        .process(new ContextCheckFunction())
        .uid("context-check");

// Get compaction side output
DataStream<CompactionRequest> compactionRequests =
    processedEvents.getSideOutput(COMPACTION_TAG);

// Compact
DataStream<CompactionResult> compactionResults =
    compactionRequests
        .process(new ContextCompactionFunction(0.5, 0.7))
        .uid("compaction");

// Inverse RAG
DataStream<InverseRagResult> storageResults =
    compactionResults
        .process(new InverseRagFunction())
        .uid("inverse-rag");

// Union compacted results back to main stream
DataStream<AgentEvent> allEvents =
    processedEvents.union(
        compactionResults.map(this::resultToEvent)
    );
```

## Performance Considerations

### Token Estimation

```java
// Rough estimation: 4 characters per token
int tokens = text.length() / 4;

// More accurate: use tokenizer library
int tokens = tokenizer.encode(text).size();
```

### Batch Compaction

For high-throughput scenarios:
```java
// Batch compaction every N seconds or M items
KeyedProcessFunction<String, AgentEvent, CompactionRequest> {
    private ListState<AgentEvent> pendingEvents;

    @Override
    public void processElement(AgentEvent event, Context ctx) {
        pendingEvents.add(event);

        if (pendingEvents.get().spliterator().getExactSizeIfKnown() >= 10) {
            triggerBatchCompaction();
        }
    }
}
```

### Async Relevancy Scoring

```java
// Score items in parallel
CompletableFuture<Map<String, Double>> scores =
    relevancyScorer.scoreAll(items, intent);

scores.thenAccept(scoreMap -> {
    // Use scores for compaction decisions
});
```

## Monitoring

### Key Metrics

- Compaction frequency (per agent, per hour)
- Tokens saved (original - compacted)
- Compression ratio (%)
- Items removed by priority (WONT, COULD, SHOULD)
- Items promoted to long-term
- Relevancy score distribution
- Compaction latency (p50, p99)

### Logging

```java
LOG.info("Compaction complete: {}", result);
// Output: Compaction complete: CompactionResult[
//   tokens: 1200→800 (saved 400, 33.3%),
//   removed=5, summarized=3, promoted=2, time=145ms
// ]
```

## Running the Example

```bash
# 1. Start dependencies
ollama serve
docker run -p 6333:6333 qdrant/qdrant

# 2. Pull models
ollama pull nomic-embed-text:latest

# 3. Build project
mvn clean install

# 4. Run example
java -cp target/agentic-flink-0.0.1-SNAPSHOT.jar \
  org.agentic.flink.example.ContextManagementExample
```

## Best Practices

1. **Set Appropriate Limits**
   - Match model context window (e.g., 4K tokens)
   - Leave headroom for responses
   - Trigger compaction at 80% full

2. **Prioritize Correctly**
   - MUST: Only truly immutable facts
   - SHOULD: Most working context
   - COULD: Supplementary info
   - WONT: Explicitly irrelevant

3. **Tune Thresholds**
   - Relevancy threshold: 0.5 is reasonable
   - Long-term promotion: 0.7+ for quality
   - Compaction trigger: 0.8 = proactive

4. **Monitor Memory**
   - Track compaction frequency
   - Alert on excessive compactions
   - Review what gets discarded

5. **Test Retention**
   - Verify MUST items always kept
   - Check long-term promotion works
   - Ensure relevant context survives

## Future Enhancements

- **Semantic summarization** using LLM
- **Hierarchical contexts** (nested windows)
- **Context sharing** between agents
- **Adaptive thresholds** based on performance
- **Context versioning** and rollback
- **Distributed context** for multi-agent systems

## References

- [MoSCoW Method](https://en.wikipedia.org/wiki/MoSCoW_method)
- [Inverse RAG Pattern](https://arxiv.org/abs/2305.14283)
- [LangChain Memory](https://python.langchain.com/docs/modules/memory/)
