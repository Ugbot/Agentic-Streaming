# Memory in Agentic Flink

Agentic Flink is **Flink-state-first**. The short-term memory of an agent — its conversation context, active reasoning chain, recent tool results — lives in Flink keyed state, durably checkpointed alongside the rest of the job. External stores (Postgres, Redis, Kafka) are optional and play specific, narrow roles. There is no separate "HOT cache" in front of Flink state.

## Layout

| Memory                 | Where it lives                           | When you need it                                          |
|------------------------|------------------------------------------|-----------------------------------------------------------|
| Short-term             | Flink `ValueState` + `MapState`          | Always. Default. No infra to run.                         |
| Long-term              | `LongTermMemoryStore` (Postgres default) | Conversation resumption across job restarts; fact archive.|
| Vector / semantic      | Flink `MapState` (brute-force KNN)       | Conversation-local semantic recall. Default, in-JVM.      |
| External vector store  | `VectorStore` SPI (user-supplied)        | When in-JVM brute-force is no longer fast enough.         |
| External feed          | `Channel<KeyedContextItem>` (Kafka/Postgres CDC/Redis pub/sub/webhook) | When another process needs to push memories in. |

## Short-term memory

Use the default:

```java
ShortTermMemorySpec spec = FlinkStateShortTermMemory.spec(Duration.ofMinutes(30));
```

In your `RichFunction.open()`:

```java
ShortTermMemory memory = spec.bind(getRuntimeContext());
```

Then in `processElement`, the memory is implicitly scoped to the operator's current key — no `flowId` arg, because Flink supplies it.

TTL is set per agent via `AgentBuilder.withShortTermTtl(Duration)` and falls back to the config key `memory.shortterm.ttl.seconds`. State cleanup is incremental and runs alongside the state-backend's own work.

## Long-term memory (optional)

```java
LongTermMemoryStore postgres = StorageFactory.createLongTermStore("postgres", postgresConfig);
agent = Agent.builder().withId(...).withLongTermStore(postgres).build();
```

Hydration happens on the first event seen for a cold key: the operator reads the conversation context and facts from Postgres into Flink state, after which all reads and writes hit Flink state directly. Sync back to Postgres is write-behind, triggered on event-count intervals and on successful MoSCoW compaction. Checkpoint barriers never block on Postgres acks.

Redis is supported via the `RedisLongTermStore` (formerly `RedisConversationStore`) but is no longer the default. Pull it in by selecting `"redis"` from the factory or by registering the implementation via `ServiceLoader`.

## Vector memory

Default: in-JVM brute-force KNN over Flink `MapState`. At d=768, brute-force handles the typical "conversation-local recall" workload (hundreds to low thousands of vectors per key) in well under a millisecond. The state itself is checkpointed; no graph is materialized outside of an active search.

```java
agent = Agent.builder()
    .withVectorMemory(FlinkStateVectorMemory.spec(768))
    .build();
```

For larger graphs (10⁵+ vectors per key), drop in a JVector- or Lucene-HNSW-backed `VectorMemorySpec` via `ServiceLoader`. The default does not pull a heavyweight ANN library into the artifact.

## Memory feeds (now `Channel<KeyedContextItem>`)

A memory feed is just a `Channel<KeyedContextItem>` — the framework's unified
continuous-input primitive. External producers push `KeyedContextItem`
records on any channel transport; the agent operator union-connects them so
the records land in Flink state through the same write path as in-band
events.

```java
agent = Agent.builder()
    .withMemoryChannel(
        new KafkaContextChannel("kafka:9092", "agent-memories", "agentic-flink"),
        new PostgresChangeChannel(url, user, pass))
    .build();
```

Channels are transport-agnostic: a `RedisPubSubChannel`, `WebhookChannel`,
or custom `Channel<KeyedContextItem>` works the same way. See
[`docs/channels.md`](channels.md) for the full SPI.

## Service discovery

The framework discovers `LongTermMemoryStore`, `VectorStore`, and
`ShortTermMemorySpec` implementations through `java.util.ServiceLoader`.
Channels are configured programmatically through `AgentBuilder` /
`ChannelRegistry`. Built-in service entries live under
`src/main/resources/META-INF/services/`; third parties register their own
by dropping a jar that contains the matching service files.
