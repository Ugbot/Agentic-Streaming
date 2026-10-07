# Memory in Agentic Flink

Agentic Flink is **Flink-state-first**. The short-term memory of an agent, its conversation context, active reasoning chain, recent tool results, lives in Flink keyed state, durably checkpointed alongside the rest of the job. External stores (Postgres, Redis, Kafka) are optional and play specific, narrow roles. There is no separate "HOT cache" in front of Flink state.

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

Then in `processElement`, the memory is implicitly scoped to the operator's current key, no `flowId` arg, because Flink supplies it.

TTL is set on the spec (`FlinkStateShortTermMemory.spec(Duration)`); `Duration.ZERO` disables it. State cleanup is incremental and runs alongside the state-backend's own work. `AgentBuilder.withShortTermTtl(Duration)` and `withShortTermMemory(ShortTermMemorySpec)` record values on the built `Agent` that no operator in this repository reads yet, so configure the spec on the operator that binds it. The config key `memory.shortterm.ttl.seconds` is declared in `ConfigKeys` but is not read by any class.

## Long-term memory (optional)

```java
LongTermMemoryStore postgres = StorageFactory.createLongTermStore("postgres", postgresConfig);
```

`AgentBuilder.withLongTermStore(store)` stores the reference on the `Agent` but no operator in the default build reads it. The operator that performs hydration and write-behind today is `ContextManagementActionWithStorage` in `plugins/flintagents` (compiled only with `-P flink-agents`): on the first event for a cold key it reads the conversation context and facts from the long-term store into Flink state, after which reads and writes hit Flink state directly; sync back is write-behind, triggered on event-count intervals and on successful MoSCoW compaction, and checkpoint barriers do not block on store acks.

Redis is supported via `org.agentic.flink.storage.redis.RedisConversationStore` but is not the default. Select it with `StorageFactory.createLongTermStore("redis", redisConfig)`; the factory resolves the name through `ServiceLoader`. The Jedis client is an optional dependency and must be on the job classpath.

## Vector memory

Default: in-JVM brute-force KNN over Flink `MapState`. At d=768, brute-force handles the typical "conversation-local recall" workload (hundreds to low thousands of vectors per key) in well under a millisecond. The state itself is checkpointed; no graph is materialized outside of an active search.

```java
VectorMemorySpec spec = FlinkStateVectorMemory.spec(768);
```

A `VectorMemorySpec` is bound in `open()` the same way as a `ShortTermMemorySpec`; `SingleOperatorCorpus.spec(name, vectorSpec)` is the shipped consumer. `AgentBuilder.withVectorMemory(spec)` records the spec on the `Agent` but no operator reads it yet.

For larger graphs the module ships `FlinkStateHnswVectorMemory.spec(dimension)`, a per-key HNSW-style index over the same Flink `MapState` that is rebuilt from state on first access after a restore. Any other `VectorMemorySpec` implementation can be passed in the same place; there is no `ServiceLoader` lookup for `VectorMemorySpec`. The default does not pull a heavyweight ANN library into the artifact.

## Memory feeds (now `Channel<KeyedContextItem>`)

A memory feed is just a `Channel<KeyedContextItem>`, the framework's unified
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

`withMemoryChannel` records the channels on the `Agent`; no operator in this repository consumes that list yet, so wire channels into your job graph directly (see [`docs/channels.md`](channels.md)).

Channels are transport-agnostic: a `RedisPubSubChannel`, `WebhookChannel`,
or custom `Channel<KeyedContextItem>` works the same way. See
[`docs/channels.md`](channels.md) for the full SPI.

## Service discovery

The framework discovers `LongTermMemoryStore`, `VectorStore`, and
`ConversationStore` implementations through `java.util.ServiceLoader`.
`ShortTermMemorySpec` and `VectorMemorySpec` are passed programmatically.
Built-in service entries live under `src/main/resources/META-INF/services/`;
third parties register their own by dropping a jar that contains the matching
service files. See [Storage Architecture](reference/storage-architecture.md)
for the registered classes and
[Creating Storage Backends](guides/creating-storage-backends.md) for the
registration procedure.
