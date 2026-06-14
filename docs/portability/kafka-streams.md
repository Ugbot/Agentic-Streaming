# Agentic-Flink on Kafka Streams (JVM)

> Per-engine doc in the `docs/portability/` series. Read
> [`00-essence-and-core-abstractions.md`](./00-essence-and-core-abstractions.md)
> first — this doc is written against the essence (§2), the capability inventory
> (§3 = C1..C12), the Engine SPI (§4c), the capability matrix (§6), and follows
> the §9 six-section template. Code is Java; the pure core (§4a) is reused
> *byte-for-byte*.

## 1. Verdict

**Kafka Streams is the closest overall analog to Flink, and the only JVM target
that reuses the existing Java core unchanged.** The mechanical correspondence is
near 1:1: the Processor API (`Processor` / `ProcessorSupplier` +
`KeyValueStore` / `TimestampedKeyValueStore`) is Flink's `KeyedProcessFunction` +
`ValueState`; a `KStream.repartition` / `selectKey` on `conversationId` is Flink's
`keyBy`, giving you one logical processor per conversation with a *partition-local
RocksDB state store* keyed the same way (C1 + C2, native); `processing.guarantee=
exactly_once_v2` gives transactional EOS over the read-process-write cycle (C3,
native); Interactive Queries are queryable state; `GlobalKTable` is broadcast
(C8); and the `Topology` built by `StreamsBuilder` / `Topology.addProcessor` *is*
the DAG (C12).

You **keep** the entire pure core — `ConversationStore`, `ToolExecutor` /
`ToolRegistry`, the `TwoTierRetriever` + RAG step logic, the A2A protocol types
and `ResilientA2AClient`, the ReAct `TurnBrain` — with **zero edits**, because
none of it imports Flink. You **re-implement** only the §4c Engine seam: the agent
processor (over `Processor` instead of `KeyedProcessFunction`), the
`KeyedStateStore` over a `KeyValueStore`, and the `Channel` source/sink over Kafka
topics.

The **one real weakness** is C4: Kafka Streams has **no native async-I/O
operator**. Every agent turn is dominated by a slow LLM / A2A call, and a
`Processor.process()` call runs *on the `StreamThread`* — block it and you stall
the whole partition (and every conversation hashed to it) plus delay
`commit.interval.ms`, threatening the `max.poll.interval.ms` rebalance trip-wire.
This is the single design decision that defines the port, so §3 details it
honestly: the recommended pattern is **async-completion via a response topic**, not
a blocking thread-pool.

Choose Kafka Streams when you are already a Kafka shop, want a *library* (not a
cluster) embedded in your own JVM service, and value transactional EOS over a
clean async story.

## 2. Capability mapping (C1..C12)

Using the §6 "Kafka Streams" column; concrete mechanism named per row.

| Cap | §6 | Mechanism on Kafka Streams |
|-----|:--:|----------------------------|
| **C1** durable keyed state | **N** | A partition-local **state store** (`KeyValueStore` / `TimestampedKeyValueStore`, RocksDB-backed) registered via `StoreBuilder`, changelog-backed to a compacted internal topic for fault tolerance. Direct analog of Flink `ValueState`/`MapState`. Holds short-term memory, `RoutingBudget`, A2A `contextId`, dedup windows. |
| **C2** per-key ordered proc | **N** | **Topic partitions.** Repartition (`repartition`/`selectKey`+through) on `conversationId` so each key lands on one partition, processed by one `StreamTask` on one `StreamThread` — single-writer-per-conversation, in order, no locks. Exactly Flink's `keyBy`. |
| **C3** fault tolerance / EOS | **N** | `processing.guarantee=exactly_once_v2`: Kafka transactions span consume-offset + state-store changelog + output produce atomically. Stronger than Flink's "≈ EOS" for the all-Kafka path; weaker only for *external* side effects (LLM/tool calls — covered by idempotency, see §5). |
| **C4** async I/O | **L** *(the weak spot)* | **No native operator.** Two bridges, detailed in §3.6: **(a)** *async-completion* — fire the LLM/A2A call from `process()`, persist a `PENDING` record in a store, return immediately; the reply arrives on a **response topic** that re-enters the topology keyed by `conversationId` and resumes the turn (recommended — never blocks the `StreamThread`). **(b)** *bounded blocking thread-pool* inside the processor (simpler, but **blocks the partition** — only viable for fast calls / low fan-in). A wall-clock **punctuator** (`PunctuationType.WALL_CLOCK_TIME`) sweeps timed-out pendings. |
| **C5** backpressure | **L** | Consumer pause/resume via `max.poll.records` + in-flight bounds; an explicit `Semaphore` around model calls to honor rate limits. No end-to-end credit-based backpressure like Flink, but the pull model + bounded async-in-flight is sufficient. |
| **C6** connectors | **N** *(Kafka-centric)* | Sources/sinks are **Kafka topics** (`StreamsBuilder.stream`/`KStream.to`). The `Channel<T>` SPI maps to topic stream/sink. Non-Kafka transports (Redis, Fluss, ZMQ, webhook, Postgres) are reached via **processor-level clients** (a Jedis/JDBC handle opened in `init()`), or front them with Kafka Connect. |
| **C7** side outputs | **L** | `KStream.branch`/`split()` or simply `context.forward(k, v, To.child("debug"))` to a named downstream node — debug stream + tool-invocation channel become extra child nodes / topics. |
| **C8** broadcast state | **N** | **`GlobalKTable`** (fully replicated to every instance) for control-plane directives / enrichment dims, joined against the keyed stream. |
| **C9** event-time / windows | **N** | Windowed stores + `TimeWindows`/`SessionWindows`; record timestamps + `TimestampExtractor`. Covers feature aggregation; CEP timing is custom. |
| **C10** CEP | **L** | No CEP library. Implement pattern detection as a **custom `Processor`** over a windowed store (NFA-as-state). One optional module; rarely needed. |
| **C11** distributed scale | **N** | Instances of the same `application.id` form a consumer group; partitions (and their state stores) rebalance across instances. Scale = add instances, up to partition count. Standby replicas (`num.standby.replicas`) for fast failover. |
| **C12** topology builder | **N** | `Topology` / `StreamsBuilder` *is* the DAG. `addSource → addProcessor → addStateStore → addSink`. Direct analog of the Flink `StreamExecutionEnvironment` graph. |

Net: **C1, C2, C3, C6, C8, C9, C11, C12 native; C5/C7/C10 idiomatic; C4 is the
one you engineer around.**

## 3. The core abstractions on this engine

### 3.0 The §4c Engine SPI realized

```
EngineRuntime         → a Topology + KafkaStreams instance
  source(ChannelSpec) → StreamsBuilder.stream(topic, Consumed.with(serdes))
  sink(stream, spec)  → KStream.to(topic, Produced.with(serdes))
  keyedAgent(...)     → repartition(by conversationId) + addProcessor(AgentProcessor)
                        + addStateStore(KeyValueStore)
  asyncStage(...)     → async-completion via response topic (§3.6) — NOT a blocking call
  route(...)          → split()/branch by Router verdict, merge into verifier node
  execute(name)       → new KafkaStreams(topology, props).start()

KeyedStateStore       → a thin adapter over KeyValueStore<String, byte[]>
```

### 3.1 The agent operator: `Processor` over a `KeyValueStore`

The Flink agent operator is a `KeyedProcessFunction<String, AgentEvent, …>` with
`ValueState`. On Kafka Streams it is a `Processor<String, AgentEvent, …>` that
looks up its state store in `init()`. Note: the processor is **not** keyed by the
runtime the way Flink keys state — *you* must ensure the upstream is repartitioned
by `conversationId`, then the store is implicitly per-key because the store is
partition-local and you read/write `store.get(conversationId)`.

```java
public final class AgentProcessor implements Processor<String, AgentEvent, String, AgentEvent> {
  private final String storeName;
  private final TurnBrain brain;                  // pure core — unchanged
  private final ConversationStore conversations;  // pure core SPI — unchanged
  private ProcessorContext<String, AgentEvent> ctx;
  private KeyValueStore<String, ShortTermState> store;

  AgentProcessor(String storeName, TurnBrain brain, ConversationStore conversations) {
    this.storeName = storeName; this.brain = brain; this.conversations = conversations;
  }

  @Override public void init(ProcessorContext<String, AgentEvent> ctx) {
    this.ctx = ctx;
    this.store = ctx.getStateStore(storeName);    // partition-local, changelog-backed
  }

  @Override public void process(Record<String, AgentEvent> rec) {
    String conversationId = rec.key();            // upstream repartitioned by this key
    ShortTermState st = store.get(conversationId);
    if (st == null) st = ShortTermState.empty();

    // Pure ReAct turn — identical logic to the Flink operator. See §3.6 for the
    // async caveat: brain.step() must NOT block the StreamThread on an LLM call.
    TurnBrain.Outcome out = brain.step(rec.value(), st, conversations.history(conversationId));

    store.put(conversationId, out.nextState());   // durable working memory (C1)
    conversations.append(conversationId, out.message());
    ctx.forward(rec.withValue(out.event()));
  }
}
```

Store registration mirrors `addStateStore`:

```java
StoreBuilder<KeyValueStore<String, ShortTermState>> shortTerm =
    Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("short-term"),
        Serdes.String(), shortTermSerde);

Topology topology = new Topology()
    .addSource("ingest", Serdes.String().deserializer(), eventDeserializer, "agent.requests")
    .addProcessor("agent", () -> new AgentProcessor("short-term", brain, conversations), "ingest")
    .addStateStore(shortTerm, "agent")
    .addSink("emit", "agent.responses", Serdes.String().serializer(), eventSerializer, "agent");
```

### 3.2 `KeyedStateStore` SPI over a `KeyValueStore`

The §4b "Flink-bound" `ShortTermMemory` is replaced by the portable
`KeyedStateStore` (§4c). On Kafka Streams it is a one-class adapter:

```java
final class KafkaStreamsKeyedStateStore implements KeyedStateStore {
  private final KeyValueStore<String, byte[]> kv;   // from ctx.getStateStore(name)
  private final Codec codec;

  KafkaStreamsKeyedStateStore(KeyValueStore<String, byte[]> kv, Codec codec) {
    this.kv = kv; this.codec = codec;
  }
  public <V> Optional<V> get(String key, Class<V> t) {
    byte[] b = kv.get(key);
    return b == null ? Optional.empty() : Optional.of(codec.decode(b, t));
  }
  public <V> void put(String key, V value) { kv.put(key, codec.encode(value)); }
  public void clear(String key) { kv.delete(key); }
}
```

`FlinkStateShortTermMemory` becomes a `KeyedStateStore`-backed `ShortTermMemory` —
same interface to the `TurnBrain`, different substrate.

### 3.3 `ConversationStore` — keep the SPI as-is, pick a backend

`ConversationStore` (the cross-operator transcript + scalar attributes, keyed by
`conversationId`, user-indexed) is **already engine-agnostic** and `Serializable`.
Two honest options on Kafka Streams:

- **A Kafka Streams state store** (a `KeyValueStore<String, ConversationRecord>`
  with a compacted changelog). Cheap, no extra infra, queryable via Interactive
  Queries. *Caveat:* it is **partition-local** — a router node and a verifier node
  in the same instance see it only if they are co-partitioned on `conversationId`
  (they are, in the §4 graph). A *different* application reading the same
  conversation cannot reach it. This matches `InMemoryConversationStore`'s
  single-JVM contract, but durable.
- **The existing Redis / Fluss / Postgres `ConversationStore`** when you need
  cross-application sharing (e.g. an inbound A2A gateway in a separate process must
  read the transcript). This is exactly why those SPI backends exist (keystone §8:
  *durability of per-conversation state* is the central porting decision). The
  `RedisConversationStore` / `PostgresConversationStore` port with **no changes** —
  open the client in `Processor.init()`, mark the field `transient` if the
  processor is serialized.

```java
@Override public void init(ProcessorContext<String, AgentEvent> ctx) {
  this.ctx = ctx;
  this.store = ctx.getStateStore("short-term");
  // Shared/durable transcript across operators & apps — Redis/Fluss/Postgres SPI:
  this.conversations = ConversationStores.discover();   // unchanged core call
}
```

### 3.4 Tools — `ToolExecutor` unchanged

`ToolExecutor` is `Map<String,Object> → CompletableFuture<Object>` — pure, no
Flink. The `ToolRegistry` and every built-in tool port verbatim. The **only**
question is *where the `CompletableFuture` completes* relative to the
`StreamThread` — which is the C4 problem (§3.6), not a `ToolExecutor` problem.

### 3.5 The routed graph — `route()` as a `Topology`

`RoutedAgentGraph.wire(...)` (router → path(s) → verifier) maps directly. Router is
a `Processor` that tags the verdict; `split()` (or `branch`) fans to a child node
per path; all paths forward into one verifier node. Full sketch in §4.

### 3.6 Async I/O — the bridge (the heart of this port)

`A2AStep` on Flink offers three wirings: `applyToKeyed` (blocking in the keyed
operator), `applyToAsync` (Flink Async I/O, stateless), and the recommended
`applyToStateful` (keyed-pre → async → keyed-post, continuity through
`ConversationStore`). Kafka Streams has **no Async I/O operator**, so we rebuild
that split manually.

**Option (a) — async-completion via a response topic (recommended).** This is the
faithful Kafka-Streams analog of `applyToStateful`: the *keyed pre* node fires the
call and parks state; the reply re-enters keyed by the same `conversationId` at a
*keyed post* node. The `StreamThread` is **never blocked**.

```java
// PRE node: fire the LLM/A2A call off-thread, park a PENDING record, return.
@Override public void process(Record<String, AgentEvent> rec) {
  String cid = rec.key();
  store.put(cid, PendingTurn.of(rec.value(), ctx.currentSystemTimeMs()));   // park (C1)

  // Off the StreamThread. ResilientA2AClient (retry/backoff/breaker) is pure core.
  asyncExecutor.submit(() -> {
    A2ATask t = resilientClient.sendAndAwait(toMessage(rec.value()), cid);  // unchanged
    producer.send(new ProducerRecord<>("agent.a2a.replies", cid, toReply(t)));
  });
  // no forward — the turn resumes when the reply lands on agent.a2a.replies
}

// Wall-clock punctuator: expire pendings past their deadline (the C4 backstop).
ctx.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, now -> {
  try (var it = store.all()) {
    while (it.hasNext()) {
      var e = it.next();
      if (e.value.isPending() && now - e.value.startedAt() > deadlineMs) {
        ctx.forward(new Record<>(e.key, timeoutEvent(e.value), now));
        store.delete(e.key);
      }
    }
  }
});

// POST node (consumes agent.a2a.replies, repartitioned by cid): resume the turn.
@Override public void process(Record<String, A2AReply> rec) {
  PendingTurn p = store.get(rec.key());
  if (p == null || !p.isPending()) return;           // already timed out / duplicate — idempotent
  conversations.append(rec.key(), reply(rec.value()));
  store.put(rec.key(), p.completed());
  ctx.forward(new Record<>(rec.key(), resume(p, rec.value()), rec.timestamp()));
}
```

The reply topic must be repartitioned by `conversationId` so the POST node's store
lookup is partition-local and single-writer-correct — exactly the invariant
`A2AStep.applyToStateful` preserves via `keyBy(contextId)` on both ends.

**Option (b) — bounded blocking thread-pool in the processor.** A `Semaphore` +
`future.get(timeout)` inside `process()`. Simpler (no reply topic), but it **blocks
the `StreamThread`**: every conversation hashed to that partition stalls behind the
in-flight call, `commit.interval.ms` is delayed, and a long stall risks tripping
`max.poll.interval.ms` → rebalance. **Only acceptable** for fast tool calls or very
low fan-in. Mitigate by raising `num.stream.threads` and `max.poll.interval.ms`,
but prefer (a) for any real LLM/A2A latency. This is the direct analog of
`A2AStep.applyToKeyed` and carries the same "acceptable on the operator thread"
caveat the Javadoc names.

### 3.7 RAG — pure logic, processors per stage

`RetrievalPipeline` (embed → search → rerank → answer) and `TwoTierRetriever`
(hot+cold merge, dedupe by id) are pure. Each Flink `ProcessFunction` stage
(`EmbedQueryFn`, `SearchFn`/`HotColdSearchFn`, `RerankFn`, `AnswerFn`) becomes a
`Processor`; the embed/search/rerank calls are I/O and follow §3.6 (async-completion
preferred). `TwoTierRetriever`'s `ColdSearch` seam binds to a pgvector/Qdrant client
opened in `init()`; the hot tier is a `KeyValueStore`-backed index. The merge logic
is unchanged.

### 3.8 A2A & inbound edge

`ResilientA2AClient`, `RemoteAgentSpec`, `A2AClient`, task/message/artifact types —
all pure, ported verbatim. The A2A-as-tool path (`A2AToolExecutor`) is a
`ToolExecutor` (§3.4). The A2A-as-graph-step path (`A2AStep`) is the §3.6 pattern.
The **inbound edge** (the Quarkus gateway speaking JSON-RPC/SSE/REST) is unchanged
in either deployment — it bridges external callers onto Kafka request topics that
the Streams topology sources from, replacing the in-JVM `A2ABridge` with a topic
pair (`agent.requests` / `agent.responses`).

## 4. Worked example — banking router → path → verifier as a `Topology`

`BankingAgentGraph.wire(...)` builds: bridge request channel → rule-based router
(tags `BankingPath`) → `RoutedAgentGraph` fans to one keyed path brain per path →
merge into rule-based verifier (advances `BankingPhase`, emits `A2AResponse`). All
operators keyed by A2A `contextId`. Here it is as a Kafka Streams `Topology`:

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "banking-" + role);   // CS / PERSONAL
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);  // C3
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

ConversationStore conversations = ConversationStores.discover();     // Redis/Fluss/in-store
Function<BankingPath, TurnBrain> brains = setup::brainFor;           // pure core

StoreBuilder<KeyValueStore<String, ShortTermState>> stStore =
    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("st"),
        Serdes.String(), shortTermSerde);

Topology t = new Topology();

// Source: external A2A requests arrive on a topic (inbound gateway produces here).
// KEY = contextId, so every node downstream is single-writer-per-conversation (C2).
t.addSource("requests", new StringDeserializer(), a2aReqDeser, "banking.requests");

// Router (rule-based: screen + classify, NO LLM — never blocks). Tags BankingPath.
t.addProcessor("router", () -> new BankingRouterProcessor(role), "requests");

// One keyed path processor per BankingPath (REFUSE = pass-through brain=null).
// Router forwards to the child named after the path; unused paths are never hit.
List<String> pathNodes = new ArrayList<>();
for (BankingPath p : BankingPath.values()) {
  String node = "path-" + p.name();
  TurnBrain brain = brains.apply(p);            // ReActTurnBrain — the only LLM caller
  // Path brains do LLM tool loops → use the §3.6 async-completion split, NOT blocking.
  t.addProcessor(node, () -> new BankingPathProcessor(p, brain, conversations, "st"), "router");
  t.addStateStore(stStore, node);               // per-path short-term working memory
  pathNodes.add(node);
}

// Verifier (rule-based): merge all paths, advance BankingPhase via ConversationStore,
// emit A2AResponse. All path nodes are its parents (the fan-in).
t.addProcessor("verifier", () -> new BankingVerifierProcessor(conversations),
    pathNodes.toArray(new String[0]));

// Sink: response back to the inbound gateway's response topic.
t.addSink("responses", "banking.responses", new StringSerializer(), a2aRespSer, "verifier");

new KafkaStreams(t, props).start();             // ≈ env.execute(...)
```

Router forwarding (the `split()`/`branch` equivalent inside the Processor API):

```java
// BankingRouterProcessor.process(): rule-based classify, then forward to the path child.
BankingTurn turn = classify(rec.value(), role);                 // no LLM
ctx.forward(rec.withValue(turn), To.child("path-" + turn.pathName()));
```

Mapping to the Flink original, line for line:

| Flink (`BankingAgentGraph`) | Kafka Streams |
|---|---|
| `bridge.requestChannel().open(env)` | `addSource("requests", "banking.requests")` |
| `.keyBy(A2ARequest::key)` | source key = `contextId` (gateway partitions by it) |
| `BankingRouterFunction` (`KeyedProcessFunction`) | `BankingRouterProcessor` (`Processor`) |
| `RoutedAgentGraph.wire(... pathFns ...)` | one `addProcessor("path-X")` per `BankingPath` + `To.child(...)` |
| `BankingPathFunction` (ReAct brain, LLM) | `BankingPathProcessor` + §3.6 async-completion |
| `BankingVerifierFunction` | `BankingVerifierProcessor` (fan-in parents) |
| `PhaseStore` / `ConversationMemory` (cross-turn) | `ConversationStore` (`putAttribute("phase",…)`) + `st` store |
| `responses.sinkTo(bridge.responseSink())` | `addSink("responses", "banking.responses")` |
| `env.execute(...)` | `new KafkaStreams(t, props).start()` |

Cross-turn chaining (the multi-step flow) works identically: the verifier writes
`BankingPhase` via `conversations.putAttribute(contextId, "phase", phase.name())`
(the `ConversationStore` scalar-attribute API), and the next turn's router reads it
back — across operators *and* across turns, because the store is keyed by
`contextId`, not by operator.

## 5. What doesn't fit

- **Async ergonomics (C4).** The headline gap. There is no `AsyncDataStream`; you
  hand-build the response-topic split (§3.6) or accept partition stalls with a
  blocking pool. It works and is correct, but it is *more code and more topics*
  than Flink's one-liner `applyToStateful`. Budget for it.
- **External-effect exactly-once.** EOS is exactly-once for the *Kafka*
  read-process-write cycle only. The LLM call, the tool side effect, and the A2A
  `message/send` are **outside** the transaction — on retry/rebalance they can
  re-fire. The `ResilientA2AClient` already assumes idempotency (stable
  `messageId`, dedupe by id; see its Javadoc), and tool calls should be idempotent
  (keystone §8). The async-completion `PENDING` record (§3.6) doubles as a dedupe
  guard: a duplicate reply finds the turn already completed and no-ops.
- **CEP (C10).** No library; pattern detection is a hand-rolled `Processor` over a
  windowed store. Fine for the rare case, not a first-class module.
- **Non-Kafka connectors (C6).** Sources/sinks want to be topics. Redis / Fluss /
  ZMQ / webhook / Postgres are reached via processor-level clients opened in
  `init()` (transient fields), or fronted by Kafka Connect. The `Channel<T>` SPI's
  Flink-specific `open(env)` is replaced by a topic + `Consumed`/`Produced`; the
  *intent* (named typed source/sink) survives.
- **Rebalance & state-locality nuances.** A partition (and its RocksDB store) can
  migrate between instances on rebalance; state-store restore from the changelog
  takes time, during which the partition is unavailable. Use
  `num.standby.replicas >= 1` for hot failover, and
  `acceptable.recovery.lag` / static membership (`group.instance.id`) to limit
  churn. Flink's keyed state has analogous but differently-tuned restore behavior;
  the operational model is genuinely different and worth a runbook.
- **No MiniCluster-style embedded "session vs cluster" modes.** Kafka Streams *is*
  a library in your JVM (closer to Flink's in-proc mode); "cluster" means more
  instances of the same `application.id`, not a separate runtime. Mostly a
  simplification, but the runtime-mode selector in the notebooks has no direct
  equivalent.
- **End-to-end backpressure (C5).** Pull-based with bounded in-flight, not
  credit-based. Protect the model endpoint with an explicit `Semaphore` /
  rate-limiter; you cannot rely on the engine to propagate backpressure upstream
  the way Flink does.

## 6. When to choose Kafka Streams

Choose it when:

- You are **already a Kafka shop** and your event backbone is Kafka — then C1/C2/
  C3/C6 are free and the operational story is one you already run.
- You want the agent runtime to be a **library embedded in your own JVM service**
  (a microservice, a Quarkus/Spring app) rather than a separate Flink cluster — no
  JobManager/TaskManager to operate.
- You value **transactional exactly-once** over the read-process-write cycle and
  can make external effects idempotent.
- You want to **reuse the existing Java core verbatim** — this is the only JVM
  target where `ConversationStore`, `ToolExecutor`, `TwoTierRetriever`, the RAG
  step logic, and `ResilientA2AClient` move across with zero edits.

Prefer **Flink** instead when async-I/O ergonomics, native CEP, rich event-time
windowing, a broad non-Kafka connector ecosystem, or credit-based backpressure are
central — i.e. when C4/C9/C10/C6-breadth carry the design. Prefer **Faust** (§7
rank 1) if you want the same streaming essence in *pure Python* with native asyncio.
Prefer **Ray** if actor-per-conversation in-memory state and trivial async matter
more than Kafka-native EOS.

> Bottom line: Kafka Streams is Flink's nearest twin for this project — native
> C1/C2/C3 and a verbatim core reuse — with one tax to pay (async I/O via a
> response-topic split). If you're on Kafka, it's the most natural JVM home.
