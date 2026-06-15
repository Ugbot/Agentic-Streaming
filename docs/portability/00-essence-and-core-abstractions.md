# The Essence of Agentic Streaming — and How to Port It Off Flink

> Keystone design note for the `docs/portability/` series. Read this first; every
> per-engine doc (`kafka-streams.md`, `pekko.md`, `temporal.md`, `pulsar.md`,
> `nats.md`, `spring.md`, `quarkus.md`, `ray.md`, `celery.md`, `dask.md`,
> `airflow.md`, `faust.md`) is written against the abstractions, capability matrix, and
> Engine SPI defined here.

## 1. Why this exists

Agentic Streaming is built *on* Apache Flink, but very little of what makes it
**useful** is intrinsically Flink. Flink supplies a set of runtime *capabilities*
(durable keyed state, partitioned ordered processing, checkpointing, async I/O,
backpressure, a connector model). The agentic value — agents as stateful
processors with per-conversation memory, tool use, routing, peer delegation, and
retrieval — sits *above* those capabilities.

This series asks: **if you deleted Flink, what would the same project look like on
Kafka Streams, Apache Pekko, Temporal, Apache Pulsar Functions, NATS JetStream, Spring,
Quarkus, Ray, Celery, Dask, Airflow, or Faust?** The goal is not
"reimplement Flink everywhere" — it's to (a) name the engine-agnostic core
precisely, (b) name exactly what Flink was providing, and (c) honestly map each
target engine: what fits cleanly, what fits awkwardly, and what simply doesn't
belong on that engine.

For the Python engines (Ray, Dask, Airflow, Faust) the target is **pure Python** —
no JVM, no JPype bridge. The Java engines (Kafka Streams, Spring, Quarkus) stay on
the JVM and can reuse the existing core types.

## 2. The essence — what this project fundamentally *is*

Strip away the framework and you have **five** load-bearing ideas:

1. **An agent is a keyed, stateful, event-driven processor.** One logical agent
   instance per conversation key (`conversationId` / `userId`), processing its
   events in order, holding state that survives across turns. A turn is: receive
   input → (optionally) loop ReAct {think → call tool → observe} → emit a result.

2. **Memory is tiered and pluggable.**
   - *Short-term* working memory, per key, for the current flow.
   - *Per-conversation* memory shared **across operators and turns** — the
     multi-turn transcript + scalar workflow attributes (routing phase, remote
     contextId), keyed by `conversationId`, indexable by `userId`.
   - *Vector* memory for semantic recall (hot recent window + cold durable corpus).
   - *Long-term* archival/resumption store.
   Each tier is an SPI with swappable backends (in-JVM, **Redis**, **Fluss**,
   Postgres, pgvector/Qdrant).

3. **Work is composed as a routed dataflow.** The canonical shape is
   `router → path(s) → verifier`: classify the request, dispatch to a specialized
   sub-agent, validate the result. Plus side flows (debug, tool side-outputs,
   control-plane broadcast). It's a small DAG of typed steps over a stream of events.

4. **Agents call tools and call *each other*.** Tools are async
   `params → result` functions. Agent-to-agent (A2A) lets one agent invoke a remote
   peer as a tool or as an explicit graph step, over a hardened client
   (retry/backoff/circuit-breaker) — blocking-in-a-keyed-operator, non-blocking
   async, or a keyed→async→keyed split that keeps per-conversation state correct.

5. **There's an inbound edge and an outbound edge.** Inbound: a proxy (today
   Quarkus) speaks a protocol (A2A JSON-RPC / SSE / REST) and bridges requests to
   the runtime. Outbound: connectors move events in/out (Kafka, Redis, Fluss, ZMQ,
   webhooks). Both are pluggable transports.

The **one-sentence essence**: *a fleet of per-conversation stateful agents that
remember, route, use tools, delegate to peers, and retrieve — driven by events,
backed by pluggable durable state.*

## 3. What Flink actually provides (the capability inventory)

These are the runtime capabilities the project leans on. Every port must answer
"how do I get this here, or do I give it up?"

| # | Capability | Where it's used | How load-bearing |
|---|------------|-----------------|------------------|
| C1 | **Durable keyed state** (ValueState/MapState/ListState per key) | short-term memory, `RoutingBudget`, A2A contextId, dedup | **Core.** The substrate for per-conversation agent state. |
| C2 | **Partitioned, per-key ordered processing** (`keyBy` → one logical processor per key) | every agent operator; one conversation handled sequentially | **Core.** Gives single-writer-per-conversation without locks. |
| C3 | **Fault tolerance / checkpointing / (≈)exactly-once** | state survives crashes; replay | High for production; demos tolerate weaker. |
| C4 | **Async I/O operator** (non-blocking external calls, bounded in-flight) | LLM + A2A calls without stalling the operator thread | High — agents are I/O-bound on model calls. |
| C5 | **Backpressure** | flow control end-to-end | Medium-high (protects model/rate limits). |
| C6 | **Connector model** (FLIP-27 sources / FLIP-143 sinks) | `Channel<T>`: Kafka/Redis/Fluss/ZMQ/webhook/Postgres | Core for I/O; per-engine equivalents exist. |
| C7 | **Side outputs** | debug stream, tool-invocation channel | Low-medium (nice, replaceable). |
| C8 | **Broadcast state** | control-plane directives, enrichment dims | Low-medium. |
| C9 | **Event-time, watermarks, windows** | feature aggregation, CEP timing | Medium (only the streaming-analytics flows). |
| C10 | **CEP (pattern detection)** | event-driven patterns | Low (one optional module). |
| C11 | **Distributed execution / elastic parallelism** | scale-out across a cluster | High for scale; low for single-node/embedded. |
| C12 | **The DAG/topology builder** | how steps wire together | Core *as a model*; each engine has its own. |

The crucial observation: **C1 + C2 are the heart.** "A durable thing per key,
processed in order" is what makes an agent operator an agent operator. An engine
that gives you C1+C2 cleanly (Kafka Streams state stores, Faust Tables, a Ray
actor-per-conversation) can host the essence faithfully. An engine that doesn't
(Airflow, Dask) can still host *parts* (the routing topology, batch RAG) but not
the live keyed-stateful streaming core.

## 4. The engine-agnostic core (what to factor out)

Most of the project is **already** engine-agnostic or one thin seam away. The
portable core is a set of SPIs + plain logic that an "engine adapter" plugs into.

### 4a. Already portable (pure logic / backend SPIs — no Flink in them)
- **`ConversationStore`** — multi-turn transcript + scalar attributes, keyed by
  `conversationId`, user index. Backends: in-JVM shared, Redis, Fluss PK table,
  Postgres. *This is the model for how all per-key durable state should look in a
  portable core: an interface with `get/put/append/history/attributes` keyed by a
  string, not Flink keyed state.*
- **`VectorStore` / `HotVectorIndex` / `TwoTierRetriever`** — embeddings,
  hot+cold retrieval, cosine KNN. Backends pluggable (in-mem, Redis, pgvector, …).
- **`ToolExecutor` / `ToolRegistry`** — `Map<String,Object> → CompletableFuture<Object>`.
- **`Embedding` / `Inference` / `ChatConnection` SPIs** — model access.
- **Ingestion / chunking / retrieval pipeline *logic*** (the steps, minus wiring).
- **A2A protocol types + resilient client** (`RemoteAgentSpec`, `A2AClient`,
  `CircuitBreaker`, task/message/artifact value types).
- **The ReAct loop / TurnBrain** — the think→tool→observe control logic.

### 4b. Flink-bound today (needs an engine seam)
- **`ShortTermMemory` (FlinkStateShortTermMemory)** — built on keyed `ValueState`.
  Portable replacement: a `KeyedStateStore` SPI (`get/put/update/clear` for a key)
  that an engine implements over its own state (Kafka Streams `KeyValueStore`,
  Faust `Table`, a Ray actor's fields, Redis).
- **The agent *operator*** (`KeyedProcessFunction`/`RichAsyncFunction` subclasses)
  — the binding of "agent logic" to "a keyed stateful operator with async I/O."
- **`Channel<T>` open/`fromSource`/`sinkTo`** — the Flink wiring; the *intent*
  (a named typed source/sink) is portable.
- **`A2AStep.applyTo/applyToAsync/applyToStateful`** — graph wiring of a remote
  step; the *pattern* is portable, the DAG calls are engine-specific.
- **Runtime modes** (in-proc / session cluster / embedded MiniCluster).
- **`@TypeInfo`/serialization** — Flink-specific; other engines have their own
  (Kafka Serde, cloudpickle, Jackson).

### 4c. The proposed **Engine SPI** (what a port implements)

A faithful port implements roughly this surface (names illustrative; Java shown,
Python is the analogous protocol):

```text
EngineRuntime
  Stream<T>      source(ChannelSpec<T>)              // inbound events
  void           sink(Stream<T>, ChannelSpec<T>)     // outbound
  KeyedAgent<IN,OUT> keyedAgent(KeySelector<IN>, AgentLogic<IN,OUT>)
                                                     // C1+C2: per-key state + ordered processing
  AsyncStage<IN,OUT> asyncStage(AsyncFn<IN,OUT>, capacity, timeout)
                                                     // C4: non-blocking external calls
  Stream<OUT>    route(Stream<IN>, Router, Map<Path, Stage>)  // the routed-graph pattern
  void           execute(name)

KeyedStateStore                                       // the portable C1
  Optional<V> get(key, name);  void put(key, name, V);  void clear(key)

AgentLogic<IN,OUT>                                    // pure: gets state handle + emit
  void onEvent(IN event, StateAccess state, Emitter<OUT> out)
```

Everything in §4a is consumed *by* `AgentLogic` unchanged. The per-engine docs
describe how that engine realizes `EngineRuntime` + `KeyedStateStore` +
`AsyncStage`, and which capabilities (§3) come native vs. need a library vs. an
external service vs. are dropped.

## 5. The portability boundary (pure vs. engine-bound)

```text
        ┌─────────────────────────────────────────────────────────┐
        │                 PURE / PORTABLE CORE                      │
        │  ReAct loop · ToolRegistry · ConversationStore (SPI) ·    │
        │  Vector hot+cold · embeddings/inference SPIs · A2A        │
        │  protocol + resilient client · RAG pipeline logic         │
        └──────────────▲───────────────────────────▲───────────────┘
                       │ uses                       │ uses
        ┌──────────────┴───────────────────────────┴───────────────┐
        │                    ENGINE SPI (§4c)                       │
        │  source/sink · keyedAgent (C1+C2) · asyncStage (C4) ·     │
        │  route · KeyedStateStore · execute                        │
        └──────────────▲────────────────────────────────────────────┘
                       │ implemented by
   ┌───────────────────┼───────────────────────────────────────────┐
   │ Flink   Kafka-Streams   Faust   Ray   Quarkus   Spring   Dask   Airflow
```

A clean port keeps the top box byte-identical (Java) or logic-identical (Python)
and re-implements only the middle box per engine.

## 6. Capability requirements matrix

How each engine supplies the §3 capabilities. Legend: **N**ative ·
**L**ibrary/idiom · **X**ternal service needed · **—** not a fit / drop.

| Cap | Flink | Kafka Streams | Faust | Ray | Quarkus | Spring | Dask | Airflow |
|-----|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| C1 keyed durable state | N | N (state stores) | N (Tables) | N* (actor fields) +X for durability | X (Redis/DB) | X (Redis/JPA) | L* (Actors, weak) | X (XCom/DB, tiny) |
| C2 per-key ordered proc | N | N (partitions) | N (partitions) | N (actor mailbox) | L (partitioned consumer) | L (partitioned consumer) | — | — (per-run) |
| C3 fault tolerance / EOS | N | N (txn EOS) | L (Kafka offsets) | X (checkpoint to store) | X (broker + store) | X (broker + store) | L (task retry) | N (task retry/idempotency) |
| C4 async I/O | N | L (async + punctuate) | N (asyncio) | N (tasks/async actors) | N (Mutiny/virtual threads) | L (Reactor/@Async) | L (futures) | L (deferrable operators) |
| C5 backpressure | N | L (consumer pause) | L (asyncio + maxsize) | L (backpressure refs) | N (reactive) | L (Reactor) | L (scheduler) | — (batch) |
| C6 connectors | N | N (Kafka-centric) | N (Kafka) | L (Ray Data/Serve) | N (SmallRye Reactive Msg) | N (Spring Cloud Stream) | L (read_*) | L (hooks/operators) |
| C7 side outputs | N | L (branch/extra topics) | L (extra topics) | L (multiple returns) | L (multiple channels) | L (multiple channels) | L | L (multiple tasks) |
| C8 broadcast state | N | N (GlobalKTable) | L (global Table) | L (actor broadcast) | L | L | L | L (Variables) |
| C9 event-time/windows | N | N | N (windowed Tables) | — | L | L | — | — |
| C10 CEP | N (lib) | L (custom) | L (custom) | L | L | L | — | — |
| C11 distributed scale | N | N | N | N | N (k8s) | N (k8s) | N | N (workers) |
| C12 topology builder | N | N (Topology) | N (agents) | N (actor/task graph) | L (msg flows) | L (integration flows) | N (task graph) | N (DAG) |

`*` = present but with a caveat spelled out in that engine's doc.

> **Apache Pekko** (added after the table; see [`pekko.md`](pekko.md)) is the JVM actor
> toolkit. Its column would read **N** for C1 (actor fields + Cluster Sharding), **N**
> for C2 (one entity per key, sequential mailbox), **N** for C3 (Persistence /
> event sourcing — durable natively, unlike Ray), **N** for C4 (async actors / `ask`),
> **N** for C5 (Pekko Streams), **L** for C6 (Pekko Connectors), **N** for C11
> (cluster sharding rebalances entities), **N** for C12 (actor/stream graph). With
> **Pulsar Functions** and **Temporal** (below), it is one of only three engines besides
> Flink here that supply **C1+C2+C3 all natively** — the actor-model peer of Kafka
> Streams' streaming-model fit.

> **Temporal** (added after the table; see [`temporal.md`](temporal.md)) is the durable
> execution engine. Its column would read **N** for C1 (workflow state, persisted via
> event-sourced history, keyed by workflowId = conversationId), **N** for C2 (exactly
> one running execution per id; Updates/Signals applied serially), **N** for C3 (the
> strongest in the series — event-sourced replay + activity retries + timers), **N** for
> C4 (activities run off the workflow thread), **N** for C11 (workers scale, service
> shards by id), **N** for C12 (workflow code *is* the orchestration graph). It is
> request/response durable orchestration, not a low-latency stream — but it is one of
> the three besides Flink to give C1+C2+C3 natively, with the strongest durability.

> **NATS JetStream** (added after the table; see [`nats.md`](nats.md)) is persistent
> streaming + a durable KV store on NATS. Its column would read **N** for C1 (the
> **KV store** — a revisioned envelope per conversationId), **L** for C2 (no native
> per-key consumer assignment; subject routing + KV revision compare-and-set), **N** for
> C3 (JetStream persistence + redelivery; the KV envelope makes a redelivered turn
> idempotent), **N** for C4 (asyncio client), **N** for C5 (flow control), **N** for C7
> (publish to a subject), **N** for C11 (clustered). A lightweight, online home with
> native durable keyed state from one small server; C2 is a convention rather than an
> engine guarantee.

> **Apache Pulsar Functions** (added after the table; see [`pulsar.md`](pulsar.md)) is
> Pulsar's lightweight serverless compute. Its column would read **N** for C1 (the
> built-in **state store**, BookKeeper-backed and durable), **N** for C2 (a
> `Key_Shared` subscription keyed by `conversationId` delivers one key to one instance
> in order), **N** for C3 (effectively-once processing + acks), **L** for C4 (publish
> the reply to a response topic to go non-blocking), **N** for C6 (Pulsar IO
> connectors), **N** for C11 (instances scale per partition), **N** for C12 (chained
> functions / topics). With Pekko and Temporal it is one of only three engines besides
> Flink to give C1+C2+C3 natively — and the closest of them to Flink's topic-in/topic-out
> shape.

> **Celery** (added after the table; see [`celery.md`](celery.md)) is the Python
> distributed task queue. Its column would read **X** for C1 (state in the result
> backend / an external ConversationStore — Redis), **L** for C2 (no native keyed
> ordering; route a conversation to one queue + a per-conversation lock), **L/N** for
> C3 (`acks_late` + retries + the durable store make a redelivered turn idempotent),
> **L** for C4 (chord/chain or an async task body), **L** for C6 (broker transports).
> It hosts the *online* request/response turn (unlike Dask/Airflow's batch), but
> assembles the keyed-state heart from routing + an external store rather than natively.

## 7. Per-engine fit, at a glance (ranked)

1. **Faust** *(Python, streaming)* — the most natural Python target. Faust's own
   primitive is literally called an **agent** (a keyed Kafka stream processor) and
   **Tables** are keyed durable state (C1+C2 native), with `asyncio` for C4. The
   project's "agent + ConversationStore" maps ≈1:1. Pure Python. Best fit for the
   live keyed-stateful essence.
2. **Kafka Streams** *(JVM, streaming)* — the closest overall analog. State stores
   = keyed state (C1), partitions = keyed ordering (C2), transactional EOS (C3),
   Processor API ≈ `KeyedProcessFunction`. Weak spot: no native async-I/O operator
   (bridge with an async executor + punctuators). Reuses the Java core directly.
3. **Apache Pekko** *(JVM, actors)* — the actor-model peer of Kafka Streams, and one of
   only three engines here besides Flink to give the C1+C2+C3 heart **all natively**:
   one actor per conversation via **Cluster Sharding** (keyed state + single-writer),
   with **Persistence**/event-sourcing for durability, `ask` for async, Pekko Streams
   for backpressure. You keep the whole Java core; only the actor seam is Pekko. Not a
   Kafka-topology streaming engine (you wire ingress yourself) and no event-time engine.
4. **Temporal** *(JVM/polyglot, durable execution)* — **one entity workflow per
   conversation** (`workflowId == conversationId`): exactly one running execution
   (single-writer — C2), event-sourced history for durable state + the strongest fault
   tolerance in the series (C1+C3), Updates deliver turns, activities run the
   non-deterministic LLM/tool I/O (C4). The deterministic banking graph runs in-workflow.
   Trade-off: request/response durable orchestration, higher per-turn latency than a
   stream, not an event-time engine. Best fit for long-running, retried, human-in-the-loop
   agentic workflows.
5. **Apache Pulsar Functions** *(JVM, serverless streaming)* — Pulsar's lightweight
   compute, and the closest of the native-C1+C2+C3 engines to Flink's topic-in/topic-out
   shape. The built-in **state store** (BookKeeper) is durable keyed state (C1+C3); a
   `Key_Shared` subscription keyed by `conversationId` gives single-writer ordering
   (C2); chained functions are the topology (C12). Reuses the whole Java core; the
   function body just calls `Banking.buildGraph().handle(...)` over a state-backed
   ConversationStore. Go non-blocking (C4) via a response topic, as on Kafka Streams.
6. **Ray** *(Python)* — **actor-per-conversation** is a beautiful fit for stateful
   agents: each actor is a single-writer, sequential, in-memory state holder (C1+C2
   in memory), Ray tasks/async actors give C4 trivially, Ray Serve is the inbound
   edge, Ray Data does batch RAG. Caveat: durability (C3) is external (checkpoint
   actor state to Redis/Fluss); continuous streaming is not Ray's home turf. (Pekko is
   the JVM analogue with durability + sharding built in.)
7. **NATS JetStream** *(Python/polyglot, streaming + KV)* — a lightweight, online home
   with **native durable keyed state**: the JetStream **KV store** is a revisioned
   envelope per conversation (C1), a persistent stream + consumer is the ordered,
   redelivering transport (C3), asyncio-native (C4). C2 is a convention (subject routing
   + KV compare-and-set) rather than a partition guarantee. Kafka-like durability from
   one small binary; great at the edge or when you already run NATS.
8. **Quarkus** *(JVM, reactive)* — already our inbound proxy. SmallRye Reactive
   Messaging (Kafka) + Mutiny/virtual threads (C4 native, C6 native). Keyed state
   is external (Redis/Fluss via the existing SPIs). Excellent for the proxy + the
   per-message agent and A2A gateway; the keyed-streaming core rides on Kafka + the
   state SPIs rather than an engine primitive.
9. **Spring** *(JVM)* — Spring Cloud Stream (Kafka binder) + Spring StateMachine +
   Spring AI + Spring Integration. Strong on the *agent/tool/memory/FSM*
   abstractions and enterprise wiring; the dataflow becomes message channels /
   integration flows rather than a streaming topology. Keyed state external.
10. **Celery** *(Python, task queue)* — hosts the *online* request/response turn (one
   turn = one task), unlike Dask/Airflow's batch. No native keyed ordering, so C2 is
   recovered by routing a conversation to one queue + a per-conversation lock; keyed
   state (C1) is an external ConversationStore (Redis), and `acks_late` + retries +
   that durable store give idempotent C3. Good for scheduled/fan-out agentic work on an
   existing Celery/Redis stack; reuses the pure-Python core unchanged.
11. **Dask** *(Python)* — shines for **batch/parallel RAG** (embed + index a corpus
   across a cluster) and offline eval/benchmark sweeps. Long-lived keyed streaming
   agents are awkward (Dask is a task-graph/futures engine; Actors exist but aren't
   the sweet spot). Port the ingestion + retrieval-eval parts; don't force the live
   loop.
12. **Airflow** *(Python, orchestration)* — not streaming at all, but the **routed
   graph maps cleanly to a DAG** (router = `BranchPythonOperator`, paths = tasks,
   verifier = downstream task) and it's ideal for **scheduled/batch agentic
   workflows, RAG ingestion DAGs, and eval runs**. Per-event keyed state and
   low-latency conversation are out of scope; state lives in an external store, runs
   are per-trigger.

## 8. Cross-cutting concerns (call these out in every engine doc)

- **Durability of per-conversation state.** Flink checkpoints it for you. Most
  ports push it to the `ConversationStore`/`KeyedStateStore` SPI backed by **Redis
  or Fluss** — which the project already supports. This is the single most
  important porting decision and the reason those SPIs exist.
- **Single-writer-per-conversation.** Flink gives it via keyBy. Ports get it via
  Kafka partitioning (Kafka Streams/Faust/Quarkus/Spring), a `Key_Shared` subscription
  (Pulsar Functions), one running execution per workflowId (Temporal), an actor/entity
  per key (Ray/Pekko), subject routing + KV compare-and-set (NATS JetStream), a routed
  queue + per-key lock (Celery), or optimistic concurrency / locks (Airflow/Dask).
- **Async, I/O-bound model calls.** Agents spend their time waiting on LLMs. Native
  asyncio (Faust/Ray) or reactive (Quarkus) win; thread-pool bridges work elsewhere.
- **Exactly-once vs. at-least-once + idempotency.** Tool calls and A2A sends should
  be idempotent (the resilient client already assumes this); engines without EOS
  lean on idempotency + the `ConversationStore` as the source of truth.
- **Backpressure & rate limits.** Protect the model endpoint; map to the engine's
  flow control or an explicit semaphore.
- **Observability.** Per-conversation tracing, the debug side-output, tool-call
  timing — map to the engine's metrics/logging.

## 9. How to read the per-engine docs

Each `<engine>.md` follows the same template so they're comparable:

1. **One-paragraph verdict** — fit, and what you keep vs. drop.
2. **Capability mapping** — the §3 row-by-row for this engine (native/library/
   external/drop), with the chosen mechanism.
3. **The core abstractions on this engine** — how Agent, keyed state,
   ConversationStore, tools, async, routed graph, A2A, RAG, inbound proxy each land.
   Code-level sketches in the engine's idiom.
4. **A worked example** — the banking router→path→verifier (or a faithful subset)
   sketched end-to-end.
5. **What doesn't fit** — honest gaps, and the workaround or "don't do this here."
6. **When to choose this engine.**

> Design intent, not a migration plan. These docs define the abstractions and the
> honest fit; an actual port would start by extracting the §4c Engine SPI in the
> Java core (and a `pyagentic` pure-Python mirror) and writing one adapter.
