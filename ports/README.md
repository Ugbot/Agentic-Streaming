# `ports/` — Agentic-Flink on ten engines, compared

Working implementations of the [`docs/portability/`](../docs/portability/) designs:
the Agentic-Flink **essence** (per-conversation stateful agents that remember, route,
use tools, and retrieve) built on engines *other than Flink*. Every port runs the
same `router → path → verifier` **banking** worked example, so the comparison is
apples-to-apples.

Two shared, **Flink-free** essence cores carry the agent logic; each engine port is
a thin **runtime seam** on top:

```
ports/
  pyagentic/      pure-Python essence + LocalRuntime     ← Faust/Ray/Celery/Dask/Airflow build on this
  jagentic-core/  pure-Java essence + LocalRuntime       ← Kafka Streams/Pekko/Pulsar/Spring/Quarkus build on this
  faust/ ray/ celery/ dask/ airflow/             (Python adapters)
  kafka-streams/ pekko/ pulsar/ spring/ quarkus/ (JVM adapters)
```

The cores implement the engine-agnostic abstractions once — `ConversationStore`,
`KeyedStateStore`, `ToolRegistry`, `AgentContext`, `RoutedGraph`, hot+cold
`Retrieval`, `Banking` example. An adapter only supplies *how this engine gives a
durable thing per conversation, processed in order, with async I/O*.

---

## The ten at a glance

| Engine | Lang | Streaming? | The one-line fit | Verified here |
|--------|:----:|:----------:|------------------|---------------|
| **Faust** | Python | ✅ yes | `@app.agent` ≈ our agent; `Table` ≈ ConversationStore; native asyncio. Thinnest Python port. | imports clean, engine-guarded¹ |
| **Kafka Streams** | Java | ✅ yes | Closest analog — state stores + partitions + EOS; reuses the Java core; bridge async I/O. | `mvn compile` ✅ |
| **Apache Pekko** | Java | ◑ actors | Actor-per-conversation via Cluster Sharding (C1+C2) **+ Persistence (C3)** — all native. | **runs on real Pekko** ✅ |
| **Pulsar Functions** | Java | ✅ yes | State store (C1+C3) + `Key_Shared` (C2) — native, in Flink's topic-in/topic-out shape. | **runs + tested** ✅ |
| **Ray** | Python | ◑ rpc/actors | Actor-per-conversation = single-writer keyed state in memory; durability write-through. | imports clean, engine-guarded¹ |
| **Quarkus** | Java | ◐ reactive | SmallRye Reactive Messaging + Mutiny; keyed state external (Redis/Fluss). Already our proxy. | `mvn compile` ✅ |
| **Spring** | Java | ◐ messaging | Spring Integration EIP maps router→path→verifier ≈1:1; StateMachine for phase. | `mvn compile` ✅ |
| **Celery** | Python | ◐ task queue | One turn = one task; online request/response; C2 via routed queue + lock, state external. | **runs on real Celery** ✅ |
| **Dask** | Python | ✗ batch | Batch data plane — parallel RAG ingest + offline eval; not the live loop. | **runs on real Dask** ✅ |
| **Airflow** | Python | ✗ orchestration | Routed graph → branching DAG; scheduled agentic + RAG ingestion + HITL. | `simulate()` runs ✅ |

¹ Faust/Ray *run* with their engine + a broker/cluster. They can't run on this box (Python 3.14, ahead of faust/ray wheels), so they're import-checked + engine-guarded; their agent logic is the tested `pyagentic` core.

**Core tests:** `pyagentic` 9/9 · `jagentic-core` 6/6 · Python adapters 5/5 · Pulsar adapter 2/2 · Celery + Dask + Pekko + Pulsar run on the real engine · all JVM modules compile.

---

## Capability comparison (how each supplies what Flink gave for free)

From the keystone's capability inventory (C1–C12). Legend: **N**ative · **L**ibrary/idiom · **X**ternal service · **—** drop / not a fit.

| Capability | Faust | Kafka Streams | Pekko | Pulsar Fn | Ray | Quarkus | Spring | Celery | Dask | Airflow |
|------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **C1** durable keyed state | N `Table` | N state store | N shard+persist | N state store | N* actor +X | X Redis/Fluss | X Redis/JPA | X store | L* Actor | X store, tiny XCom |
| **C2** per-key ordering | N partition | N partition | N actor mailbox | N Key_Shared | N actor mailbox | L partition | L partition | L queue+lock | — | — |
| **C3** fault tolerance / EOS | L offsets | N txn EOS | N persistence | N eff-once | X checkpoint | X broker+store | X broker+store | L acks+retry | L retry | N retry/idempotent |
| **C4** async I/O | N asyncio | L async-bridge | N ask/pipeToSelf | L resp-topic | N async actor | N Mutiny/vthreads | L Reactor/@Async | L chord/chain | L futures | L deferrable |
| **C5** backpressure | L | L pause | N Pekko Streams | L flow-ctl | L | N reactive | L | L prefetch | L | — |
| **C6** connectors | N Kafka | N Kafka | L Connectors | N Pulsar IO | L Serve/Data | N SmallRye | N Cloud Stream | L brokers | L read_* | L hooks |
| **C9** event-time/windows | N | N | L streams | L windowed | — | L | L | — | — | — |
| **C12** topology builder | N agents | N Topology | N actor graph | N fn chain | N actor/task | L msg-flows | L EIP flows | L canvas | N task graph | N DAG |

`*` = present with a caveat (see that port's design doc). The pattern is stark: the
**heart is C1 + C2** ("a durable thing per key, processed in order"). Engines that give
both natively — **Faust, Kafka Streams, Pekko, Pulsar Functions**, and **Ray** (in
memory) — host the *live* essence faithfully. **Pekko and Pulsar Functions are the only
ones besides Flink to also give C3 (durability) natively** — Pekko via Cluster Sharding
+ Persistence, Pulsar via its BookKeeper state store + effectively-once — with no
external store. Quarkus/Spring/Celery assemble C1+C2 from Kafka partitions or a routed
queue + an external store. Celery still hosts the *online* turn (one turn = one task);
Dask/Airflow don't have C2 at all, so they host *parts* (the data plane / the workflow
topology), not the live conversational loop.

---

## How the worked example lands on each

Same `Banking.buildGraph()` (router → cards/payments/general → verifier) everywhere;
only the wiring differs:

- **Faust** — `@app.agent` consumes `agentic.requests.group_by(conversation_id)`,
  runs the graph against a `FaustTableConversationStore`, emits to `agentic.replies`.
- **Kafka Streams** — a `Topology`: source topic → `BankingAgentProcessor` (builds an
  `AgentContext` over a `KeyValueStore`-backed `KeyedStateStore` + `ConversationStore`,
  runs the graph) → sink topic; state store wired via `StoreBuilder`.
- **Pekko** — one typed `ConversationActor` per `conversationId` (Cluster Sharding);
  the actor's mailbox gives single-writer ordering, its fields are the keyed state, it
  runs the graph and replies via `ask`. `LocalDemo` runs it single-node, no broker.
- **Pulsar Functions** — `BankingFunction.process` builds an `AgentContext` over a
  `PulsarStateConversationStore` (the BookKeeper-backed state store) and runs the graph;
  the `conversationId` is the message key (`Key_Shared` = single-writer). `LocalDemo`
  runs it against an in-memory `Context`, and the persisted transcript survives turns.
- **Ray** — `RayRuntime.submit` routes each event to the get-or-create
  `ConversationAgent` actor named `conv:<cid>`; the actor *is* the keyed state, runs
  the graph, with a write-through point to a durable store.
- **Quarkus** — `@Incoming("requests")`/`@Outgoing("replies")` agent keyed by
  `conversation_id` + a Mutiny `Uni` REST `AgentResource`; state via the
  `ConversationStore` CDI bean.
- **Spring** — `POST /agent` controller + a Spring Integration flow
  (`.route(Banking::router)` → `cards`/`payments`/`general` channels → `verify`) +
  a Spring StateMachine for the phase FSM.
- **Celery** — `process_turn` is a task routed to `conversation_queue(cid)` (single
  worker = single-writer) + a per-conversation lock; `CeleryRuntime(eager=True)` runs
  it in-process; state in a shared (Redis in prod) `ConversationStore`.
- **Dask** — not the live graph: a batch pipeline that ingests the KB in parallel,
  scores `recall@1`, and *replays* the routed graph over many transcripts.
- **Airflow** — a `routed_triage` DAG: `@task.branch` (router) → `path_*` tasks →
  `one_success` `verify`; plus an `agentic_ingestion` DAG for the cold index.

---

## Run it

```bash
# pure-Python core (no deps)
cd ports/pyagentic && PYTHONPATH=. python -m pytest tests/ -q          # 6 pass

# Python adapters' portable logic (Dask + Celery use the real engine if installed)
cd ports && PYTHONPATH=pyagentic python -m pytest tests/ -q           # 5 pass
python ports/celery/agentic_celery.py         # live banking turns, eager mode (no broker)
python ports/dask/agentic_dask.py             # batch RAG + recall@1 + replay
python ports/airflow/agentic_banking_dag.py   # routing simulate (no scheduler)
# faust:  faust -A agentic_faust:app worker -l info     (needs Kafka + faust-streaming)
# ray:    python ports/ray/agentic_ray.py               (needs ray[default])

# pure-Java core + JVM engine modules
mvn -f ports/jagentic-core/pom.xml test       # 4 pass
mvn -f ports/kafka-streams/pom.xml compile
mvn -f ports/spring/pom.xml compile
mvn -f ports/quarkus/pom.xml compile
mvn -f ports/pekko/pom.xml compile            # BUILD SUCCESS
mvn -f ports/pekko/pom.xml -q exec:java       # runs the banking demo on real Pekko actors
mvn -f ports/pulsar/pom.xml test              # 2 pass (banking + extended-graph through the seam)
mvn -f ports/pulsar/pom.xml -q exec:java      # runs the banking Pulsar Function (in-memory Context)
```

---

## Choosing an engine

- **Want the live, low-latency, stateful conversational loop?** → **Faust** (pure
  Python) or **Kafka Streams** (JVM). They give keyed durable state + per-key
  ordering natively; the agent maps almost 1:1.
- **Actor-shaped agents on the JVM, with native durability + clustering?** → **Pekko**
  — one supervised, event-sourced entity per conversation via Cluster Sharding.
- **Already on Pulsar, want native durable state without Flink?** → **Pulsar
  Functions** — the closest non-Flink engine to the topic-in/topic-out streaming shape;
  the state store gives C1+C3 and `Key_Shared` gives C2, all native, ops-light. With
  Pekko it's one of only two engines here besides Flink that give C1+C2+C3 natively.
- **Pure-Python, actor-shaped, request/response agents?** → **Ray** — the most
  idiomatic Python home for the stateful-agent essence (one actor per conversation),
  with durability written through to Redis/Fluss. Pekko is its JVM peer.
- **Already reactive / on the JVM, state in Redis or Fluss?** → **Quarkus** (we
  already ship the A2A + RAG gateway on it) or **Spring** (best EIP/enterprise
  fit; Spring AI can supply chat/tools/vectors).
- **Online agentic turns / scheduled / fan-out work on an existing queue?** →
  **Celery** — one turn = one task, on the Celery + Redis stack you already run; C2 via
  a routed queue + lock, state in a Redis `ConversationStore`.
- **Heavy offline data work** — build the cold index, sweep eval/benchmarks, replay
  the graph over a dataset? → **Dask**.
- **Scheduled / triggered agentic workflows, RAG ingestion, human-in-the-loop?** →
  **Airflow** — its retries/backfill/sensors/branching are exactly the fit.

The recurring lesson across all ten: the agent logic is engine-agnostic; the only
thing that changes is the operator/state/DAG seam — and **Redis or Fluss** is the
durable-state answer once Flink's checkpointed keyed state is gone (except Pekko and
Pulsar Functions, which carry durable keyed state natively). Start by reading
[`docs/portability/00-essence-and-core-abstractions.md`](../docs/portability/00-essence-and-core-abstractions.md);
each engine has a matching deep-dive in that folder.

---

## Extending the essence (add it once, every port gets it)

The architecture's payoff: the **two cores are the single source of truth**. Every
adapter consumes the core factories (`Banking.buildGraph()` / `build_banking_graph()`,
`defaultTools()` / `default_tools()`, `retriever()`) and runs `RoutedGraph.handle` —
**not one of the ten reimplements routing, a path, a tool, or retrieval.** So:

- **Add a tool** (`ToolRegistry.register(...)`), **a path** (an `Agent` in the graph's
  paths), **a router rule**, or **a retrieval source** to `jagentic-core` (Java) or
  `pyagentic` (Python) — and every port on that core picks it up with **zero adapter
  changes**. A new path on the Java side flows to Kafka Streams, Pekko, Pulsar, Spring,
  and Quarkus at once; on the Python side to Faust, Ray, Celery, Dask, and Airflow.
- This is enforced by tests, not just convention:
  - `pyagentic/tests/test_extensibility.py` and `jagentic-core` `ExtensibilityTest`
    add a brand-new `freeze_card` tool + `fraud` path **through the public API only**
    (no framework edits) and prove the core routes to and invokes them.
  - The adapter-level counterparts run that *same extension through a real engine seam*:
    `test_adapters.py::test_celery_propagates_an_extended_core_graph` (the live Celery
    task) and the pulsar module's `BankingFunctionTest.extendedCoreGraphFlowsThroughThePulsarSeam`
    (the Pulsar state seam) — confirming a core addition reaches durable state on the
    engine without touching the adapter.

To make a port accept an *arbitrary* extended graph (not just the default `Banking`
one), the seam takes it by injection — e.g. `new BankingFunction(graph, tools,
retriever)` (Pulsar) or `agentic_celery.configure(graph=..., tools=...)` (Celery).
