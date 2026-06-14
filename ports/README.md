# `ports/` — Agentic-Flink on eight engines, compared

Working implementations of the [`docs/portability/`](../docs/portability/) designs:
the Agentic-Flink **essence** (per-conversation stateful agents that remember, route,
use tools, and retrieve) built on engines *other than Flink*. Every port runs the
same `router → path → verifier` **banking** worked example, so the comparison is
apples-to-apples.

Two shared, **Flink-free** essence cores carry the agent logic; each engine port is
a thin **runtime seam** on top:

```
ports/
  pyagentic/      pure-Python essence + LocalRuntime     ← Faust/Ray/Dask/Airflow build on this
  jagentic-core/  pure-Java essence + LocalRuntime       ← Kafka Streams/Spring/Quarkus build on this
  faust/ ray/ dask/ airflow/             (Python adapters)
  kafka-streams/ pekko/ spring/ quarkus/ (JVM adapters)
```

The cores implement the engine-agnostic abstractions once — `ConversationStore`,
`KeyedStateStore`, `ToolRegistry`, `AgentContext`, `RoutedGraph`, hot+cold
`Retrieval`, `Banking` example. An adapter only supplies *how this engine gives a
durable thing per conversation, processed in order, with async I/O*.

---

## The eight at a glance

| Engine | Lang | Streaming? | The one-line fit | Verified here |
|--------|:----:|:----------:|------------------|---------------|
| **Faust** | Python | ✅ yes | `@app.agent` ≈ our agent; `Table` ≈ ConversationStore; native asyncio. Thinnest Python port. | imports clean, engine-guarded¹ |
| **Kafka Streams** | Java | ✅ yes | Closest analog — state stores + partitions + EOS; reuses the Java core; bridge async I/O. | `mvn compile` ✅ |
| **Apache Pekko** | Java | ◑ actors | Actor-per-conversation via Cluster Sharding (C1+C2) **+ Persistence (C3)** — all native. | **runs on real Pekko** ✅ |
| **Ray** | Python | ◑ rpc/actors | Actor-per-conversation = single-writer keyed state in memory; durability write-through. | imports clean, engine-guarded¹ |
| **Quarkus** | Java | ◐ reactive | SmallRye Reactive Messaging + Mutiny; keyed state external (Redis/Fluss). Already our proxy. | `mvn compile` ✅ |
| **Spring** | Java | ◐ messaging | Spring Integration EIP maps router→path→verifier ≈1:1; StateMachine for phase. | `mvn compile` ✅ |
| **Dask** | Python | ✗ batch | Batch data plane — parallel RAG ingest + offline eval; not the live loop. | **runs on real Dask** ✅ |
| **Airflow** | Python | ✗ orchestration | Routed graph → branching DAG; scheduled agentic + RAG ingestion + HITL. | `simulate()` runs ✅ |

¹ Faust/Ray *run* with their engine + a broker/cluster. They can't run on this box (Python 3.14, ahead of faust/ray wheels), so they're import-checked + engine-guarded; their agent logic is the tested `pyagentic` core.

**Core tests:** `pyagentic` 6/6 · `jagentic-core` 4/4 · Python adapters 3/3 · Dask + Pekko run on the real engine · 3 JVM modules compile.

---

## Capability comparison (how each supplies what Flink gave for free)

From the keystone's capability inventory (C1–C12). Legend: **N**ative · **L**ibrary/idiom · **X**ternal service · **—** drop / not a fit.

| Capability | Faust | Kafka Streams | Pekko | Ray | Quarkus | Spring | Dask | Airflow |
|------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **C1** durable keyed state | N `Table` | N state store | N shard+persist | N* actor +X | X Redis/Fluss | X Redis/JPA | L* Actor | X store, tiny XCom |
| **C2** per-key ordering | N partition | N partition | N actor mailbox | N actor mailbox | L partition | L partition | — | — |
| **C3** fault tolerance / EOS | L offsets | N txn EOS | N persistence | X checkpoint | X broker+store | X broker+store | L retry | N retry/idempotent |
| **C4** async I/O | N asyncio | L async-bridge | N ask/pipeToSelf | N async actor | N Mutiny/vthreads | L Reactor/@Async | L futures | L deferrable |
| **C5** backpressure | L | L pause | N Pekko Streams | L | N reactive | L | L | — |
| **C6** connectors | N Kafka | N Kafka | L Connectors | L Serve/Data | N SmallRye | N Cloud Stream | L read_* | L hooks |
| **C9** event-time/windows | N | N | L streams | — | L | L | — | — |
| **C12** topology builder | N agents | N Topology | N actor graph | N actor/task | L msg-flows | L EIP flows | N task graph | N DAG |

`*` = present with a caveat (see that port's design doc). The pattern is stark: the
**heart is C1 + C2** ("a durable thing per key, processed in order"). Engines that
give both natively — **Faust, Kafka Streams, Pekko**, and **Ray** (in memory) — host
the *live* essence faithfully. **Pekko is the only one besides Flink to also give C3
(durability) natively** — via Cluster Sharding + Persistence — without an external
store. Quarkus/Spring assemble C1+C2 from Kafka partitions + an external store.
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
- **Ray** — `RayRuntime.submit` routes each event to the get-or-create
  `ConversationAgent` actor named `conv:<cid>`; the actor *is* the keyed state, runs
  the graph, with a write-through point to a durable store.
- **Quarkus** — `@Incoming("requests")`/`@Outgoing("replies")` agent keyed by
  `conversation_id` + a Mutiny `Uni` REST `AgentResource`; state via the
  `ConversationStore` CDI bean.
- **Spring** — `POST /agent` controller + a Spring Integration flow
  (`.route(Banking::router)` → `cards`/`payments`/`general` channels → `verify`) +
  a Spring StateMachine for the phase FSM.
- **Dask** — not the live graph: a batch pipeline that ingests the KB in parallel,
  scores `recall@1`, and *replays* the routed graph over many transcripts.
- **Airflow** — a `routed_triage` DAG: `@task.branch` (router) → `path_*` tasks →
  `one_success` `verify`; plus an `agentic_ingestion` DAG for the cold index.

---

## Run it

```bash
# pure-Python core (no deps)
cd ports/pyagentic && PYTHONPATH=. python -m pytest tests/ -q          # 6 pass

# Python adapters' portable logic (Dask uses the real engine if installed)
cd ports && PYTHONPATH=pyagentic python -m pytest tests/ -q           # 3 pass
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
```

---

## Choosing an engine

- **Want the live, low-latency, stateful conversational loop?** → **Faust** (pure
  Python) or **Kafka Streams** (JVM). They give keyed durable state + per-key
  ordering natively; the agent maps almost 1:1.
- **Actor-shaped agents on the JVM, with native durability + clustering?** → **Pekko**
  — one supervised, event-sourced entity per conversation via Cluster Sharding; the
  only engine here besides Flink that gives C1+C2+C3 natively (no external store).
- **Pure-Python, actor-shaped, request/response agents?** → **Ray** — the most
  idiomatic Python home for the stateful-agent essence (one actor per conversation),
  with durability written through to Redis/Fluss. Pekko is its JVM peer.
- **Already reactive / on the JVM, state in Redis or Fluss?** → **Quarkus** (we
  already ship the A2A + RAG gateway on it) or **Spring** (best EIP/enterprise
  fit; Spring AI can supply chat/tools/vectors).
- **Heavy offline data work** — build the cold index, sweep eval/benchmarks, replay
  the graph over a dataset? → **Dask**.
- **Scheduled / triggered agentic workflows, RAG ingestion, human-in-the-loop?** →
  **Airflow** — its retries/backfill/sensors/branching are exactly the fit.

The recurring lesson across all eight: the agent logic is engine-agnostic; the only
thing that changes is the operator/state/DAG seam — and **Redis or Fluss** is the
durable-state answer once Flink's checkpointed keyed state is gone. Start by reading
[`docs/portability/00-essence-and-core-abstractions.md`](../docs/portability/00-essence-and-core-abstractions.md);
each engine has a matching deep-dive in that folder.
