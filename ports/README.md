# `ports/` — Agentic-Flink on twelve engines, compared

Working implementations of the [`docs/portability/`](../docs/portability/) designs:
the Agentic-Flink **essence** (per-conversation stateful agents that remember, route,
use tools, and retrieve) built on engines *other than Flink*. Every port runs the
same `router → path → verifier` **banking** worked example, so the comparison is
apples-to-apples.

Three shared, **Flink-free** essence cores carry the agent logic — one per language;
each engine port is a thin **runtime seam** on top, and two HTTP **gateways** are front
doors over the cores:

```
ports/
  pyagentic/      pure-Python essence + LocalRuntime     ← Faust/Ray/NATS/Celery/Dask/Airflow build on this
  jagentic-core/  pure-Java essence + LocalRuntime       ← Kafka Streams/Pekko/Temporal/Pulsar/Spring/Quarkus build on this
  go/             pure-Go essence (core/) + gateway + NATS + Temporal + cmds, one module
  faust/ ray/ nats/ celery/ dask/ airflow/                       (Python adapters)
  kafka-streams/ pekko/ temporal/ pulsar/ spring/ quarkus/       (JVM adapters)
  gateway-fastapi/   FastAPI HTTP gateway over pyagentic (local/celery/nats backends)
  go/gateway/        stdlib net/http gateway over the Go core
  agentic-pipeline/  declarative pipeline.yaml loader + backend shim (Python); Java in
                     jagentic-core/.../pipeline, Go in go/pipeline
```

**Build once, deploy anywhere.** Define an agent in a `pipeline.yaml` (prompts, tools,
calls to other agents, retrieval, guardrails, hot-swappable stores), pick a `backend:`,
and run the *same* spec on any backend in any language — see
[`docs/portability/pipelines.md`](../docs/portability/pipelines.md), the
[parity matrix](../docs/portability/parity-matrix.md), and
[choosing a backend](../docs/portability/choosing-a-backend.md). External services
(Redis/Valkey, Kafka/Fluss, Postgres) sit behind interfaces and come up via
[`examples/compose/externals.yml`](../examples/compose/externals.yml).

The cores implement the engine-agnostic abstractions once — `ConversationStore`,
`KeyedStateStore`, `ToolRegistry`, `AgentContext`, `RoutedGraph`, hot+cold
`Retrieval`, `Banking` example. An adapter only supplies *how this engine gives a
durable thing per conversation, processed in order, with async I/O*.

---

## The twelve at a glance

| Engine | Lang | Streaming? | The one-line fit | Verified here |
|--------|:----:|:----------:|------------------|---------------|
| **Faust** | Python | ✅ yes | `@app.agent` ≈ our agent; `Table` ≈ ConversationStore; native asyncio. Thinnest Python port. | imports clean, engine-guarded¹ |
| **Kafka Streams** | Java | ✅ yes | Closest analog — state stores + partitions + EOS; reuses the Java core; bridge async I/O. | `mvn compile` ✅ |
| **Apache Pekko** | Java | ◑ actors | Actor-per-conversation via Cluster Sharding (C1+C2) **+ Persistence (C3)** — all native. | **runs on real Pekko** ✅ |
| **Temporal** | Java | ◑ durable exec | Entity workflow per conversation; event-sourced **C1+C2+C3** — strongest durability. | **runs + tested** ✅ |
| **Pulsar Functions** | Java | ✅ yes | State store (C1+C3) + `Key_Shared` (C2) — native, in Flink's topic-in/topic-out shape. | **runs + tested** ✅ |
| **Ray** | Python | ◑ rpc/actors | Actor-per-conversation = single-writer keyed state in memory; durability write-through. | imports clean, engine-guarded¹ |
| **NATS JetStream** | Python | ✅ yes | Durable **KV** state (C1) + persistent stream (C3); C2 via subject + KV CAS. Lightweight. | **runs on real JetStream** ✅ |
| **Quarkus** | Java | ◐ reactive | SmallRye Reactive Messaging + Mutiny; keyed state external (Redis/Fluss). Already our proxy. | `mvn compile` ✅ |
| **Spring** | Java | ◐ messaging | Spring Integration EIP maps router→path→verifier ≈1:1; StateMachine for phase. | `mvn compile` ✅ |
| **Celery** | Python | ◐ task queue | One turn = one task; online request/response; C2 via routed queue + lock, state external. | **runs on real Celery** ✅ |
| **Dask** | Python | ✗ batch | Batch data plane — parallel RAG ingest + offline eval; not the live loop. | **runs on real Dask** ✅ |
| **Airflow** | Python | ✗ orchestration | Routed graph → branching DAG; scheduled agentic + RAG ingestion + HITL. | `simulate()` runs ✅ |

¹ Faust/Ray *run* with their engine + a broker/cluster. They can't run on this box (Python 3.14, ahead of faust/ray wheels), so they're import-checked + engine-guarded; their agent logic is the tested `pyagentic` core.

**Core tests:** `pyagentic` 9/9 · `jagentic-core` 6/6 · Python adapters 7/7 · Pulsar adapter 2/2 · Temporal adapter 2/2 · Celery + Dask + Pekko + Pulsar + NATS + Temporal run on the real engine · all JVM modules compile.

---

## Capability comparison (how each supplies what Flink gave for free)

From the keystone's capability inventory (C1–C12). Legend: **N**ative · **L**ibrary/idiom · **X**ternal service · **—** drop / not a fit.

| Capability | Faust | Kafka Streams | Pekko | Temporal | Pulsar Fn | Ray | NATS JS | Quarkus | Spring | Celery | Dask | Airflow |
|------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **C1** durable keyed state | N `Table` | N state store | N shard+persist | N event-source | N state store | N* actor +X | N KV store | X Redis/Fluss | X Redis/JPA | X store | L* Actor | X store, tiny XCom |
| **C2** per-key ordering | N partition | N partition | N actor mailbox | N 1 exec/id | N Key_Shared | N actor mailbox | L subj+CAS | L partition | L partition | L queue+lock | — | — |
| **C3** fault tolerance / EOS | L offsets | N txn EOS | N persistence | N replay+retry | N eff-once | X checkpoint | N JS+idemp | X broker+store | X broker+store | L acks+retry | L retry | N retry/idempotent |
| **C4** async I/O | N asyncio | L async-bridge | N ask/pipeToSelf | N activities | L resp-topic | N async actor | N asyncio | N Mutiny/vthreads | L Reactor/@Async | L chord/chain | L futures | L deferrable |
| **C5** backpressure | L | L pause | N Pekko Streams | L task-queue | L flow-ctl | L | N flow-ctl | N reactive | L | L prefetch | L | — |
| **C6** connectors | N Kafka | N Kafka | L Connectors | L activities | N Pulsar IO | L Serve/Data | L subjects | N SmallRye | N Cloud Stream | L brokers | L read_* | L hooks |
| **C9** event-time/windows | N | N | L streams | — | L windowed | — | — | L | L | — | — | — |
| **C12** topology builder | N agents | N Topology | N actor graph | N workflow code | N fn chain | N actor/task | L subjects | L msg-flows | L EIP flows | L canvas | N task graph | N DAG |

`*` = present with a caveat (see that port's design doc). The pattern is stark: the
**heart is C1 + C2** ("a durable thing per key, processed in order"). Engines that give
both natively — **Faust, Kafka Streams, Pekko, Temporal, Pulsar Functions**, and **Ray**
(in memory) — host the *live* essence faithfully. **Pekko, Temporal, and Pulsar
Functions are the only ones besides Flink to also give C3 (durability) natively** —
Pekko via Cluster Sharding + Persistence, Temporal via event-sourced replay + activity
retries (the strongest), Pulsar via its BookKeeper state store + effectively-once —
with no external store. NATS JetStream gives native durable state (KV) + C3 but makes
C2 a convention (subject + CAS). Quarkus/Spring/Celery assemble C1+C2 from Kafka
partitions or a routed queue + an external store. Celery/NATS still host the *online*
turn; Dask/Airflow don't have C2 at all, so they host *parts* (the data plane / the
workflow topology), not the live conversational loop.

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
- **Temporal** — one `ConversationWorkflow` entity per `conversationId` (`workflowId ==
  conversationId`); each turn is a synchronous `@UpdateMethod` that runs the graph over
  the durable in-workflow `ConversationStore`. `LocalDemo` runs it on an in-memory
  `TestWorkflowEnvironment`; a Query reads back the event-sourced transcript.
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
- **NATS JetStream** — turns publish to `agentic.turn.<cid>` on a persistent stream; a
  consumer runs the graph in a load → handle → save bracket around a per-conversation
  **KV** envelope (durable state, revision-CAS for single-writer) and replies on
  `agentic.reply.<cid>`.
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

# Python adapters' portable logic (Dask + Celery + NATS use the real engine if available)
cd ports && PYTHONPATH=pyagentic python -m pytest tests/ -q           # 7 pass (NATS test skips w/o a server)
python ports/celery/agentic_celery.py         # live banking turns, eager mode (no broker)
podman run -d -p 4222:4222 nats:latest -js && python ports/nats/agentic_nats.py  # live JetStream + KV
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
mvn -f ports/temporal/pom.xml test            # 2 pass (banking + extended-graph via worker factory)
mvn -f ports/temporal/pom.xml -q compile exec:java   # runs banking workflows on an in-memory Temporal service

# pure-Go core + Go engines + Go gateway (one module)
cd ports/go && go test ./...                  # core + gateway + temporal; natsjs runs if a JetStream server is up
go run ./cmd/demo                             # banking graph on the Go LocalRuntime
go run ./cmd/gateway                          # HTTP gateway on :8080
go run ./cmd/natsdemo                         # streamed NATS JetStream round-trip (needs a server)

# HTTP gateways (front doors over the cores)
PYTHONPATH=ports/gateway-fastapi /tmp/af-venv/bin/python -m pytest ports/gateway-fastapi/tests -q  # 9 pass
uvicorn gateway_fastapi.__main__:app          # FastAPI gateway over pyagentic (from ports/gateway-fastapi)
```

---

## A third core (Go) + HTTP gateways

The essence isn't Python- or JVM-specific. [`ports/go/`](go/) is a **third core**, in
pure Go, with the same abstractions (`ConversationStore`, `KeyedStateStore`,
`ToolRegistry`, `RoutedGraph`, `Retrieval`, `Banking`, `LocalRuntime`) and the same
extensibility invariant — and it ships its own **NATS JetStream** and **Temporal**
engines (the Go peers of the Python/Java ports) plus an HTTP gateway, all in one module
reusing the Go core. So NATS JetStream and Temporal each now have **two** implementations
(Python/Go and Java/Go respectively), proving the essence is language-portable.

Two **HTTP gateways** are front doors that expose the banking agent over the same
A2A-style contract — an **Agent Card** at `/.well-known/agent-card.json`, `POST /agent`,
`GET /conversations/{id}`, `GET /healthz` — with an *identical card shape* so a client
can't tell them apart:

| Gateway | Stack | Over | Backends |
|---------|-------|------|----------|
| [`gateway-fastapi/`](gateway-fastapi/) | FastAPI + Pydantic (Python) | `pyagentic` | local (default), celery, nats — via `AGENTIC_GATEWAY_BACKEND` |
| [`go/gateway/`](go/gateway/) | stdlib `net/http` (Go) | `goagentic` core | the Go `LocalRuntime` (or any Go engine `Runtime`) |

Both reuse their core verbatim; the FastAPI one can route turns to the Local, Celery, or
NATS runtimes behind one HTTP surface.

## Choosing an engine

- **Want the live, low-latency, stateful conversational loop?** → **Faust** (pure
  Python) or **Kafka Streams** (JVM). They give keyed durable state + per-key
  ordering natively; the agent maps almost 1:1.
- **Actor-shaped agents on the JVM, with native durability + clustering?** → **Pekko**
  — one supervised, event-sourced entity per conversation via Cluster Sharding.
- **Long-running, retried, human-in-the-loop durable workflows?** → **Temporal** — an
  entity workflow per conversation; the strongest durability here (event-sourced replay
  + activity retries + timers), with the LLM/tool calls as activities. Request/response
  durable orchestration, not a low-latency stream.
- **Already on Pulsar, want native durable state without Flink?** → **Pulsar
  Functions** — the closest non-Flink engine to the topic-in/topic-out streaming shape;
  the state store gives C1+C3 and `Key_Shared` gives C2, all native, ops-light. With
  Pekko and Temporal it's one of only three engines here besides Flink that give
  C1+C2+C3 natively.
- **Lightweight, online, durable — at the edge or already on NATS?** → **NATS
  JetStream** — native durable keyed state (KV) + a persistent stream from one small
  binary, asyncio-native; C2 is a convention (subject + KV compare-and-set).
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

The recurring lesson across all twelve: the agent logic is engine-agnostic; the only
thing that changes is the operator/state/DAG seam — and **Redis or Fluss** is the
durable-state answer once Flink's checkpointed keyed state is gone (except Pekko,
Temporal, Pulsar Functions, and NATS JetStream, which carry durable keyed state
natively). Start by reading
[`docs/portability/00-essence-and-core-abstractions.md`](../docs/portability/00-essence-and-core-abstractions.md);
each engine has a matching deep-dive in that folder.

---

## Extending the essence (add it once, every port gets it)

The architecture's payoff: the **two cores are the single source of truth**. Every
adapter consumes the core factories (`Banking.buildGraph()` / `build_banking_graph()`,
`defaultTools()` / `default_tools()`, `retriever()`) and runs `RoutedGraph.handle` —
**not one of the twelve reimplements routing, a path, a tool, or retrieval.** So:

- **Add a tool** (`ToolRegistry.register(...)`), **a path** (an `Agent` in the graph's
  paths), **a router rule**, or **a retrieval source** to `jagentic-core` (Java) or
  `pyagentic` (Python) — and every port on that core picks it up with **zero adapter
  changes**. A new path on the Java side flows to Kafka Streams, Pekko, Temporal,
  Pulsar, Spring, and Quarkus at once; on the Python side to Faust, Ray, NATS, Celery,
  Dask, and Airflow.
- This is enforced by tests, not just convention:
  - `pyagentic/tests/test_extensibility.py` and `jagentic-core` `ExtensibilityTest`
    add a brand-new `freeze_card` tool + `fraud` path **through the public API only**
    (no framework edits) and prove the core routes to and invokes them.
  - The adapter-level counterparts run that *same extension through a real engine seam*:
    `test_adapters.py` (the live Celery task **and** the live NATS JetStream seam), the
    pulsar module's `BankingFunctionTest.extendedCoreGraphFlowsThroughThePulsarSeam`,
    and the temporal module's `ConversationWorkflowTest.extendedCoreGraphFlowsThroughTheWorkflow`
    — confirming a core addition reaches durable state on the engine without touching
    the adapter.

To make a port accept an *arbitrary* extended graph (not just the default `Banking`
one), the seam takes it by injection — e.g. `new BankingFunction(graph, tools,
retriever)` (Pulsar), `new ConversationWorkflowImpl(graph, tools, retriever)` via a
Temporal worker factory, or `agentic_celery.configure(...)` / `NatsRuntime(graph=...)`.
