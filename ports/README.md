# `ports/`: the shared cores, and the experimental engine adapters compared

> Layout. `ports/` holds three things that are on the acceptance path: the conformance tested
> cores [`jagentic-core/`](jagentic-core/) (JVM) and [`pyagentic/`](pyagentic/) (Python), and
> the portable `pipeline.yaml` CLI [`agentic-pipeline/`](agentic-pipeline/). Everything else,
> the engine adapters, the Go core, the FastAPI gateway and the adapter tests, lives under
> [`experimental/`](experimental/) and is described in
> [`experimental/README.md`](experimental/README.md).
>
> Status of `experimental/`: these adapters predate the `agentic/v1` spec and are not
> conformance tested. None of them run the 24 `agentic/v1` fixtures under `spec/conformance/v1/fixtures`.
> The conformance tested bindings are the ones listed in the generated
> [`docs/capabilities.md`](../docs/capabilities.md) (reference, jvm-core, flink, pekko, clojure,
> python, pyflink, python-jvm, python-flink); of the code under `ports/` only the shared cores
> `pyagentic` and `jagentic-core` are on that list. The adapters are not on the acceptance path
> and may be removed. The "Verified here" column below records what was checked for each adapter
> (compiles, imports, or runs the banking example on the real engine) and nothing more. Every
> claim in the tables names the file that backs it; test counts are the numbers Surefire or
> pytest printed when the commands under "Run it" were executed on a Python 3.12 / JDK 21 box
> without a Go toolchain, Kafka, Pulsar, Temporal, NATS or Ray/Faust installed.

Working implementations of the [`docs/portability/`](../docs/portability/) designs:
the Agentic-Flink **essence** (per-conversation stateful agents that remember, route,
use tools, and retrieve) built on engines *other than Flink*. Every port runs the
same `router → path → verifier` **banking** worked example, so the comparison is
apples-to-apples.

Three shared, **Flink-free** essence cores carry the agent logic, one per language;
each engine port is a thin **runtime seam** on top, and two HTTP **gateways** are front
doors over the cores:

```
ports/
  pyagentic/         pure-Python essence + LocalRuntime, conformance tested   <- experimental/{faust,ray,nats,celery,dask,airflow} build on this
  jagentic-core/     pure-Java essence + LocalRuntime, conformance tested     <- experimental/{kafka-streams,temporal,pulsar,spring,quarkus} build on this
  agentic-pipeline/  declarative pipeline.yaml loader + backend registry (Python); Java in
                     jagentic-core/.../pipeline, Go in experimental/go/pipeline
  experimental/      predates agentic/v1, not conformance tested, may be removed:
    go/                pure-Go essence (core/) + gateway + NATS + Temporal + cmds, one module
    faust/ ray/ nats/ celery/ dask/ airflow/                 (Python adapters)
    kafka-streams/ temporal/ pulsar/ spring/ quarkus/        (JVM adapters)
    pekko/             README only: the original POC was deleted, use ../agentic-pekko/
    gateway-fastapi/   FastAPI HTTP gateway over pyagentic (local/celery/nats backends)
    go/gateway/        stdlib net/http gateway over the Go core
    tests/             pytest for the Python adapters' portable logic
```

**Build once, deploy anywhere.** Define an agent in a `pipeline.yaml` (prompts, tools,
calls to other agents, retrieval, guardrails, hot-swappable stores), pick a `backend:`,
and run the *same* spec on any backend in any language, see
[`docs/portability/pipelines.md`](../docs/portability/pipelines.md), the
[parity matrix](../docs/portability/parity-matrix.md), and
[choosing a backend](../docs/portability/choosing-a-backend.md). External services
(Redis/Valkey, Kafka/Fluss, Postgres) sit behind interfaces and come up via
[`examples/compose/externals.yml`](../examples/compose/externals.yml).

The cores implement the engine-agnostic abstractions once, `ConversationStore`,
`KeyedStateStore`, `ToolRegistry`, `AgentContext`, `RoutedGraph`, hot+cold
`Retrieval`, `Banking` example, and are now **near-complete standalone agent
frameworks**: an LLM `ChatClient` SPI (litellm / langchaingo / LangChain4J + Ollama/
OpenAI), an `Embedder` SPI, structured output, skills, regex **and** classifier
guardrails, ≈9 listener hooks, a `VectorStore` SPI with an in-process **HNSW** index
(+ Qdrant / DuckDB), a `LongTermStore` SPI (Postgres), Redis/Valkey conversations, an
**MCP** client, an **A2A** peer-as-tool client, saga/compensation, context-window
management, a web toolkit, and a DL-inference (`Classifier`/`Scorer`) SPI. Everything has
a model-free default so the offline suites stay green; real providers/backends are opt-in.
An adapter only supplies *how this engine gives a durable thing per conversation,
processed in order, with async I/O*.

---

## The twelve at a glance

| Engine | Lang | Streaming? | The one-line fit | Verified here |
|--------|:----:|:----------:|------------------|---------------|
| **Faust** | Python | yes, yes | `@app.agent` maps to our agent; `FaustTableConversationStore` (`experimental/faust/agentic_faust.py`) puts the transcript in a Faust `Table`. | import-checked only: `test_faust_and_ray_adapters_import_without_engine`; no test runs the Faust worker |
| **Kafka Streams** | Java | yes, yes | `BankingTopology` registers a `persistentKeyValueStore` for the keyed attributes (`STATE_STORE`, changelog-backed by Kafka Streams). The transcript is a `ConversationStore.InMemory()` field on `BankingAgentProcessor`, a heap map that is not in any state store and is lost on restart. `processing.guarantee` is not set. | `BankingTopologyTest` 2/2 on `TopologyTestDriver`, no broker |
| **Apache Pekko** | Java | ◑ actors | Top-level [`agentic-pekko/`](../agentic-pekko/): `ConversationEntity` is an `EventSourcedBehavior`, `ConversationSharding` puts one entity per `conversationId` behind Cluster Sharding, `DurabilityProfile` selects the memory, Postgres, Cassandra or Redis journal, and `ClusterMain` boots a sharded single node. | `./mvnw -f agentic-pekko/pom.xml test`: 62 run, 0 failures, 2 skipped (service-backed journals); `ClusterMainTest` runs the sharded entity on a one-node cluster; `PekkoConformanceTest` runs the 24 fixtures |
| **Temporal** | Java | ◑ durable exec | `ConversationWorkflowImpl`, one entity workflow per `conversationId`, transcript in workflow state that Temporal event-sources. | `ConversationWorkflowTest` 2/2 on `TestWorkflowEnvironment` (in-memory service), no Temporal server |
| **Pulsar Functions** | Java | yes, yes | `BankingFunction` stores the transcript through `PulsarStateConversationStore` on `Context.getState/putState`; `Key_Shared` keying by `conversationId` is deployment configuration described in `experimental/pulsar/README.md`, not code. | `BankingFunctionTest` 2/2 against the test-only reflective `InMemoryContext` (`src/test`); no Pulsar broker or BookKeeper is exercised |
| **Ray** | Python | partial, rpc/actors | `RayRuntime` in `experimental/ray/agentic_ray.py` keeps one `ConversationAgent` actor per `conversationId`; its state is actor memory. The "write-through to a durable store" is a comment at the persistence point, no store is written. | import-checked only: `test_faust_and_ray_adapters_import_without_engine`; no test starts Ray |
| **NATS JetStream** | Python | yes, yes | `NatsRuntime` in `experimental/nats/agentic_nats.py` keeps the per-conversation envelope in a JetStream KV bucket with revision CAS; turns travel on a JetStream stream. | `test_nats_adapter_imports_without_engine` passes; `test_nats_jetstream_kv_roundtrip_and_extension` needs a live server and skipped here |
| **Quarkus** | Java | ◐ reactive | SmallRye Reactive Messaging agent plus a Mutiny REST resource; keyed state is whatever `ConversationStore` bean is wired, external in production. | compiles; `src/test` does not exist (Surefire: "No tests to run") |
| **Spring** | Java | ◐ messaging | Spring Integration flow (`route` to `cards`/`payments`/`general`, then `verify`) plus a Spring StateMachine for the phase. | compiles; `src/test` does not exist (Surefire: "No tests to run") |
| **Celery** | Python | ◐ task queue | `process_turn` task routed by `conversation_queue(cid)`; `CeleryRuntime(eager=True)` runs it in-process with `task_always_eager`. | `test_celery_runtime_runs_banking_on_real_engine` and `test_celery_propagates_an_extended_core_graph` pass in eager mode, no broker |
| **Dask** | Python | batch | Batch ingest, `recall@1` eval and graph replay over local Dask; not the live loop. | `test_dask_ingest_eval_and_replay` passes on local Dask |
| **Airflow** | Python | orchestration | `routed_triage` DAG with `@task.branch`; `simulate()` runs the routing without a scheduler. | `test_airflow_simulate_routes_correctly` and `test_airflow_injectable_extended_graph` pass; no Airflow scheduler runs |

The Pekko row describes the top-level [`agentic-pekko/`](../agentic-pekko/) runtime, which is conformance tested; the `ports/pekko` proof-of-concept it grew out of has been deleted and [`experimental/pekko/`](experimental/pekko/) is a pointer README. No Pekko code lives under `ports/`.

**Test counts as run on this box** (commands under "Run it"): `pyagentic` 242 passed, 8 skipped · `jagentic-core` 167 run, 5 skipped · `agentic-pipeline` 15 passed, 4 skipped · `experimental/tests` (Python adapters) 7 passed, 1 skipped (NATS server) · `gateway-fastapi` 15 passed · Pulsar 2/2 · Kafka Streams 2/2 · Temporal 2/2 · Spring and Quarkus have no tests · `agentic-pekko` 62 run, 2 skipped. The Go module (`experimental/go`) was not run because no Go toolchain was installed; its count is not asserted here. Skips are service-backed paths (Ollama, Qdrant, Postgres, Valkey, MCP, NATS) that report a skip reason instead of passing.

---

## Capability comparison (how each supplies what Flink gave for free)

From the keystone's capability inventory (C1-C12). Legend: **N**ative · **L**ibrary/idiom · **X**ternal service · **-** drop / not a fit.

| Capability | Faust | Kafka Streams | Pekko | Temporal | Pulsar Fn | Ray | NATS JS | Quarkus | Spring | Celery | Dask | Airflow |
|------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **C1** durable keyed state | N `Table` | N* attributes in a state store, transcript in a heap map | N shard+persist (`agentic-pekko`) | N event-source | N state store API (untested against a broker) | - actor memory only | N KV store | X Redis/Fluss | X Redis/JPA | X store | L* Actor | X store, tiny XCom |
| **C2** per-key ordering | N partition | N partition | N actor mailbox | N 1 exec/id | N* Key_Shared (deploy-time setting) | N actor mailbox | L subj+CAS | L partition | L partition | L queue+lock | - | - |
| **C3** fault tolerance / EOS | L offsets | L at-least-once (`processing.guarantee` not set, EOS is a design note only) | N persistence (`agentic-pekko` journals) | N replay+retry | N* state store (untested against a broker) | - none implemented | N JS+idemp | X broker+store | X broker+store | L acks+retry | L retry | N retry/idempotent |
| **C4** async I/O | N asyncio | L async-bridge | N ask/pipeToSelf | N activities | L resp-topic | N async actor | N asyncio | N Mutiny/vthreads | L Reactor/@Async | L chord/chain | L futures | L deferrable |
| **C5** backpressure | L | L pause | N Pekko Streams | L task-queue | L flow-ctl | L | N flow-ctl | N reactive | L | L prefetch | L | - |
| **C6** connectors | N Kafka | N Kafka | L Connectors | L activities | N Pulsar IO | L Serve/Data | L subjects | N SmallRye | N Cloud Stream | L brokers | L read_* | L hooks |
| **C9** event-time/windows | N | N | L streams | - | L windowed | - | - | L | L | - | - | - |
| **C12** topology builder | N agents | N Topology | N actor graph | N workflow code | N fn chain | N actor/task | L subjects | L msg-flows | L EIP flows | L canvas | N task graph | N DAG |

`*` = present with a caveat stated in the cell or in the table above. The pattern: the
**heart is C1 + C2** ("a durable thing per key, processed in order"). The engines whose
shipped adapter gives both are **Pekko** (the top-level `agentic-pekko` module: sharded
event-sourced entity, tested on a one-node cluster) and **Temporal** (entity workflow,
tested on the in-memory service). **Kafka Streams** gives C2 natively but only the keyed
attributes are in the changelog-backed store; the transcript is a heap map. **Pulsar
Functions** codes C1 against the state store API, but the only tests run on a test-only
fake `Context`, and C2 depends on deploying with `Key_Shared`. **Faust** and **Ray** are
import-checked only. **NATS JetStream** gives durable state (KV) plus a persistent stream
and makes C2 a convention (subject + CAS); its live test skips without a server.
Quarkus/Spring/Celery assemble C1+C2 from Kafka partitions or a routed queue plus an
external store. Celery/NATS still host the *online* turn; Dask/Airflow don't have C2 at
all, so they host *parts* (the data plane / the workflow topology), not the live
conversational loop.

---

## How the worked example lands on each

Same `Banking.buildGraph()` (router → cards/payments/general → verifier) everywhere;
only the wiring differs:

- **Faust**: `@app.agent` consumes `agentic.requests.group_by(conversation_id)`,
  runs the graph against a `FaustTableConversationStore`, emits to `agentic.replies`.
- **Kafka Streams**: a `Topology`: source topic → `BankingAgentProcessor` (builds an
  `AgentContext` over a `KeyValueStore`-backed `KeyedStateStore` for attributes and a
  `ConversationStore.InMemory()` heap map for the transcript, runs the graph) → sink
  topic; the keyed store is wired via `StoreBuilder`, the transcript is not.
- **Pekko** (`agentic-pekko/`): one `ConversationEntity` (event-sourced) per
  `conversationId`; the mailbox gives single-writer ordering, the journal selected by
  `DurabilityProfile` gives durability, `ConversationSharding` places entities via
  Cluster Sharding. `PipelineMain` runs any `pipeline.yaml` on a local (unsharded)
  system; `ClusterMain --profile memory|postgres|cassandra|redis` boots a one-node
  cluster with the sharded entity; `RecoveryDemo` shows journal replay.
- **Temporal**: one `ConversationWorkflow` entity per `conversationId` (`workflowId ==
  conversationId`); each turn is a synchronous `@UpdateMethod` that runs the graph over
  the durable in-workflow `ConversationStore`. `LocalDemo` runs it on an in-memory
  `TestWorkflowEnvironment`; a Query reads back the event-sourced transcript.
- **Pulsar Functions**: `BankingFunction.process` builds an `AgentContext` over a
  `PulsarStateConversationStore` (the `Context.getState/putState` API, BookKeeper-backed
  when deployed) and runs the graph; single-writer ordering requires deploying the
  function with a `Key_Shared` subscription keyed by `conversationId`. The test-scoped
  `LocalDemo` runs it against the test-only `InMemoryContext` (a heap map behind
  dynamic proxies), so "state survives turns" there means within one JVM.
- **Ray**: `RayRuntime.submit` routes each event to the get-or-create
  `ConversationAgent` actor named `conv:<cid>`; the actor *is* the keyed state and runs
  the graph. Nothing is written through to a durable store; the code marks the point
  where that would go.
- **Quarkus**: `@Incoming("requests")`/`@Outgoing("replies")` agent keyed by
  `conversation_id` + a Mutiny `Uni` REST `AgentResource`; state via the
  `ConversationStore` CDI bean.
- **Spring**: `POST /agent` controller + a Spring Integration flow
  (`.route(Banking::router)` → `cards`/`payments`/`general` channels → `verify`) +
  a Spring StateMachine for the phase FSM.
- **NATS JetStream**: turns publish to `agentic.turn.<cid>` on a persistent stream; a
  consumer runs the graph in a load → handle → save bracket around a per-conversation
  **KV** envelope (durable state, revision-CAS for single-writer) and replies on
  `agentic.reply.<cid>`.
- **Celery**: `process_turn` is a task routed to `conversation_queue(cid)` (single
  worker = single-writer) + a per-conversation lock; `CeleryRuntime(eager=True)` runs
  it in-process; state in a shared (Redis in prod) `ConversationStore`.
- **Dask**: not the live graph: a batch pipeline that ingests the KB in parallel,
  scores `recall@1`, and *replays* the routed graph over many transcripts.
- **Airflow**: a `routed_triage` DAG: `@task.branch` (router) → `path_*` tasks →
  `one_success` `verify`; plus an `agentic_ingestion` DAG for the cold index.

---

## Run it

```bash
# pure-Python core (no deps)
cd ports/pyagentic && PYTHONPATH=. python -m pytest tests/ -q

# Python adapters' portable logic (Dask + Celery + NATS use the real engine if available)
python -m pytest ports/experimental/tests -q                          # adapters (NATS test skips w/o a server)
cd ports/agentic-pipeline && PYTHONPATH=.:../pyagentic python -m pytest -q   # declarative loader + backend registry
python ports/experimental/celery/agentic_celery.py         # live banking turns, eager mode (no broker)
podman run -d -p 4222:4222 nats:latest -js && python ports/experimental/nats/agentic_nats.py  # live JetStream + KV
python ports/experimental/dask/agentic_dask.py             # batch RAG + recall@1 + replay
python ports/experimental/airflow/agentic_banking_dag.py   # routing simulate (no scheduler)
# faust:  faust -A agentic_faust:app worker -l info     (needs Kafka + faust-streaming)
# ray:    python ports/experimental/ray/agentic_ray.py  (needs ray[default])

# pure-Java core + JVM engine modules (install the core first)
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -f ports/experimental/kafka-streams/pom.xml test       # 2 tests on TopologyTestDriver
./mvnw -f ports/experimental/spring/pom.xml test              # compiles; no tests
./mvnw -f ports/experimental/quarkus/pom.xml test             # compiles; no tests
./mvnw -f ports/experimental/pulsar/pom.xml test              # 2 tests: banking + extended graph on the test-only InMemoryContext
./mvnw -f ports/experimental/pulsar/pom.xml -q test-compile exec:java   # LocalDemo (test scope) on the same fake Context
./mvnw -f ports/experimental/temporal/pom.xml test            # banking + extended-graph via worker factory
./mvnw -f ports/experimental/temporal/pom.xml -q compile exec:java   # runs banking workflows on an in-memory Temporal service
./mvnw -f agentic-pekko/pom.xml test                          # the Pekko runtime of record (not under ports/)

# pure-Go core + Go engines + Go gateway (one module)
cd ports/experimental/go && go test ./...     # core + gateway + temporal; natsjs runs if a JetStream server is up
go run ./cmd/demo                             # banking graph on the Go LocalRuntime
go run ./cmd/gateway                          # HTTP gateway on :8080
go run ./cmd/natsdemo                         # streamed NATS JetStream round-trip (needs a server)

# HTTP gateways (front doors over the cores)
python -m pytest ports/experimental/gateway-fastapi/tests -q
cd ports/experimental/gateway-fastapi && uvicorn gateway_fastapi.__main__:app   # FastAPI gateway over pyagentic
```

---

## A third core (Go) + HTTP gateways

The essence isn't Python- or JVM-specific. [`ports/experimental/go/`](experimental/go/) is a **third core**, in
pure Go, with the same abstractions (`ConversationStore`, `KeyedStateStore`,
`ToolRegistry`, `RoutedGraph`, `Retrieval`, `Banking`, `LocalRuntime`) and the same
extensibility invariant, and it ships its own **NATS JetStream** and **Temporal**
engines (the Go peers of the Python/Java ports) plus an HTTP gateway, all in one module
reusing the Go core. So NATS JetStream and Temporal each now have **two** implementations
(Python/Go and Java/Go respectively), proving the essence is language-portable.

Two **HTTP gateways** are front doors that expose the banking agent over the same
A2A-style contract, an **Agent Card** at `/.well-known/agent-card.json`, `POST /agent`,
`GET /conversations/{id}`, `GET /healthz`, with an *identical card shape* so a client
can't tell them apart:

| Gateway | Stack | Over | Backends |
|---------|-------|------|----------|
| [`experimental/gateway-fastapi/`](experimental/gateway-fastapi/) | FastAPI + Pydantic (Python) | `pyagentic` | local (default), celery, nats, via `AGENTIC_GATEWAY_BACKEND` |
| [`experimental/go/gateway/`](experimental/go/gateway/) | stdlib `net/http` (Go) | `goagentic` core | the Go `LocalRuntime` (or any Go engine `Runtime`) |

Both reuse their core verbatim; the FastAPI one can route turns to the Local, Celery, or
NATS runtimes behind one HTTP surface.

## Choosing an engine

- **Want the live, low-latency, stateful conversational loop?** → **Faust** (pure
  Python) or **Kafka Streams** (JVM). Both engines give keyed durable state + per-key
  ordering natively; the shipped Kafka Streams adapter uses that for attributes only
  (transcript in a heap map), and the Faust adapter is import-checked only.
- **Actor-shaped agents on the JVM, with native durability + clustering?** → **Pekko**
  (`agentic-pekko/`), one supervised, event-sourced entity per conversation via Cluster
  Sharding, started by `ClusterMain`.
- **Long-running, retried, human-in-the-loop durable workflows?** → **Temporal**, an
  entity workflow per conversation; the strongest durability here (event-sourced replay
  + activity retries + timers), with the LLM/tool calls as activities. Request/response
  durable orchestration, not a low-latency stream.
- **Already on Pulsar, want native durable state without Flink?** → **Pulsar
  Functions**, the closest non-Flink engine to the topic-in/topic-out streaming shape;
  the state store API carries C1 and a `Key_Shared` deployment gives C2. The adapter
  here is tested only against a fake `Context`, so treat the broker path as unverified.
- **Lightweight, online, durable, at the edge or already on NATS?** → **NATS
  JetStream**, native durable keyed state (KV) + a persistent stream from one small
  binary, asyncio-native; C2 is a convention (subject + KV compare-and-set).
- **Pure-Python, actor-shaped, request/response agents?** → **Ray**, the most
  idiomatic Python home for the stateful-agent essence (one actor per conversation).
  The adapter keeps state in actor memory and does not persist it. Pekko is its JVM peer.
- **Already reactive / on the JVM, state in Redis or Fluss?** → **Quarkus** (we
  already ship the A2A + RAG gateway on it) or **Spring** (best EIP/enterprise
  fit; Spring AI can supply chat/tools/vectors).
- **Online agentic turns / scheduled / fan-out work on an existing queue?** →
  **Celery**: one turn = one task, on the Celery + Redis stack you already run; C2 via
  a routed queue + lock, state in a Redis `ConversationStore`.
- **Heavy offline data work**: build the cold index, sweep eval/benchmarks, replay
  the graph over a dataset? → **Dask**.
- **Scheduled / triggered agentic workflows, RAG ingestion, human-in-the-loop?** →
  **Airflow**: its retries/backfill/sensors/branching are exactly the fit.

The recurring lesson across all twelve: the agent logic is engine-agnostic; the only
thing that changes is the operator/state/DAG seam, and **Redis or Fluss** is the
durable-state answer once Flink's checkpointed keyed state is gone (except Pekko and
Temporal, whose adapters here are tested with durable keyed state, and Pulsar Functions
and NATS JetStream, whose adapters code against a native durable store but are only
exercised here against a fake or skipped without a server). Start by reading
[`docs/portability/00-essence-and-core-abstractions.md`](../docs/portability/00-essence-and-core-abstractions.md);
each engine has a matching deep-dive in that folder.

---

## Extending the essence (add it once, every port gets it)

The architecture's payoff: the **two cores are the single source of truth**. Every
adapter consumes the core factories (`Banking.buildGraph()` / `build_banking_graph()`,
`defaultTools()` / `default_tools()`, `retriever()`) and runs `RoutedGraph.handle`,
**not one of the twelve reimplements routing, a path, a tool, or retrieval.** So:

- **Add a tool** (`ToolRegistry.register(...)`),**a path** (an `Agent` in the graph's
  paths), **a router rule**, or **a retrieval source** to `jagentic-core` (Java) or
  `pyagentic` (Python), and every port on that core picks it up with **zero adapter
  changes**. A new path on the Java side flows to Kafka Streams, Pekko, Temporal,
  Pulsar, Spring, and Quarkus at once; on the Python side to Faust, Ray, NATS, Celery,
  Dask, and Airflow.
- This is enforced by tests, not just convention:
  - `pyagentic/tests/test_extensibility.py` and `jagentic-core` `ExtensibilityTest`
    add a brand-new `freeze_card` tool + `fraud` path **through the public API only**
    (no framework edits) and prove the core routes to and invokes them.
  - The adapter-level counterparts run that *same extension through a real engine seam*:
    `experimental/tests/test_adapters.py` (the live Celery task **and** the live NATS JetStream seam), the
    pulsar module's `BankingFunctionTest.extendedCoreGraphFlowsThroughThePulsarSeam`,
    and the temporal module's `ConversationWorkflowTest.extendedCoreGraphFlowsThroughTheWorkflow`
, confirming a core addition reaches durable state on the engine without touching
    the adapter.

To make a port accept an *arbitrary* extended graph (not just the default `Banking`
one), the seam takes it by injection, e.g. `new BankingFunction(graph, tools,
retriever)` (Pulsar), `new ConversationWorkflowImpl(graph, tools, retriever)` via a
Temporal worker factory, or `agentic_celery.configure(...)` / `NatsRuntime(graph=...)`.
