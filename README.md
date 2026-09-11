# Agentic Streaming

Agentic Streaming builds agents as streaming, stateful, event-sourced systems: an agent's
state is a materialized view over an ordered log of events, one writer per conversation.
Apache Flink is the most complete runtime, and the same agent spec runs on a dozen other
engines across Python, the JVM, Go, and Clojure.

The project was called Agentic Flink. It started as an agent framework for Apache Flink
and grew past the name; Flink is still the richest runtime, but no longer the only one.

Licensed under [Apache 2.0](LICENSE).

## Why streaming engines

Agents that do real work (moving money, resolving tickets, answering customers) have to
survive traffic spikes, node failures, and replayed messages. Streaming engines already
solve that: durable keyed state, exactly-once or idempotent processing, backpressure, and
automatic recovery. Putting agents on top of one means the agent you prototype is the
agent that runs in production, and you get to pick the engine that matches your scale.

## What you can build

| Capability | How it works |
|------------|--------------|
| Agents over live event streams | Kafka, Postgres CDC, Redis pub/sub, webhooks, NATS, Fluss, ZeroMQ, and static seeds are all `Channel<T>`; many channels can fan into one agent |
| Routing and chaining with checkable outcomes | a `router -> path -> verifier` graph dispatches each turn and validates the reply, with input/output guardrails and reproducible rule brains that need no model |
| Almost any function as a tool | `@Tool` methods, async `ToolExecutor`s, MCP servers (stdio and HTTP/SSE), DJL models, and HTTP endpoints, all in one `ToolRegistry` |
| Agents that call other agents | A2A treats a peer agent as a tool: in-process, over a gateway (JSON-RPC, SSE, gRPC, REST), or as an explicit pipeline step, with retries and circuit breaking |
| State that survives failure | per-conversation memory plus keyed state, with durability from the engine (Flink checkpoints, Kafka Streams transactions, Pulsar/BookKeeper, Pekko persistence, Temporal history) |
| Exactly-once where the engine provides it | Flink checkpointed state and Kafka Streams `exactly_once_v2`; idempotent (effectively-once) elsewhere, with the `ConversationStore` as the source of truth |
| Long-running work with the saga pattern | compensation handlers unwind a multi-step flow when a later step fails; Temporal and Pekko add durable, retried, human-in-the-loop workflows |
| Pattern detection across events (CEP) | a declarative [`cep:`](docs/portability/stream-stateful-core.md) block ("3 anomalies on one host within 5 min, escalate") fires a tool or a derived event; portable on every core ([`incident.yaml`](examples/pipelines/incident.yaml)) and native on Flink, alongside timers, windows, replay, and suspend/resume |
| Most data systems | memory, vectors, and long-term storage are SPIs (Postgres, Redis/Valkey, Fluss, pgvector/Qdrant, NATS KV) chosen by a connection link, swappable without touching agent code |
| One definition, many deployments | define the agent in a [`pipeline.yaml`](docs/portability/pipelines.md) and run the same spec on Flink, Pekko, Clojure, or a dozen other backends |

## Quick start

The same banking agent runs on whichever runtime you like. One
[`pipeline.yaml`](examples/pipelines/banking.yaml) describes a router/path/verifier graph
with a tool, a knowledge base, and a guardrail, and runs unchanged everywhere.

```bash
git clone https://github.com/Ugbot/Agentic-Streaming.git && cd Agentic-Streaming
```

```bash
# Python, model-free, no infrastructure (about 30 seconds).
# The backend changes without touching the spec:
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend nats --text "card types?"

# Agentic Pekko: the same spec on an event-sourced actor runtime
mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# Agentic Clojure: pure Clojure on Datomic
cd agentic-clj && clojure -M:run && cd ..

# Apache Flink: the code-first framework
docker compose up -d && docker compose exec ollama ollama pull qwen2.5:3b   # optional infra (podman compose works too)
mvn clean test
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.QuickStartExample"

# ...or run the same pipeline.yaml as a real Flink job
# (source, native CEP, keyBy, agent, sink):
mvn exec:java -Dexec.mainClass="org.agentic.flink.pipeline.FlinkPipelineRunner" \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
```

Each one answers with path `payments` and a balance of `1234.56`. The full walkthrough,
including Go and the swappable backends, is in
[the banking agent on every runtime](docs/examples/banking-everywhere.md).

<details>
<summary><b>Build an agent: Flink Java DSL</b></summary>

```java
Agent agent = Agent.builder()
    .withId("research-bot")
    .withSystemPrompt("You are a research assistant.")
    .withChatConnection(LangChain4jChatConnection.ollama("http://localhost:11434"))
    .withChatSetup(ChatSetup.builder()
        .withModel("qwen2.5:7b")
        .withTemperature(0.3)
        .withMaxResponseTokens(2048)
        .withOutputSchema(OutputSchema.of(ResearchVerdict.class))
        .build())
    .withShortTermTtl(Duration.ofMinutes(30))
    .withVectorMemory(FlinkStateHnswVectorMemory.spec(768))
    .withLongTermStore(StorageFactory.createLongTermStore("postgres", pgConfig))
    .withMemoryChannel(new KafkaContextChannel("kafka:9092", "agent-memories", "research-bot"))
    .withMcpServer(McpServerSpec.stdio("calc", "npx", "-y", "mcp-server-calculator"))
    .withSkill(Skill.builder()
        .withName("citations")
        .withTools("doc-fetch", "summarize")
        .withSystemPromptFragment("Prefer primary sources. Cite arxiv IDs.")
        .build())
    .withListener(new LoggingAgentEventListener(), new MetricsAgentEventListener())
    .withMaxIterations(10)
    .build();
```

Every `with*` method is optional; defaults are discovered via `ServiceLoader`. The minimum
viable agent is `Agent.builder().withId(...).withSystemPrompt(...).build()`.

</details>

<details>
<summary><b>Build an agent: Python</b></summary>

The [`agentic-flink` Python package](docs/python.md) has two paths: PyFlink-native (real
Flink operators via PEMJA, see [`docs/pyflink-integration.md`](docs/pyflink-integration.md))
and JPype standalone (an in-process JVM for notebooks and scripts).

```python
import agentic_flink as af
from agentic_flink import Agent, ChatSetup, langchain4j_ollama, tool

af.start_jvm()

@tool
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

agent = (
    Agent.builder()
        .with_id("calc-bot")
        .with_system_prompt("You are a calculator.")
        .with_chat_connection(langchain4j_ollama())
        .with_chat_setup(ChatSetup(model="qwen2.5:3b"))
        .with_tools(add)
        .build()
)
```

Full guide: [`docs/python.md`](docs/python.md). Examples live under
`python/agentic_flink/examples/`.

</details>

<details>
<summary><b>Build an agent: Agentic Pekko (actors)</b></summary>

The agent brain is reused verbatim from the Flink-free core; only the actor and persistence
shell is Pekko, with one event-sourced, sharded entity per conversation. Run any
`pipeline.yaml` on it, or expose it over HTTP:

```bash
# HTTP front door (Agent Card + POST /agent), A2A-interoperable
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.http.HttpMain
curl -XPOST localhost:8080/agent -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","user_id":"u","text":"what is my balance?"}'

# durability demo: passivate the entity, watch it rehydrate from the event journal
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo
```

Durability profiles (memory, Postgres, Cassandra, Redis) are config-only. Full guide:
[`agentic-pekko/README.md`](agentic-pekko/README.md).

</details>

<details>
<summary><b>Build an agent: Agentic Clojure (Datomic)</b></summary>

An idiomatic Clojure implementation: brains, routers, and verifiers are functions, and the
transcript is immutable Datomic datoms, so history is time-travellable. Requires the
[Clojure CLI](https://clojure.org/guides/install_clojure).

```clojure
;; brains are just functions; the graph is data
(defn balance-brain [user-text ctx]
  (str "[payments] Your balance is " (ctx/call-tool ctx "get_balance" {})))
```

```bash
cd agentic-clj
clojure -M:run            # banking demo (multi-turn, persisted state)
clojure -M:http           # HTTP front door on :8080
clojure -M:time-travel    # replay the transcript `as-of` an earlier point
```

Full guide: [`agentic-clj/README.md`](agentic-clj/README.md).

</details>

## Deploy anywhere

Flink is the feature-richest runtime, but the agent itself is engine-agnostic. Prototype on
the embedded local runtime, then move to a streaming, durable, or batch backend by changing
one line of YAML.

```yaml
# pipeline.yaml: prompts, tools, calls to other agents, retrieval, guardrails, stores
backend: nats            # local, celery, nats, faust, kafka-streams, pekko, temporal, ...
agent:
  router:  { kind: keyword, default: general, rules: { payments: [balance], cards: [card] } }
  paths:
    payments: { brain: llm, prompt: "You are a payments specialist.", tools: [get_balance] }
    cards:    { brain: rule, prompt: "You answer card questions." }
    general:  { brain: rule, prompt: "You answer general questions." }
tools:   [ { id: get_balance, kind: constant, value: 1234.56 } ]
stores:  { conversation: { kind: redis, url: "${AGENTIC_REDIS_URL}" } }
```

External services (Redis/Valkey, Kafka/Fluss, Postgres, NATS) sit behind interfaces and come
up via [`examples/compose/externals.yml`](examples/compose/externals.yml).

See the [pipeline reference](docs/portability/pipelines.md), the
[parity matrix](docs/portability/parity-matrix.md) for what each backend can and cannot do,
and [choosing a backend](docs/portability/choosing-a-backend.md).

## The model: an agent as a materialized view over a stream of events

A conversation is not a request/response call. It is an ordered log of events (turns, tool
results, model outputs, routing decisions), and the agent's state is the value you get by
replaying that log.

Two familiar patterns follow from that:

- Event sourcing: the log is the source of truth and state is derived. That is the
  durability, replay, audit, and recovery story, which each engine implements differently
  (Flink checkpoints, Kafka/NATS offsets, Pulsar BookKeeper, Pekko persistence, Temporal
  history).
- CQRS: a command ("process this turn") is an ordered, single-writer-per-conversation
  mutation, while a query ("what is the current answer or state?") is a fan-out read of the
  view. Separating them lets a conversation be both a durable keyed entity and a stream.

Every engine here does the same thing underneath: materialize a series of events into a
value, in order, durably, per key. See the
[capability inventory](docs/portability/00-essence-and-core-abstractions.md).

## What is in the repository

| Component | What it is | Start here |
|-----------|------------|------------|
| Flink framework | the full agent framework on Apache Flink: state-first memory, vector memory, CEP, chat/embedding/tool/inference SPIs, A2A, RAG, PyFlink | this README |
| Agentic Pekko | the agent core on Apache Pekko actors: one event-sourced, cluster-sharded entity per conversation (single-writer, durable, recoverable), async turns, `backend: pekko`, Pekko HTTP and Kafka Streams front doors, durability on memory/Postgres/Cassandra/Redis | [`agentic-pekko/`](agentic-pekko/) |
| Agentic Clojure | the agent core in pure Clojure (no Java-core dependency) on Datomic: each message is an immutable datom, so the transcript is an event log with time-travel; functions for brains, routers, and verifiers, the FNV embedder at byte-parity, an EDN and YAML pipeline loader, http-kit and MCP-stdio front doors | [`agentic-clj/`](agentic-clj/) |
| Portability pack | the same core on 12 engines across 3 pure cores (`pyagentic`, `jagentic-core`, `goagentic`) plus 2 HTTP gateways; a new tool or path in a core propagates to every port. The cores are standalone agent frameworks in their own right: LLM and embedding libraries, structured output, skills, MCP and A2A clients, saga, context-window management, an in-process HNSW index, vector/long-term/conversation store SPIs (Qdrant, Postgres, Redis), web toolkit, and a DL inference SPI | [`ports/`](ports/) |
| Declarative pipelines | one `pipeline.yaml` (or EDN) targeting any backend, with loaders in Python, the JVM, Go, and Clojure | [`pipelines.md`](docs/portability/pipelines.md) |
| Tool services | the toolkit (web scraping, Tika, RAG, inference, utilities) as standalone, framework-agnostic tools any LLM or framework can call over MCP, REST, gRPC, or Kafka/Redis (Quarkus, no Flink) | [`tool-services/`](tool-services/), [`tool-services.md`](docs/portability/tool-services.md) |
| Design docs | per-engine mapping, parity matrix, choosing a backend | [`docs/portability/`](docs/portability/) |

## Architecture

<details open>
<summary><b>The agent turn (every runtime)</b></summary>

One turn is the same pipeline everywhere. The logic lives in a Flink-free core; each runtime
supplies only the seam that runs it, which is ordering plus durability.

```
        Event in  (a Channel / HTTP / Kafka / queue / seed)
                        |
                        v
        +-------------------------------+
        |  input guardrails             |   regex, classifier: block or allow
        +---------------+---------------+
                        v
        +-------------------------------+
        |  router                       |   keyword or LLM: pick a path
        +---------------+---------------+
                        v
        +-------------------------------+
        |  path brain                   |   rule | ReAct LLM loop
        |    - ToolRegistry (+ MCP)     |   call functions / peer agents (A2A)
        |    - retrieval (hot + cold)   |   RAG over the embedder + vector store
        |    - context-window (MoSCoW)  |
        +---------------+---------------+
                        v
        +-------------------------------+
        |  verifier                     |   validate the reply (prefix, schema)
        +---------------+---------------+
                        v
        +-------------------------------+
        |  output guardrails -> listeners|  logging, metrics, custom hooks
        +---------------+---------------+
                        v
        Reply out  +  one ordered append to the durable conversation log
```

The runtime supplies ordering and durability; the agent code is identical.

| Runtime | single-writer ordering | durability of the log |
|---------|------------------------|-----------------------|
| Flink | keyBy / keyed operator | checkpoints + keyed state |
| Agentic Pekko | actor mailbox + cluster sharding | event-sourced persistence |
| Agentic Clojure | per-conversation serialize | Datomic immutable datoms (with time-travel) |
| Kafka Streams / Pulsar | partition / Key_Shared | changelog / BookKeeper |
| Temporal | one workflow per id | event history |

See the [capability inventory](docs/portability/00-essence-and-core-abstractions.md) for the
engine-agnostic core and the per-engine seam.

</details>

<details>
<summary><b>Flink-runtime specifics</b></summary>

On Flink the loop also offers CEP pattern matching (validation, escalation, saga
compensation), Flink-state-first short-term memory (`ValueState`/`MapState` with
`StateTtlConfig`, durable via checkpoints and with no external hot tier), and in-JVM vector
memory over Flink state (`FlinkStateHnswVectorMemory`):

```
        Events (any Channel<T>: Kafka / Postgres / Redis / webhook / seed)
                        |
                        v
        +-------------------------------+
        |  Flink CEP pattern matching   |   validation, escalation, compensation
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Agent loop                   |   ChatConnection (SPI), ToolRegistry + MCP
        |                               |   ReAct / workflow / custom
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Context management            |  MoSCoW 5-phase compaction
        |                               |   embedder-driven relevancy
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Memory                        |  short-term: Flink keyed state (+TTL)
        |                               |   vector: Flink MapState KNN
        |                               |   long-term: Postgres (optional)
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Listeners (SPI)               |  logging, metrics, custom
        +-------------------------------+
```

Short-term memory is Flink-state-first: checkpoints provide durability and TTL runs
incrementally inside the state backend. Long-term storage is opt-in, for conversation
resumption across job lifetimes and fact archival.

</details>

## Reference

<details>
<summary><b>Features shared by every runtime</b></summary>

These are portable and identical on Flink, Pekko, Clojure, the three cores, and every
backend, enforced by cross-core parity tests.

- Routed graph: `router -> path -> verifier` with input/output guardrails (regex and
  classifier) and reproducible rule brains that need no model.
- LLM brain: a bounded ReAct loop over a `ChatClient` SPI (Ollama, OpenAI, stub) with
  structured output.
- Tools: one `ToolRegistry` holding functions, MCP servers (stdio and HTTP/SSE), and HTTP,
  shared by every brain.
- A2A: a peer agent is just a tool (card, send, retries), in-process or over a gateway.
- Retrieval: the FNV-1a hashing embedder (byte-identical across languages), cosine
  similarity, and a two-tier hot/cold retriever with an in-process HNSW cold tier.
- Skills: bundle tools, a prompt fragment, and required facts onto a path.
- Context window: MoSCoW compaction of the replayed transcript to a token budget.
- Saga and compensation: reverse-order rollback of a multi-step flow.
- Listeners: lifecycle hooks for logging, metrics, and custom handlers.
- Declarative pipeline: the whole agent as one `pipeline.yaml`, or EDN in Clojure.
- Stores behind SPIs: conversation, keyed-state, long-term, and vector, swappable per
  runtime.

</details>

<details>
<summary><b>Flink-runtime features</b></summary>

- Flink-state-first memory: short-term memory is `ValueState`/`MapState` with
  `StateTtlConfig`, and checkpoints provide durability with no external hot tier.
- In-JVM vector memory over Flink state: brute-force KNN or HNSW
  (`FlinkStateHnswVectorMemory`), with an SPI escape hatch for external HNSW backends.
- Named, shareable corpora: `Corpus` with three flavours (single-operator, broadcast,
  external) so ingest and retrieve share one index.
- Unified `Channel<T>` SPI: Kafka, Postgres CDC, Redis pub/sub, webhook, static seeds, and
  LLM-driven tool invocations; many channels union into one operator.
- Web toolkit: Jsoup, crawler-commons, and Apache Tika behind `WebFetchTool`, `CrawlerCore`,
  and `DocumentExtractor`.
- Postgres-default long-term storage: resumption and fact archive via `LongTermMemoryStore`,
  with Redis optional.
- Chat-model SPI: `ChatConnection` (transport) split from `ChatSetup` (per-agent model,
  temperature, structured output). LangChain4J is the default implementation, not the API.
- Embedder SPI: `EmbeddingConnection`, `EmbeddingSetup`, and `EmbeddingClient`; the default
  talks to a local Ollama.
- MCP support: `tools/mcp/` wraps Model Context Protocol servers (stdio and HTTP/SSE) as
  ordinary `ToolExecutor`s.
- Traditional DL models: an `inference/` SPI for classifiers, scorers, embedders, and
  generic models (DJL with PyTorch, TensorFlow, ONNX, HuggingFace). Use them as tools,
  guardrails, the scorer's backend, or standalone.
- Structured output: `OutputSchema<T>` infers JSON Schema from Lombok POJOs and parses LLM
  responses via Jackson.
- ReAct agent: `ReActProcessFunction` packages the thought/action/observation loop, bounded
  by `getMaxIterations()`.
- Skills: bundle tools, a system-prompt fragment, and required facts through
  `AgentBuilder.withSkill(...)`.
- Listeners: the `AgentEventListener` SPI (nine lifecycle hooks), with
  `LoggingAgentEventListener` and `MetricsAgentEventListener` included.
- CEP-driven orchestration: Flink CEP patterns drive validation, escalation, and saga
  compensation.
- `@Tool` annotation discovery: LangChain4J-annotated tools, MCP tools, and `ToolExecutor`s
  share one `ToolRegistry`.

</details>

<details>
<summary><b>Pekko-runtime features</b></summary>

- Event-sourced sharded entity: one `EventSourcedBehavior` per conversation. The mailbox
  gives single-writer ordering, the journal gives durability and recovery, and cluster
  sharding gives one live entity per id across the cluster.
- Async turns: `graph.handle` runs off the actor thread (blocking dispatcher plus
  `pipeToSelf`) and stashes concurrent turns. One `TurnCommitted` event per turn, with
  `turnId` dedupe for at-least-once ingress.
- Durability profiles, config-only: in-memory, Postgres (`pekko-persistence-jdbc`),
  Cassandra, or Redis (write-through).
- Front doors: Pekko HTTP (Agent Card and `POST /agent`) and a backpressured Pekko Streams
  Kafka flow.
- `backend: pekko`: any `pipeline.yaml` runs on the actor runtime via the `BackendProvider`
  SPI (`PipelineMain`). See [`agentic-pekko/`](agentic-pekko/).

</details>

<details>
<summary><b>Clojure-runtime features</b></summary>

- Idiomatic Clojure with no Java-core dependency: brains, routers, and verifiers are
  functions, the registry is a map, and a turn is a pure transform over a context map.
- Datomic storage: each message is an immutable datom, so the transcript is an event log,
  and `as-of` replays any past state. The same client API covers in-process
  `com.datomic/local`, Datomic Pro, and Cloud.
- EDN and YAML pipeline loader: the shared schema, parsed natively, running `banking`,
  `banking-llm`, and `banking-rag`.
- Front doors: http-kit (Agent Card and `POST /agent`) and a JSON-RPC MCP stdio server over
  the tool registry. See [`agentic-clj/`](agentic-clj/).

</details>

<details>
<summary><b>Pluggable surfaces (SPI summary)</b></summary>

| Concern | Interface | Default | Discovery |
|---------|-----------|---------|-----------|
| Short-term memory | `memory.ShortTermMemorySpec` | `FlinkStateShortTermMemory` | `ServiceLoader` + builder |
| Vector memory | `memory.vector.VectorMemorySpec` | `FlinkStateVectorMemory` / `FlinkStateHnswVectorMemory` | Builder |
| Corpus | `corpus.CorpusSpec` | `SingleOperatorCorpus` / `BroadcastCorpus` / `ExternalCorpus` | Builder |
| Long-term store | `storage.LongTermMemoryStore` | `PostgresConversationStore` | `ServiceLoader` + factory |
| External vector store | `storage.VectorStore` | `PgVectorStore` (opt-in) | `ServiceLoader` + factory |
| Channel (continuous input) | `channel.Channel<T>` | `StaticSeed`, `Kafka`, `Webhook`, `KafkaContext`, `PostgresChange`, `RedisPubSub`, `ToolInvocation` | Programmatic |
| Chat transport | `llm.ChatConnection` | `LangChain4jChatConnection` (Ollama) | `ServiceLoader` |
| Embedding transport | `embedding.EmbeddingConnection` | `OllamaEmbeddingConnection` / `DjlEmbeddingConnection` | `ServiceLoader` |
| MCP server | `tools.mcp.McpServerSpec` | none | Programmatic |
| Inference model | `inference.InferenceConnection` | `DjlInferenceConnection` (opt-in) | `ServiceLoader` + builder |
| Guardrail | `inference.Guardrail` | none | Programmatic |
| Web fetch | `web.WebFetchTool` / `web.CrawlerCore` | Jsoup + crawler-commons + Tika (opt-in) | Programmatic |
| Listener | `listener.AgentEventListener` | `LoggingAgentEventListener` | `ServiceLoader` |
| Tool | `tools.ToolExecutor` | built-ins and `@Tool` | `ToolRegistry` |

LangChain4J is the default chat backend, wrapped behind `ChatConnection`. If you need the
raw model, downcast to `LangChain4jChatClient` and call `getUnderlyingModel()` for the
`dev.langchain4j.model.chat.ChatLanguageModel`.

</details>

<details>
<summary><b>Examples</b></summary>

Portable examples run on any runtime, with identical behaviour from one spec. The full
walkthrough is in [the banking agent on every runtime](docs/examples/banking-everywhere.md).

| Spec | Demonstrates | Run (pick a runtime) |
|------|--------------|----------------------|
| [`banking.yaml`](examples/pipelines/banking.yaml) | router, path, verifier, a tool, a guardrail | `python -m agentic_pipeline run examples/pipelines/banking.yaml --text "..."`, `clojure -M:run`, Pekko `PipelineMain` |
| [`banking-llm.yaml`](examples/pipelines/banking-llm.yaml) | a bounded ReAct LLM brain on a path | `... run examples/pipelines/banking-llm.yaml --text "..."` |
| [`banking-rag.yaml`](examples/pipelines/banking-rag.yaml) | HNSW cold tier, skills, context window, classifier guardrail | `... run examples/pipelines/banking-rag.yaml --text "how do I dispute a charge?"` |
| [`multiagent.yaml`](examples/pipelines/multiagent.yaml) | A2A, a peer agent as a tool | `... run examples/pipelines/multiagent.yaml --text "escalate this"` |

Runtime-specific demos: Pekko durability and recovery (`RecoveryDemo`), Clojure Datomic
time-travel (`clojure -M:time-travel`).

<details>
<summary><b>Flink-runtime showcases</b></summary>

These exercise Flink-only capabilities (CEP, side outputs, keyed-state vector memory, Kafka
streaming). Each has an inline `README.md`, a walkthrough under
[`docs/examples/`](docs/examples/), and a wrapper script under
[`examples-bin/`](examples-bin/).

| Use case | Package | Flink-only capability | Run |
|----------|---------|-----------------------|-----|
| Customer-support triage | `example.triage` | guardrail and scorer over keyed state | `./examples-bin/run-support-triage.sh` |
| Real-time content moderation | `example.moderation` | OutputTag side outputs | `./examples-bin/run-moderation.sh` |
| RAG research assistant | `example.rag` | Flink-state keyed vector memory | `./examples-bin/run-rag.sh` |
| Anomaly and incident agent | `example.incident` | Flink CEP pattern matching | `./examples-bin/run-incident.sh` |
| Live research and RAG | `example.research` | crawler frontier as Flink operators | `./examples-bin/run-live-research.sh` |
| Markets (bond, crypto) | `example.markets` | Kafka and Flink streaming | `./examples-bin/run-bond-market.sh` |
| Quick start | `example.QuickStartExample` | minimal agent, one tool | `mvn -q exec:java -Dexec.mainClass=...QuickStartExample` |

</details>

For shorter recipes, see [docs/cookbook.md](docs/cookbook.md).

</details>

<details>
<summary><b>Documentation index</b></summary>

| Document | Description |
|----------|-------------|
| [docs/examples/banking-everywhere.md](docs/examples/banking-everywhere.md) | the same banking agent on every runtime: one spec, the run command per runtime |
| [agentic-pekko/README.md](agentic-pekko/README.md) | Agentic Pekko, the event-sourced sharded actor runtime |
| [agentic-clj/README.md](agentic-clj/README.md) | Agentic Clojure, pure Clojure on Datomic |
| [docs/portability/pekko.md](docs/portability/pekko.md), [clojure.md](docs/portability/clojure.md) | per-engine design notes for the two newest runtimes |
| [docs/portability/pipelines.md](docs/portability/pipelines.md) | declarative `pipeline.yaml` schema and loaders (Python, JVM, Go) |
| [docs/portability/parity-matrix.md](docs/portability/parity-matrix.md) | what each backend can do, plus limitations and three-core parity |
| [docs/portability/choosing-a-backend.md](docs/portability/choosing-a-backend.md) | decision guide across Flink and 12 engines |
| [docs/portability/stream-stateful-core.md](docs/portability/stream-stateful-core.md) | the stream-stateful core: CEP, timers, windows, replay, suspend/resume, tracing |
| [docs/concepts.md](docs/concepts.md) | core concepts: agents, events, tools, memory, the routed graph |
| [docs/configuration.md](docs/configuration.md) | configuration reference (env vars, resolution order) |
| [docs/a2a.md](docs/a2a.md) | the Agent-to-Agent protocol: peer-as-tool, gateway, bridges |
| [docs/memory.md](docs/memory.md) | Flink-state-first memory model, vector memory, feeds |
| [docs/inference.md](docs/inference.md) | DL models as tools, guardrails, scorers, embedders |
| [docs/channels.md](docs/channels.md) | `Channel<T>` SPI: Kafka, Postgres CDC, Redis, webhook, tool transport |
| [docs/corpus.md](docs/corpus.md) | the `Corpus` abstraction and its three flavours |
| [docs/web-toolkit.md](docs/web-toolkit.md) | Jsoup, crawler-commons, Tika: robots-aware fetch and extract |
| [docs/python.md](docs/python.md) | the Python API (JPype standalone) and a pointer to PyFlink-native |
| [docs/pyflink-integration.md](docs/pyflink-integration.md) | PyFlink-native: agent plan, CompileUtils, PEMJA |
| [docs/cookbook.md](docs/cookbook.md) | short recipes for common SPI combinations |
| [docs/examples/](docs/examples/) | long-form walkthroughs of the headline use cases |
| [docs/getting-started.md](docs/getting-started.md) | setup guide and first steps |
| [docs/guides/context-management.md](docs/guides/context-management.md) | MoSCoW prioritization and compaction |
| [docs/guides/storage-quickstart.md](docs/guides/storage-quickstart.md) | storage backend setup |
| [docs/guides/openai-setup.md](docs/guides/openai-setup.md) | configuring OpenAI as the chat backend |
| [docs/guides/flink-agents-integration.md](docs/guides/flink-agents-integration.md) | the optional Apache Flink Agents bridge |
| [docs/reference/agent-framework.md](docs/reference/agent-framework.md) | framework reference and agent patterns |
| [docs/reference/storage-architecture.md](docs/reference/storage-architecture.md) | storage design |
| [docs/reference/troubleshooting.md](docs/reference/troubleshooting.md) | common issues and fixes |

</details>

<details>
<summary><b>Relationship to Apache Flink Agents, and work in progress</b></summary>

Agentic Streaming predates upstream Apache Flink Agents and stays compatible in vocabulary
without taking a hard dependency. User-facing SPI names (`ChatConnection`, `ChatSetup`,
`Skill`, `OutputSchema`, `MemorySet`) mirror upstream's, so a bridge stays thin. The optional
`plugins/flintagents/` module, gated by the `flink-agents` Maven profile, provides
bidirectional adapters.

In development:

- advanced CEP patterns for multi-agent coordination
- a JMH benchmark suite for the chat, embedding, and vector-memory hot paths
- an HNSW-backed `VectorMemorySpec` (JVector or Lucene) as a drop-in upgrade
- a native PyFlink port of the memory primitives
- a plugin refresh to upstream Flink Agents 0.3-SNAPSHOT

</details>

## Requirements

- Java 21+ and Maven 3.9+ (or the committed `./mvnw`) for the Flink framework (Apache Flink 2.2, native FLIP-27/143) and
  for Agentic Pekko, which is built separately after
  `mvn -f ports/jagentic-core/pom.xml install`
- Clojure CLI (tools.deps) for Agentic Clojure under `agentic-clj/`
- Go 1.24+ for the Go core, gateway, and engines under `ports/go/`
- Python 3.11+ for the pure-Python cores, ports, and the FastAPI gateway
- Docker or Podman for the optional Postgres, Redis, Ollama, and NATS services
- Ollama for the local LLM examples

## Contributing

Contributions are welcome; open an issue or a PR. Additional `ChatConnection`,
`EmbeddingConnection`, `LongTermMemoryStore`, `VectorStore`, `InferenceConnection`, and
`Channel<T>` implementations are especially useful.

## License

[Apache License 2.0](LICENSE).
