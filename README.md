<div align="center">

# Agentic Streaming

**Build resilient pipelines of stateful agents that act on continuous streams of data —
chain them with almost any function call, reach almost any data system, and deploy the
same agent on Flink or a dozen other engines.**

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Languages](https://img.shields.io/badge/languages-Python%20·%20JVM%20·%20Go%20·%20Clojure-informational.svg)](#whats-in-the-box)
[![Backends](https://img.shields.io/badge/backends-Flink%20%2B%2012%20engines-success.svg)](docs/portability/parity-matrix.md)
[![Build once](https://img.shields.io/badge/build%20once-deploy%20anywhere-orange.svg)](docs/portability/pipelines.md)

[Why](#why) ·
[What you can build](#what-you-can-build) ·
[Quick start](#quick-start) ·
[Deploy anywhere](#deploy-anywhere) ·
[Architecture](#architecture) ·
[Docs](#reference)

<sub>Formerly **Agentic Flink**. It began as an agent framework *for* Apache Flink and outgrew the name —
the same essence now runs across Python, the JVM, Go, and Clojure. Flink is still the first-class, most
feature-complete runtime; it's just no longer the only one.</sub>

</div>

---

## Why

**An agent is only worth building if it actually works — and "works" means it stays
reliable under pressure.** The moment agents do real work — moving money, resolving
tickets, touching infrastructure, answering customers — a clever demo isn't enough. It has
to keep its promises when traffic spikes, a node dies, or a message is replayed.

Reliability at scale isn't a feature you bolt on later; it's the **substrate**. So instead
of inventing a fragile new runtime, Agentic Streaming puts agents on the foundations that
already run the world's high-throughput, fault-tolerant systems — **durable keyed state,
exactly-once / idempotent processing, backpressure, and automatic recovery**. The agent you
prototype in a notebook is the *same* agent that survives production load, partial
failures, and restarts.

The worst outcome is building something awesome that falls over the first time it matters.
This project exists so you can build something awesome **and** keep it standing under
pressure — then deploy it on whichever battle-tested engine your scale demands.

---

## What you can build

Agentic Streaming is for **resilient pipelines of stateful agents over continuous data** —
not one-shot chatbot calls.

| You can… | How |
|----------|-----|
| 🌊 **Run agents over live event streams** | Kafka · Postgres CDC · Redis pub/sub · webhooks · NATS · Fluss · ZeroMQ · seeds — every input is a `Channel<T>`, and many fan into one agent |
| 🧭 **Route & chain with deterministic outcomes** | a `router → path → verifier` graph dispatches each turn and **validates the reply**, with input/output **guardrails** and fully reproducible rule brains (no model required) |
| 🛠️ **Call almost any function as a tool** | `@Tool` methods · async `ToolExecutor`s · **MCP** servers (stdio + HTTP/SSE) · DJL models (classifier/scorer/guardrail) · HTTP — one `ToolRegistry` |
| 🤝 **Chain agents that call other agents** | **A2A** — a peer agent is just a tool: in-process, over a gateway (JSON-RPC / SSE / gRPC / REST), or as an explicit step, with retries + circuit-breaking |
| 💾 **Keep state & survive failure** | first-class per-conversation memory + keyed state; durability from the engine — Flink checkpoints, Kafka Streams transactions, Pulsar/BookKeeper, Pekko persistence, Temporal history |
| ✅ **Exactly-once where the engine gives it** | Flink checkpointed state · Kafka Streams `exactly_once_v2`; **idempotent / effectively-once** everywhere else (the `ConversationStore` is the source of truth) |
| 🔄 **Resolve long work with the saga pattern** | compensation/rollback handlers unwind a multi-step flow when a later step fails; Temporal/Pekko add durable, retried, human-in-the-loop workflows |
| ⏱️ **Detect patterns across events (CEP)** | a declarative [`cep:`](docs/portability/stream-stateful-core.md) block — "3 anomalies on one host within 5 min → escalate" — fires a tool or a derived event; **portable on every core** ([`incident.yaml`](examples/pipelines/incident.yaml)) and **native on Flink**, with timers · windows · replay · suspend/resume alongside |
| 🔌 **Reach almost any data system** | memory, vectors, and long-term storage are SPIs — Postgres · Redis/Valkey · Fluss · pgvector/Qdrant · NATS KV — chosen by a connection link and **hot-swappable** without touching agent code |
| 📦 **Build once, deploy anywhere** | define the whole agent in a [`pipeline.yaml`](docs/portability/pipelines.md) and run the *same* spec on Flink, Pekko, Clojure, or a dozen other backends (Python / JVM / Go / Clojure) |

> **The through-line:** resilient pipelines of agents that act on almost any data, chain
> with almost any function call, and reach almost any data system — with the correctness
> guarantees the underlying engine can give.

---

## Quick start

**The same banking agent, on whichever runtime you like.** One
[`pipeline.yaml`](examples/pipelines/banking.yaml) — a router→path→verifier graph with a tool, a
knowledge base, and a guardrail — runs unchanged everywhere. Pick a runtime:

```bash
git clone https://github.com/Ugbot/Agentic-Streaming.git && cd Agentic-Streaming
```

```bash
# ⚡ Fastest — Python, model-free, no infra (30 seconds). Swap the backend without touching the spec:
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend nats --text "card types?"

# 🎭 Agentic Pekko — the spec on an event-sourced actor runtime
mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"

# 🟢 Agentic Clojure — pure Clojure on Datomic
cd agentic-clj && clojure -M:run && cd ..

# 🌊 Apache Flink — the code-first framework (richest runtime)
docker compose up -d && docker compose exec ollama ollama pull qwen2.5:3b   # optional infra (or `podman compose`)
mvn clean test
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.QuickStartExample"
# …or run the SAME pipeline.yaml as a real Flink job (source → native CEP → keyBy → agent → sink):
mvn exec:java -Dexec.mainClass="org.agentic.flink.pipeline.FlinkPipelineRunner" \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"
```

Every one answers with path `payments` and a balance of `1234.56`. The full walkthrough — including
Go and the dozen swappable backends — is **[the banking agent on every runtime](docs/examples/banking-everywhere.md)**.

<details>
<summary><b>Build an agent — Flink Java DSL</b></summary>

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

Every `with*` method is optional — defaults are discovered via `ServiceLoader`. The
minimum viable agent is `Agent.builder().withId(...).withSystemPrompt(...).build()`.

</details>

<details>
<summary><b>Build an agent — Python</b></summary>

The framework ships an [`agentic-flink` Python package](docs/python.md) with two paths:
**PyFlink-native** (real Flink operators via PEMJA, see
[`docs/pyflink-integration.md`](docs/pyflink-integration.md)) and **JPype standalone**
(in-process JVM for notebooks/scripts).

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

Full guide: [`docs/python.md`](docs/python.md) · examples under `python/agentic_flink/examples/`.

</details>

<details>
<summary><b>Build an agent — Agentic Pekko (actors)</b></summary>

The agent brain is reused verbatim from the Flink-free core; only the actor + persistence shell is
Pekko (one event-sourced, sharded entity per conversation). Run any `pipeline.yaml` on it, or expose
it over HTTP:

```bash
# HTTP front door (Agent Card + POST /agent) — A2A-interoperable
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.http.HttpMain
curl -XPOST localhost:8080/agent -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","user_id":"u","text":"what is my balance?"}'

# durability showcase: passivate the entity, watch it rehydrate from the event journal
mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo
```

Durability profiles (memory · Postgres · Cassandra · Redis) are config-only. Full guide:
[`agentic-pekko/README.md`](agentic-pekko/README.md).

</details>

<details>
<summary><b>Build an agent — Agentic Clojure (Datomic)</b></summary>

A pure, idiomatic Clojure realization — brains/routers/verifiers are functions, the transcript is
immutable Datomic datoms (so history is time-travellable). Requires the
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
clojure -M:time-travel    # replay the transcript `as-of` an earlier point (Datomic immutability)
```

Full guide: [`agentic-clj/README.md`](agentic-clj/README.md).

</details>

---

## Deploy anywhere

Flink is the first-class, feature-richest runtime — but the **agent itself is
engine-agnostic**. Prototype on the embedded **Local** runtime, then move to a
streaming / durable / batch backend by changing one line of YAML.

```yaml
# pipeline.yaml — prompts, tools, calls-to-other-agents, retrieval, guardrails, stores
backend: nats            # local · celery · nats · faust · kafka-streams · pekko · temporal · …
agent:
  router:  { kind: keyword, default: general, rules: { payments: [balance], cards: [card] } }
  paths:
    payments: { brain: llm, prompt: "You are a payments specialist.", tools: [get_balance] }
    cards:    { brain: rule, prompt: "You answer card questions." }
    general:  { brain: rule, prompt: "You answer general questions." }
tools:   [ { id: get_balance, kind: constant, value: 1234.56 } ]
stores:  { conversation: { kind: redis, url: "${AGENTIC_REDIS_URL}" } }   # hot-swappable
```

External services (Redis/Valkey, Kafka/Fluss, Postgres, NATS) sit behind interfaces and
come up via [`examples/compose/externals.yml`](examples/compose/externals.yml).

➡️ **[Pipeline reference](docs/portability/pipelines.md)** ·
**[Parity matrix](docs/portability/parity-matrix.md)** (what each backend can do + its
limits) · **[Choosing a backend](docs/portability/choosing-a-backend.md)**

---

## The idea: an agent is a materialized view over a stream of events

> Treat an agent as **both stateful and a stream**. A conversation isn't a
> request/response call — it's an ordered **log of events** (turns, tool results, model
> outputs, routing decisions), and the agent's state is just a **materialized view** over
> that log: *the value you get by replaying its events.*

Two long-standing patterns fall straight out of that — and they're the real ethos here:

- **Event sourcing** — the log is the source of truth, state is derived. That's the
  durability / replay / audit / recovery story every engine implements differently (Flink
  checkpoints, Kafka/NATS offsets, Pulsar BookKeeper, Pekko persistence, Temporal history).
- **CQRS** — a **command** ("process this turn") is an ordered, single-writer-per-conversation
  mutation; a **query** ("what's the current answer/state?") is a fan-outable read of the
  view. Separating them lets a conversation be both a durable keyed entity *and* a stream.

Every engine here is, at heart, the same move: **materialize a series of events into a
value, in order, durably, per key** — see the
[capability inventory](docs/portability/00-essence-and-core-abstractions.md).

---

## What's in the box

| Component | What it is | Start here |
|-----------|-----------|------------|
| **Flink framework** *(first-class)* | the full agent framework on Apache Flink — state-first memory, vector memory, CEP, chat/embedding/tool/inference SPIs, A2A, RAG, PyFlink | this README |
| **Agentic Pekko** *(first-class)* | the agent essence on **Apache Pekko** actors — one event-sourced, cluster-sharded entity per conversation (single-writer + durable + recoverable), async turns, `backend: pekko`, Pekko HTTP + Kafka-Streams front doors, durability on memory/Postgres/Cassandra/Redis | [`agentic-pekko/`](agentic-pekko/) |
| **Agentic Clojure** *(first-class)* | the agent essence as **pure, idiomatic Clojure** (no Java-core dep) on **Datomic** — each message an immutable datom, so the transcript *is* an event log with time-travel; functions for brains/routers/verifiers, the FNV embedder at byte-parity, EDN+YAML pipeline loader, http-kit + MCP-stdio front doors | [`agentic-clj/`](agentic-clj/) |
| **Portability pack** | the same essence on **12 engines** across **3 pure cores** (`pyagentic` / `jagentic-core` / `goagentic`) + 2 HTTP gateways; a new tool/path in a core propagates to every port. The cores are **near-complete standalone agent frameworks** — real LLM/embedding libs, structured output, skills, MCP + A2A clients, saga, context-window mgmt, an in-process **HNSW** index, vector/long-term/conversation store SPIs (Qdrant/Postgres/Redis), web toolkit, DL-inference SPI | [`ports/`](ports/) |
| **Declarative pipelines** | one `pipeline.yaml` (or EDN) → any backend; loaders in Python, JVM, Go, and Clojure | [`pipelines.md`](docs/portability/pipelines.md) |
| **Tool services** | the toolkit (web scraping, **Tika**, RAG, inference, utilities) as standalone, framework-agnostic tools any LLM/framework runs over **MCP · REST · gRPC · Kafka/Redis** (Quarkus, Flink-free) | [`tool-services/`](tool-services/) · [`tool-services.md`](docs/portability/tool-services.md) |
| **Design docs** | per-engine mapping, parity matrix, choosing-a-backend | [`docs/portability/`](docs/portability/) |

---

## Architecture

<details open>
<summary><b>The agent turn (every runtime)</b></summary>

One turn is the same pipeline everywhere — the **essence** lives in a Flink-free core; each runtime
only supplies the seam that runs it (ordering + durability).

```
        Event in  (a Channel / HTTP / Kafka / queue / seed)
                        |
                        v
        +-------------------------------+
        |  input guardrails             |   regex · classifier — block / allow
        +---------------+---------------+
                        v
        +-------------------------------+
        |  router                       |   keyword / LLM → pick a path
        +---------------+---------------+
                        v
        +-------------------------------+
        |  path brain                   |   rule | ReAct LLM loop
        |    · ToolRegistry (+ MCP)     |   call functions / peer agents (A2A)
        |    · retrieval (hot + cold)   |   RAG over the embedder + vector store
        |    · context-window (MoSCoW)  |
        +---------------+---------------+
                        v
        +-------------------------------+
        |  verifier                     |   validate the reply (e.g. prefix / schema)
        +---------------+---------------+
                        v
        +-------------------------------+
        |  output guardrails → listeners|   logging · metrics · custom hooks
        +---------------+---------------+
                        v
        Reply out  +  one ordered append to the durable conversation log
```

**The runtime supplies ordering + durability** — the agent code is identical:

| Runtime | single-writer ordering | durability of the log |
|---------|------------------------|-----------------------|
| **Flink** | keyBy / keyed operator | checkpoints + keyed state |
| **Agentic Pekko** | actor mailbox + cluster sharding | event-sourced persistence |
| **Agentic Clojure** | per-conversation serialize | Datomic immutable datoms (+ time-travel) |
| **Kafka Streams / Pulsar** | partition / Key_Shared | changelog / BookKeeper |
| **Temporal** | one workflow per id | event history |

See the [capability inventory](docs/portability/00-essence-and-core-abstractions.md) for the
engine-agnostic core + the per-engine seam.

</details>

<details>
<summary><b>Flink-runtime specifics</b></summary>

On Flink the loop additionally offers **CEP pattern matching** (validation / escalation / saga
compensation), **Flink-state-first short-term memory** (`ValueState`/`MapState` + `StateTtlConfig`,
durable via checkpoints — no external HOT tier), and **in-JVM vector memory over Flink state**
(`FlinkStateHnswVectorMemory`):

```
        Events (any Channel<T>: Kafka / Postgres / Redis / webhook / seed)
                        |
                        v
        +-------------------------------+
        |  Flink CEP pattern matching   |   validation · escalation · compensation
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Agent loop                   |   ChatConnection (SPI) · ToolRegistry + MCP
        |                               |   ReAct / workflow / custom
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Context management            |   MoSCoW 5-phase compaction
        |                               |   embedder-driven relevancy
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Memory                        |   short-term: Flink keyed state (+TTL)
        |                               |   vector: Flink MapState KNN
        |                               |   long-term: Postgres (opt.)
        +---------------+---------------+
                        v
        +-------------------------------+
        |  Listeners (SPI)               |   logging · metrics · custom
        +-------------------------------+
```

Short-term memory is Flink-state-first: checkpoints provide durability and TTL runs
incrementally inside the state backend. Long-term storage is opt-in (conversation
resumption across job lifetimes + fact archival).

</details>

---

## Reference

<details>
<summary><b>Key features — essence (every runtime)</b></summary>

These are the portable agent features — identical on Flink, Pekko, Clojure, the three cores, and
every backend (enforced by cross-core parity tests):

- **Routed graph** — `router → path → verifier` with input/output **guardrails** (regex + classifier) and reproducible **rule brains** (no model required).
- **LLM brain** — a bounded **ReAct** loop over a `ChatClient` SPI (Ollama/OpenAI/stub), with structured output.
- **Tools** — one `ToolRegistry`: functions, **MCP** servers (stdio + HTTP/SSE), and HTTP, shared by every brain.
- **A2A** — a peer agent is just a tool (card + send + retries), in-process or over a gateway.
- **Retrieval** — the FNV-1a hashing embedder (byte-identical across languages) + cosine + a two-tier hot/cold retriever; in-process **HNSW** cold tier.
- **Skills** — bundle tools + a prompt fragment + required facts onto a path.
- **Context-window** — MoSCoW compaction of the replayed transcript to a token budget.
- **Saga / compensation** — reverse-order rollback of a multi-step flow.
- **Listeners** — lifecycle hooks (logging / metrics / custom).
- **Declarative pipeline** — the whole agent as one `pipeline.yaml` (EDN too, in Clojure).
- **Stores behind SPIs** — conversation / keyed-state / long-term / vector, hot-swappable per runtime.

</details>

<details>
<summary><b>Key features — Flink runtime</b></summary>

- **Flink-state-first memory** — short-term memory is `ValueState`/`MapState` with `StateTtlConfig`; checkpoints provide durability with no external HOT tier.
- **In-JVM vector memory** over Flink state — brute-force KNN or HNSW (`FlinkStateHnswVectorMemory`); SPI escape hatch for external HNSW backends.
- **Named, shareable corpora** — `Corpus` with three flavours (single-operator, broadcast, external) so ingest + retrieve share one index.
- **Unified `Channel<T>` SPI** — Kafka, Postgres CDC, Redis pub/sub, webhook, static seeds, LLM-driven tool invocations; many channels union into one operator.
- **Web toolkit** — Jsoup + crawler-commons + Apache Tika behind `WebFetchTool`, `CrawlerCore`, `DocumentExtractor`.
- **Postgres-default long-term storage** — resumption + fact archive via `LongTermMemoryStore`; Redis optional.
- **Chat-model SPI** — `ChatConnection` (transport) split from `ChatSetup` (per-agent model/temperature/structured output). LangChain4J is the default impl, not the API.
- **Embedder SPI** — `EmbeddingConnection` / `EmbeddingSetup` / `EmbeddingClient`; default talks to local Ollama.
- **MCP support** — `tools/mcp/` wraps Model Context Protocol servers (stdio + HTTP/SSE) as ordinary `ToolExecutor`s.
- **Traditional DL models** — `inference/` SPI for classifiers, scorers, embedders, generic models (DJL: PyTorch/TensorFlow/ONNX/HuggingFace). Use as tools, guardrails, the scorer's backend, or standalone.
- **Structured output** — `OutputSchema<T>` infers JSON Schema from Lombok POJOs and parses LLM responses via Jackson.
- **ReAct agent** — `ReActProcessFunction` packages the Thought/Action/Observation loop, bounded by `getMaxIterations()`.
- **Skills** — bundle tools + system-prompt fragment + required facts; `AgentBuilder.withSkill(...)`.
- **Listeners** — `AgentEventListener` SPI (nine lifecycle hooks); `LoggingAgentEventListener` + `MetricsAgentEventListener` ship in-box.
- **CEP-driven orchestration** — Flink CEP patterns drive validation, escalation, saga compensation.
- **`@Tool` annotation discovery** — LangChain4J-annotated tools, MCP tools, and `ToolExecutor`s share one `ToolRegistry`.

</details>

<details>
<summary><b>Key features — Pekko runtime</b></summary>

- **Event-sourced sharded entity** — one `EventSourcedBehavior` per conversation; the mailbox gives single-writer ordering, the journal gives durability + recovery, Cluster Sharding gives one live entity per id across the cluster.
- **Async turns** — `graph.handle` runs off the actor thread (blocking dispatcher + `pipeToSelf`), stashing concurrent turns; one `TurnCommitted` event per turn, `turnId` dedupe for at-least-once ingress.
- **Durability profiles** — config-only: in-memory · Postgres (`pekko-persistence-jdbc`) · Cassandra · Redis (write-through).
- **Front doors** — Pekko HTTP (Agent Card + `POST /agent`) and a backpressured Pekko Streams Kafka flow.
- **`backend: pekko`** — any `pipeline.yaml` runs on the actor runtime via the `BackendProvider` SPI (`PipelineMain`). See [`agentic-pekko/`](agentic-pekko/).

</details>

<details>
<summary><b>Key features — Clojure runtime</b></summary>

- **Pure, idiomatic Clojure** — no Java-core dependency; brains/routers/verifiers are functions, the registry is a map, a turn is a pure transform over a context map.
- **Datomic storage** — each message is an immutable datom, so the transcript *is* an event log; **time-travel** (`as-of`) replays any past state. Same client API for in-process `com.datomic/local` · Datomic Pro · Cloud.
- **EDN + YAML pipeline loader** — the shared schema, parsed natively; runs `banking`/`banking-llm`/`banking-rag`.
- **Front doors** — http-kit (Agent Card + `POST /agent`) and a JSON-RPC MCP stdio server over the tool registry. See [`agentic-clj/`](agentic-clj/).

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
| Tool | `tools.ToolExecutor` | built-ins + `@Tool` | `ToolRegistry` |

LangChain4J is the default chat backend, wrapped behind `ChatConnection`. Power users can
downcast to `LangChain4jChatClient` and call `getUnderlyingModel()` for the raw
`dev.langchain4j.model.chat.ChatLanguageModel`.

</details>

<details>
<summary><b>Examples</b></summary>

**Portable examples (run on any runtime).** One spec, identical behaviour everywhere — full
walkthrough in **[the banking agent on every runtime](docs/examples/banking-everywhere.md)**.

| Spec | Demonstrates | Run (pick a runtime) |
|------|--------------|----------------------|
| [`banking.yaml`](examples/pipelines/banking.yaml) | router→path→verifier, a tool, a guardrail | `python -m agentic_pipeline run examples/pipelines/banking.yaml --text "…"` · `clojure -M:run` · Pekko `PipelineMain` |
| [`banking-llm.yaml`](examples/pipelines/banking-llm.yaml) | a bounded **ReAct LLM brain** on a path | `… run examples/pipelines/banking-llm.yaml --text "…"` |
| [`banking-rag.yaml`](examples/pipelines/banking-rag.yaml) | HNSW cold tier, skills, context-window, classifier guardrail | `… run examples/pipelines/banking-rag.yaml --text "how do I dispute a charge?"` |
| [`multiagent.yaml`](examples/pipelines/multiagent.yaml) | A2A — a peer agent as a tool | `… run examples/pipelines/multiagent.yaml --text "escalate this"` |

**Runtime-distinctive demos** — Pekko durability/recovery (`RecoveryDemo`) · Clojure Datomic
time-travel (`clojure -M:time-travel`).

<details>
<summary><b>Advanced — Flink-runtime showcases</b></summary>

These exercise **Flink-only** capabilities (CEP, side outputs, keyed-state vector memory, Kafka
streaming). Each has an inline `README.md`, a walkthrough under [`docs/examples/`](docs/examples/),
and a wrapper script under [`examples-bin/`](examples-bin/).

| Use case | Package | Flink-only capability | Run |
|----------|---------|-----------------------|-----|
| Customer-support triage | `example.triage` | guardrail + scorer over keyed state | `./examples-bin/run-support-triage.sh` |
| Real-time content moderation | `example.moderation` | OutputTag **side outputs** | `./examples-bin/run-moderation.sh` |
| RAG research assistant | `example.rag` | **Flink-state** keyed vector memory | `./examples-bin/run-rag.sh` |
| Anomaly + incident agent | `example.incident` | **Flink CEP** pattern matching | `./examples-bin/run-incident.sh` |
| Live research + RAG | `example.research` | crawler frontier as Flink operators | `./examples-bin/run-live-research.sh` |
| Markets (bond / crypto) | `example.markets` | **Kafka + Flink** streaming | `./examples-bin/run-bond-market.sh` |
| Quick start | `example.QuickStartExample` | minimal agent, one tool | `mvn -q exec:java -Dexec.mainClass=…QuickStartExample` |

</details>

Need a recipe rather than a full example? See [docs/cookbook.md](docs/cookbook.md).

</details>

<details>
<summary><b>Documentation index</b></summary>

| Document | Description |
|----------|-------------|
| [docs/examples/banking-everywhere.md](docs/examples/banking-everywhere.md) | **The same banking agent on every runtime** — one spec, the run command per runtime |
| [agentic-pekko/README.md](agentic-pekko/README.md) | **Agentic Pekko** *(first-class)* — event-sourced sharded actor runtime |
| [agentic-clj/README.md](agentic-clj/README.md) | **Agentic Clojure** *(first-class)* — pure Clojure on Datomic |
| [docs/portability/pekko.md](docs/portability/pekko.md) · [clojure.md](docs/portability/clojure.md) | Per-engine design notes for the two newest first-class runtimes |
| [docs/portability/pipelines.md](docs/portability/pipelines.md) | Declarative `pipeline.yaml` schema + loaders (Python/JVM/Go) |
| [docs/portability/parity-matrix.md](docs/portability/parity-matrix.md) | What each backend can do + limitations; three-core parity |
| [docs/portability/choosing-a-backend.md](docs/portability/choosing-a-backend.md) | Decision guide across Flink + 12 engines |
| [docs/portability/stream-stateful-core.md](docs/portability/stream-stateful-core.md) | The **stream-stateful core** — CEP · timers · windows · replay · suspend/resume · tracing, on every core |
| [docs/concepts.md](docs/concepts.md) | Core concepts — agents, events, tools, memory, the routed graph |
| [docs/configuration.md](docs/configuration.md) | Configuration reference (env vars, resolution order) |
| [docs/a2a.md](docs/a2a.md) | Agent-to-Agent (A2A) protocol — peer-as-tool, gateway, bridges |
| [docs/memory.md](docs/memory.md) | Flink-state-first memory model, vector memory, feeds |
| [docs/inference.md](docs/inference.md) | DL models as tools, guardrails, scorers, embedders |
| [docs/channels.md](docs/channels.md) | `Channel<T>` SPI: Kafka, Postgres CDC, Redis, webhook, tool transport |
| [docs/corpus.md](docs/corpus.md) | `Corpus` abstraction + three flavours |
| [docs/web-toolkit.md](docs/web-toolkit.md) | Jsoup + crawler-commons + Tika: robots-aware fetch + extract |
| [docs/python.md](docs/python.md) | Python API — JPype standalone + pointer to PyFlink-native |
| [docs/pyflink-integration.md](docs/pyflink-integration.md) | PyFlink-native: agent plan + CompileUtils + PEMJA |
| [docs/cookbook.md](docs/cookbook.md) | Short recipes for common SPI combinations |
| [docs/examples/](docs/examples/) | Long-form walkthroughs of the headline use cases |
| [docs/getting-started.md](docs/getting-started.md) | Setup guide and first steps |
| [docs/guides/context-management.md](docs/guides/context-management.md) | MoSCoW prioritization and compaction |
| [docs/guides/storage-quickstart.md](docs/guides/storage-quickstart.md) | Storage backend setup |
| [docs/guides/openai-setup.md](docs/guides/openai-setup.md) | Configuring OpenAI as the chat backend |
| [docs/guides/flink-agents-integration.md](docs/guides/flink-agents-integration.md) | Optional Apache Flink Agents bridge |
| [docs/reference/agent-framework.md](docs/reference/agent-framework.md) | Framework reference and agent patterns |
| [docs/reference/storage-architecture.md](docs/reference/storage-architecture.md) | Storage design |
| [docs/reference/troubleshooting.md](docs/reference/troubleshooting.md) | Common issues and fixes |

</details>

<details>
<summary><b>Relationship to Apache Flink Agents · In development</b></summary>

**Relationship to Apache Flink Agents.** Agentic Streaming predates upstream Apache Flink
Agents and stays compatible in *vocabulary* without a hard dependency. User-facing SPI
names (`ChatConnection`, `ChatSetup`, `Skill`, `OutputSchema`, `MemorySet`) mirror
upstream's, so a bridge stays thin. The optional `plugins/flintagents/` module (gated by
the `flink-agents` Maven profile) provides bidirectional adapters.

**In development.**
- Advanced CEP patterns for multi-agent coordination
- JMH benchmark suite for chat / embedding / vector-memory hot paths
- HNSW-backed `VectorMemorySpec` (JVector or Lucene) as a drop-in upgrade
- Native PyFlink port of the memory primitives
- Plugin refresh to upstream Flink Agents 0.3-SNAPSHOT

</details>

---

## Requirements

- **Java 17+** · **Maven 3.8+** — the Flink framework (Apache Flink 2.2, native FLIP-27/143) **and** Agentic Pekko (built separately after `mvn -f ports/jagentic-core/pom.xml install`)
- **Clojure CLI (tools.deps)** — only for Agentic Clojure under `agentic-clj/`
- **Go 1.24+** — only for the Go core / gateway / engines under `ports/go/`
- **Python 3.11+** — only for the pure-Python cores, ports, and the FastAPI gateway
- **Docker or Podman** — only for optional Postgres / Redis / Ollama / NATS services
- **Ollama** — for local LLM examples

## Contributing

Contributions welcome — open an issue or PR. Especially valued: additional
`ChatConnection`, `EmbeddingConnection`, `LongTermMemoryStore`, `VectorStore`,
`InferenceConnection`, and `Channel<T>` implementations.

## License

[Apache License 2.0](LICENSE).
