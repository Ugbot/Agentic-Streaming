<div align="center">

# Agentic Streaming

**Build resilient pipelines of stateful agents that act on continuous streams of data —
chain them with almost any function call, reach almost any data system, and deploy the
same agent on Flink or a dozen other engines.**

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Languages](https://img.shields.io/badge/languages-Python%20·%20JVM%20·%20Go-informational.svg)](#whats-in-the-box)
[![Backends](https://img.shields.io/badge/backends-Flink%20%2B%2012%20engines-success.svg)](docs/portability/parity-matrix.md)
[![Build once](https://img.shields.io/badge/build%20once-deploy%20anywhere-orange.svg)](docs/portability/pipelines.md)

[What you can build](#what-you-can-build) ·
[Quick start](#quick-start) ·
[Deploy anywhere](#deploy-anywhere) ·
[Architecture](#architecture) ·
[Docs](#reference)

<sub>Formerly **Agentic Flink**. It began as an agent framework *for* Apache Flink and outgrew the name —
the same essence now runs across Python, the JVM, and Go. Flink is still the first-class, most
feature-complete runtime; it's just no longer the only one.</sub>

</div>

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
| 🔌 **Reach almost any data system** | memory, vectors, and long-term storage are SPIs — Postgres · Redis/Valkey · Fluss · pgvector/Qdrant · NATS KV — chosen by a connection link and **hot-swappable** without touching agent code |
| 📦 **Build once, deploy anywhere** | define the whole agent in a [`pipeline.yaml`](docs/portability/pipelines.md) and run the *same* spec on Flink or a dozen other backends (Python / JVM / Go) |

> **The through-line:** resilient pipelines of agents that act on almost any data, chain
> with almost any function call, and reach almost any data system — with the correctness
> guarantees the underlying engine can give.

---

## Quick start

**Run a declarative agent in 30 seconds** (model-free, no infra) — the same `pipeline.yaml`
on three different backends, changing only `--backend`:

```bash
git clone https://github.com/Ugbot/Agentic-Streaming.git && cd Agentic-Streaming

python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend celery --text "card types?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend nats   --text "what is my balance?"
```

**Run the full Flink framework** (the first-class runtime):

```bash
# Optional infra (Ollama/Postgres/Redis). Use `podman compose` if you run Podman.
docker compose up -d
docker compose exec ollama ollama pull qwen2.5:3b

mvn clean test                                                    # the test suite
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.QuickStartExample"
```

<details>
<summary><b>Build an agent — Java DSL</b></summary>

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
| **Portability pack** | the same essence on **12 engines** across **3 pure cores** (`pyagentic` / `jagentic-core` / `goagentic`) + 2 HTTP gateways; a new tool/path in a core propagates to every port | [`ports/`](ports/) |
| **Declarative pipelines** | one `pipeline.yaml` → any backend; loaders in Python, JVM, and Go | [`pipelines.md`](docs/portability/pipelines.md) |
| **Design docs** | per-engine mapping, parity matrix, choosing-a-backend | [`docs/portability/`](docs/portability/) |

---

## Architecture

<details open>
<summary><b>The agent loop (Flink runtime)</b></summary>

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
<summary><b>Key features (Flink framework)</b></summary>

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

Runnable examples live under `src/main/java/org/agentic/flink/example/` — each headline use
case has an inline `README.md`, a walkthrough under [`docs/examples/`](docs/examples/), and a
wrapper script under [`examples-bin/`](examples-bin/).

| Use case | Package | Demonstrates | Run |
|----------|---------|--------------|-----|
| Customer-support triage | `example.triage` | Guardrail + InferenceToolAdapter + Scorer rerank | `./examples-bin/run-support-triage.sh` |
| Real-time content moderation | `example.moderation` | Classifier hard-gate + side outputs + listener audit | `./examples-bin/run-moderation.sh` |
| RAG research assistant | `example.rag` | Sentence-transformers embedder + Flink-state vector memory + rerank | `./examples-bin/run-rag.sh` |
| Anomaly + CEP incident agent | `example.incident` | `GenericInferenceModel` + Flink CEP + runbook/ticket tools | `./examples-bin/run-incident.sh` |
| Live research + RAG | `example.research` | Crawler ingest (frontier, LLM `crawl-url`) + RAG over HNSW corpus | `./examples-bin/run-live-research.sh` |
| Quick start | `example.QuickStartExample` | Minimal agent with one tool — start here | `mvn -q exec:java -Dexec.mainClass=…QuickStartExample` |

Need a recipe rather than a full example? See [docs/cookbook.md](docs/cookbook.md).

</details>

<details>
<summary><b>Documentation index</b></summary>

| Document | Description |
|----------|-------------|
| [docs/portability/pipelines.md](docs/portability/pipelines.md) | Declarative `pipeline.yaml` schema + loaders (Python/JVM/Go) |
| [docs/portability/parity-matrix.md](docs/portability/parity-matrix.md) | What each backend can do + limitations; three-core parity |
| [docs/portability/choosing-a-backend.md](docs/portability/choosing-a-backend.md) | Decision guide across Flink + 12 engines |
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

- **Java 17+** · **Maven 3.8+** · **Apache Flink 2.2** (native FLIP-27 sources / FLIP-143 sinks) — for the first-class runtime
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
