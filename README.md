# Agentic Flink

> **Heads up (2026-05-26):** The repo needed a sizeable cleanup pass with a big
> module reshuffle, and its git history was reset to a fresh root commit. If
> you cloned before this date your local copy will not fast-forward — please
> **delete it and clone fresh**.

A framework for building LLM-powered AI agents on Apache Flink. Memory lives in
Flink keyed state by default; LLM providers, embedders, tools, and observability
all sit behind small SPIs that ship with sane defaults and stay open for
hot-swap. The result is agent workflows that scale horizontally, survive job
restarts via Flink checkpoints, and don't lock you into any one LLM vendor or
storage backend.

## Key features

- **Flink-state-first memory** — short-term conversation memory is `ValueState`
  / `MapState` with `StateTtlConfig`; checkpoints provide durability with no
  external HOT tier required.
- **In-JVM vector memory** over Flink state — brute-force KNN or
  HNSW-over-Flink-state (`FlinkStateHnswVectorMemory`); SPI escape hatch for
  external HNSW backends.
- **Named, shareable corpora** — `Corpus` abstraction with three flavours
  (single-operator, broadcast, external) so ingest and retrieve operators
  can share the same vector index.
- **Unified `Channel<T>` SPI** — Kafka, Postgres CDC, Redis pub/sub, HTTP
  webhook, static seeds, and LLM-driven tool invocations all share one
  shape. Multiple channels union into the same operator's input.
- **Web toolkit** — Jsoup + crawler-commons + Apache Tika behind
  `WebFetchTool`, `CrawlerCore`, and `DocumentExtractor`. StormCrawler's
  capabilities without StormCrawler's Storm dep.
- **Postgres-default long-term storage** — conversation resumption and fact
  archive through `LongTermMemoryStore`; Redis remains an optional backend.
- **Chat-model SPI** — `ChatConnection` (vendor transport) split from
  `ChatSetup` (per-agent model + temperature + structured output). LangChain4J
  is the default implementation; not the API.
- **Embedder SPI** — `EmbeddingConnection` / `EmbeddingSetup` /
  `EmbeddingClient`; the default talks to a local Ollama service.
- **MCP support** — `tools/mcp/` wraps Model Context Protocol servers
  (stdio + HTTP/SSE transports) as ordinary `ToolExecutor`s.
- **Traditional DL models** — `inference/` SPI for classifiers, scorers,
  embedders, and generic models with DJL as the default backend (PyTorch /
  TensorFlow / ONNX / HuggingFace under one API). Slot them in as tools,
  guardrails, the relevancy scorer's backend, or standalone.
- **Structured output** — `OutputSchema<T>` infers JSON Schema from
  Lombok-annotated POJOs and parses LLM responses through Jackson.
- **ReAct agent** — `ReActProcessFunction` packages the canonical Thought /
  Action / Observation loop, bounded by `Agent.getMaxIterations()` and built on
  the chat-model SPI.
- **Skills** — bundle tools + system-prompt fragment + required facts under one
  named capability; `AgentBuilder.withSkill(...)` fans them out.
- **Listeners** — `AgentEventListener` SPI with nine lifecycle hooks; reference
  `LoggingAgentEventListener` and `MetricsAgentEventListener` ship in-box.
- **CEP-driven orchestration** — Flink CEP patterns drive validation,
  escalation, and saga compensation alongside the agent loop.
- **`@Tool` annotation discovery** — LangChain4J-annotated tools, MCP tools,
  and `ToolExecutor`s share one `ToolRegistry`.

## Quick start

```bash
git clone https://github.com/Ugbot/Agentic-Flink.git
cd Agentic-Flink

# Optional infrastructure. PostgreSQL is only needed for long-term resumption;
# the framework runs entirely on Flink state otherwise. Use `podman compose` if
# you run Podman rather than Docker.
docker compose up -d

# Pull a local LLM
docker compose exec ollama ollama pull qwen2.5:3b

# Run the full test suite (435 tests)
mvn clean test

# Run a working example
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.QuickStartExample"
```

### Python

The framework ships an [`agentic-flink` Python package](docs/python.md)
with **two paths**:

- **PyFlink-native** (recommended for streaming jobs): build an agent
  plan in Python, run it as a real Flink operator with Python callbacks
  invoked via PEMJA on the operator thread. See
  [`docs/pyflink-integration.md`](docs/pyflink-integration.md).
- **JPype standalone** (notebooks, scripts, services without PyFlink):
  in-process JVM via JPype — same threads, no serialization, no extra
  process.

```bash
pip install agentic-flink
```

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

Full guide: [`docs/python.md`](docs/python.md). Runnable examples under
`python/agentic_flink/examples/`.

## Building an agent

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

Every `with*` method is optional — defaults are discovered via `ServiceLoader`.
The minimum viable agent is `Agent.builder().withId(...).withSystemPrompt(...).build()`.

## Architecture

```
        Events (any Channel<T>: Kafka / Postgres / Redis / webhook / seed)
                        |
                        v
        +-------------------------------+
        |  Flink CEP pattern matching   |
        |  - validation triggers        |
        |  - escalation detection       |
        |  - compensation conditions    |
        +---------------+---------------+
                        |
                        v
        +-------------------------------+
        |  Agent loop                   |
        |  - ChatConnection (SPI)       |
        |  - ToolRegistry + MCP         |
        |  - ReAct / workflow / custom  |
        +---------------+---------------+
                        |
                        v
        +-------------------------------+
        |  Context management           |
        |  - MoSCoW 5-phase compaction  |
        |  - Embedder-driven relevancy  |
        +---------------+---------------+
                        |
                        v
        +-------------------------------+
        |  Memory                       |
        |  Short-term: Flink keyed      |
        |              state (+TTL)     |
        |  Vector:     Flink MapState   |
        |              brute-force KNN  |
        |  Long-term:  Postgres (opt.)  |
        +-------------------------------+
                        |
                        v
        +-------------------------------+
        |  Listeners (SPI)              |
        |  Logging / Metrics / custom   |
        +-------------------------------+
```

Short-term memory is Flink-state-first: checkpoints provide durability and TTL
runs incrementally inside the state backend. Long-term storage is opt-in and
used for conversation resumption across job lifetimes plus fact archival.

## Pluggable surfaces (SPI summary)

| Concern | Interface | Default | Discovery |
|---------|-----------|---------|-----------|
| Short-term memory | `memory.ShortTermMemorySpec` | `FlinkStateShortTermMemory` | `ServiceLoader` + builder |
| Vector memory | `memory.vector.VectorMemorySpec` | `FlinkStateVectorMemory` (brute-force) / `FlinkStateHnswVectorMemory` (HNSW) | Builder |
| Corpus | `corpus.CorpusSpec` | `SingleOperatorCorpus` / `BroadcastCorpus` / `ExternalCorpus` | Builder |
| Long-term store | `storage.LongTermMemoryStore` | `PostgresConversationStore` | `ServiceLoader` + factory |
| External vector store | `storage.VectorStore` | `PgVectorStore` (opt-in) | `ServiceLoader` + factory |
| Channel (continuous input) | `channel.Channel<T>` | `StaticSeedChannel`, `KafkaChannel<T>`, `WebhookChannel<T>`, `KafkaContextChannel`, `PostgresChangeChannel`, `RedisPubSubChannel`, `ToolInvocationChannel<T>` | Programmatic |
| Chat transport | `llm.ChatConnection` | `LangChain4jChatConnection` (Ollama) | `ServiceLoader` |
| Embedding transport | `embedding.EmbeddingConnection` | `OllamaEmbeddingConnection` / `DjlEmbeddingConnection` | `ServiceLoader` |
| MCP server | `tools.mcp.McpServerSpec` | none | Programmatic |
| Inference model | `inference.InferenceConnection` | `DjlInferenceConnection` (opt-in) | `ServiceLoader` + builder |
| Guardrail | `inference.Guardrail` | none | Programmatic |
| Web fetch | `web.WebFetchTool` / `web.CrawlerCore` | Jsoup + crawler-commons + Tika (opt-in) | Programmatic |
| Listener | `listener.AgentEventListener` | `LoggingAgentEventListener` | `ServiceLoader` |
| Tool | `tools.ToolExecutor` | (built-ins + `@Tool`) | `ToolRegistry` |

LangChain4J is the default chat backend but is wrapped behind the
`ChatConnection` SPI. Power users who need vendor-specific behaviour can
downcast to `LangChain4jChatClient` and call `getUnderlyingModel()` to reach the
underlying `dev.langchain4j.model.chat.ChatLanguageModel`.

## Examples

Runnable examples live under `src/main/java/org/agentic/flink/example/`.
Each headline use case has an inline `README.md` next to its source, a longer
walkthrough under [`docs/examples/`](docs/examples/), and a wrapper script
under [`examples-bin/`](examples-bin/) that boots prerequisites and runs the
example.

| Use case | Package | Demonstrates | Walkthrough | Run |
|----------|---------|--------------|-------------|-----|
| Customer-support triage | `example.triage` | Guardrail + InferenceToolAdapter + Scorer rerank + LangChain4J escape hatch | [doc](docs/examples/support-triage.md) | `./examples-bin/run-support-triage.sh` |
| Real-time content moderation | `example.moderation` | Classifier-as-hard-gate + Flink side outputs + listener audit (optional Kafka) | [doc](docs/examples/moderation.md) | `./examples-bin/run-moderation.sh` |
| RAG research assistant | `example.rag` | Sentence-transformers embedder + Flink-state vector memory + cross-encoder rerank | [doc](docs/examples/rag.md) | `./examples-bin/run-rag.sh` |
| Anomaly + CEP incident agent | `example.incident` | `GenericInferenceModel` + Flink CEP + runbook/ticket tools — LLM only on confirmed incidents | [doc](docs/examples/incident.md) | `./examples-bin/run-incident.sh` |
| Live research + RAG | `example.research` | Two-input job: crawler ingest (multi-source frontier, LLM can `crawl-url`) + RAG retrieve over HNSW corpus | [doc](docs/examples/live-research.md) | `./examples-bin/run-live-research.sh` |
| Quick start | `example.QuickStartExample` | Minimal agent with a single tool — start here | — | `mvn -q exec:java -Dexec.mainClass=…QuickStartExample` |
| Storage integration | `example.StorageIntegratedFlinkJob` | Full Flink-state + Postgres persistence | — | — |

Need a quick recipe rather than a full example? See [docs/cookbook.md](docs/cookbook.md).

## Documentation

| Document | Description |
|----------|-------------|
| [docs/memory.md](docs/memory.md) | Flink-state-first memory model, vector memory, feeds |
| [docs/inference.md](docs/inference.md) | Traditional DL models as tools, guardrails, scorers, embedders |
| [docs/channels.md](docs/channels.md) | `Channel<T>` SPI: Kafka, Postgres CDC, Redis pub/sub, webhook, LLM-tool transport |
| [docs/corpus.md](docs/corpus.md) | `Corpus` abstraction + three flavours (single-op, broadcast, external) |
| [docs/web-toolkit.md](docs/web-toolkit.md) | Jsoup + crawler-commons + Tika: robots-aware fetch + multi-format extract |
| [docs/python.md](docs/python.md) | Python API overview — JPype standalone + pointer to PyFlink-native |
| [docs/pyflink-integration.md](docs/pyflink-integration.md) | PyFlink-native: agent plan + CompileUtils + PEMJA |
| [docs/cookbook.md](docs/cookbook.md) | Short recipes for common SPI combinations |
| [docs/examples/](docs/examples/) | Long-form walkthroughs of the four headline use cases |
| [docs/pyflink.md](docs/pyflink.md) | Background notes on the state-primitive mapping |
| [docs/getting-started.md](docs/getting-started.md) | Setup guide and first steps |
| [docs/guides/context-management.md](docs/guides/context-management.md) | MoSCoW prioritization and compaction |
| [docs/guides/storage-quickstart.md](docs/guides/storage-quickstart.md) | Storage backend setup and integration |
| [docs/guides/openai-setup.md](docs/guides/openai-setup.md) | Configuring OpenAI as the chat backend |
| [docs/guides/flink-agents-integration.md](docs/guides/flink-agents-integration.md) | Optional Apache Flink Agents bridge |
| [docs/reference/agent-framework.md](docs/reference/agent-framework.md) | Framework reference and agent patterns |
| [docs/reference/storage-architecture.md](docs/reference/storage-architecture.md) | Storage design |
| [docs/reference/troubleshooting.md](docs/reference/troubleshooting.md) | Common issues and fixes |

## Relationship to Apache Flink Agents

Agentic Flink predates upstream Apache Flink Agents and stays compatible in
*vocabulary* without taking a hard dependency on it. The user-facing SPI names
(`ChatConnection`, `ChatSetup`, `Skill`, `OutputSchema`, `MemorySet`) mirror
upstream's terminology so an eventual bridge to Flink Agents 1.0 stays thin.
The optional `plugins/flintagents/` module — gated by the `flink-agents` Maven
profile — provides bidirectional adapters for users who want to co-host our
agents inside an upstream runtime.

## In development

- Advanced CEP patterns for multi-agent coordination
- JMH benchmark suite for chat / embedding / vector memory hot paths
- HNSW-backed `VectorMemorySpec` (JVector or Lucene) as a drop-in upgrade
- Native PyFlink port of the memory primitives (currently: drive Java
  operators from Python — see [docs/pyflink.md](docs/pyflink.md))
- Plugin refresh to upstream Flink Agents 0.3-SNAPSHOT

## Requirements

- Java 17+
- Maven 3.8+
- Apache Flink 1.20
- Docker or Podman (only for optional Postgres / Redis / Ollama services)
- Ollama (for local LLM examples)

## Contributing

Contributions welcome. Open an issue to report bugs, suggest features, or ask
questions. Pull requests appreciated — especially for additional
`ChatConnection`, `EmbeddingConnection`, `LongTermMemoryStore`, `VectorStore`,
`InferenceConnection`, and `Channel<T>` implementations.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
