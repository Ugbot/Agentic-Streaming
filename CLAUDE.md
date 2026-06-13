# Agentic Flink

Standalone agentic framework for Apache Flink with LangChain4J integration.
Java 17 target, Flink 1.20.1, LangChain4J 0.35.0.

## Project Structure

- `src/main/java/org/agentic/flink/` -- main source root
  - `config/` -- AgenticFlinkConfig, ConfigKeys (env-var-based configuration)
  - `core/` -- ToolDefinition, shared model classes
  - `dsl/` -- AgentBuilder fluent DSL for defining agents
  - `tools/` -- ToolExecutor interface, AbstractToolExecutor, built-in tools
  - `tool/` -- ToolRegistry (central tool registry with builder)
  - `langchain/` -- ToolAnnotationRegistry, LangChainToolAdapter (@Tool bridge)
  - `memory/` -- Flink-state-first short-term memory (ShortTermMemory, FlinkStateShortTermMemory) — the **default** for in-job memory
  - `memory/conversation/` -- **Per-conversation memory shared across operators**: `ConversationStore` SPI (multi-turn ChatMessage transcript + scalar workflow attributes, keyed by conversationId, also indexable by userId). `InMemoryConversationStore` (default, process-wide `shared()` singleton for embedded single-JVM; bounded transcript); `ConversationStores.discover()` (ServiceLoader → else shared in-JVM). This is the layer between per-operator short-term Flink state and the long-term store — what a routed graph (router→path→verifier) needs to progress across turns. Wired via `AgentBuilder.withConversationStore(...)`.
  - `memory/vector/` -- In-JVM vector memory backed by Flink state (FlinkStateVectorMemory, brute-force KNN); external VectorStore SPI for HNSW backends
  - (memory feeds moved to `channel/` — see below; `Channel<KeyedContextItem>` replaces the old MemoryFeed)
  - `storage/` -- Long-term store SPI (LongTermMemoryStore) for resumption + archival; ServiceLoader-based discovery
  - `storage/memory/` -- InMemoryLongTermStore for tests/dev; legacy InMemoryShortTermStore (deprecated path)
  - `storage/postgres/` -- PostgresConversationStore (production default for long-term)
  - `storage/redis/` -- RedisConversationStore — optional, no longer default; Jedis dep is optional
  - `context/` -- Context management (ContextItem, ContextWindowManager)
  - `statemachine/` -- AgentStateMachine for workflow state transitions
  - `inference/` -- Traditional DL model SPI (Classifier, Scorer, EmbeddingClient, GenericInferenceModel) + guardrails + tool adapter; DJL is the default optional backend
  - `channel/` -- `Channel<T>` SPI: Kafka/Postgres/Redis/Webhook/StaticSeed + `ToolInvocationChannel` (side-output / Kafka / in-JVM transports)
  - `corpus/` -- `Corpus` abstraction + three flavours: SingleOperatorCorpus, BroadcastCorpus, ExternalCorpus
  - `web/` -- Optional toolkit: Jsoup + crawler-commons (robots.txt) + Tika (PDF/DOC/PPT/EPUB/HTML), plus WebFetchTool, ExtractLinksTool, CrawlerCore
  - `ingest/` -- Chunker SPI + RecursiveTextChunker + IngestionPipeline builder
  - `retrieve/` -- RetrievalPipeline builder (embed → search → rerank → answer)
  - `storage/vector/` -- pgvector implementation of VectorStore (Qdrant via the SPI path)
  - `cep/` -- Flink CEP integration for event-driven patterns
  - `compensation/` -- Saga compensation/rollback support
  - `a2a/` -- A2A (Agent2Agent) protocol support. Outbound: `RemoteAgentSpec`, `A2AClient` SPI + `SdkA2AClient` (official a2a-java SDK, optional dep, isolated behind the SPI), `A2AToolExecutor` (peer-as-tool), `A2AStep` (explicit pipeline step). `a2a/bridge/` -- pluggable gateway↔Flink transport (`inproc`/`zeromq`/`redis`). `a2a/storage/` -- `A2ATaskStore` (memory/postgres/redis). See `docs/a2a.md`.
  - `example/` -- Working examples (SimpleCalculatorTool, ToolAnnotationExample)
  - `plugins/flintagents/` -- Optional Apache Flink Agents integration (excluded from default build)
- `a2a-gateway/` -- Optional standalone Quarkus module: inbound A2A gateway (Agent Card + JSON-RPC/SSE + gRPC + REST) bridging external A2A callers into a Flink job. Built separately (`mvn -f a2a-gateway/pom.xml package`), kept out of the core reactor. See `a2a-gateway/README.md`.
- `python/` -- JPype-backed Python facade (`agentic-flink` PyPI package). In-process JVM, `@tool` decorator for Python functions, examples mirror the Java ones. See `docs/python.md`.

## Build

```
mvn clean test                        # unit tests
mvn test -P integration-tests         # integration tests (requires containers)
mvn clean package -P flink-agents     # build with optional Flink Agents plugin
mvn install -DskipTests && mvn -f a2a-gateway/pom.xml package   # build the A2A gateway
```

The `plugins/flintagents/` directory is excluded from the default Maven compiler configuration.
Enable it with `-P flink-agents` after building Flink Agents from source.

## Key Patterns

- **AgentBuilder DSL**: `Agent.builder().withId(...).withSystemPrompt(...).withTools(...).withShortTermTtl(Duration.ofMinutes(30)).withLongTermStore(...).withMemoryChannel(...).withVectorMemory(FlinkStateHnswVectorMemory.spec(768)).build()`
- **Flink-state-first memory**: Short-term memory is `FlinkStateShortTermMemory` — built in `RichFunction.open()` from `ShortTermMemorySpec`. No external HOT tier required.
- **LongTermMemoryStore** (optional): Postgres default, Redis optional, ServiceLoader-discovered. Used only for conversation resumption + fact archive. Write-behind from Flink state.
- **ToolExecutor interface**: Async tool execution via `CompletableFuture<Object> execute(Map<String, Object>)`
- **@Tool annotations**: LangChain4J annotation-based tool discovery via ToolAnnotationRegistry
- **ToolRegistry**: `ToolRegistry.builder().registerTool(name, executor).build()` -- central tool registration

## Configuration

Config resolution order: explicit properties > environment variables (AGENTIC_FLINK_ prefix) > system properties (agentic.flink. prefix) > defaults.
See `docs/configuration.md` for the full reference.

## Conventions

- No TODOs, no placeholders -- implement working code or a working subset
- Uses Podman, not Docker
- All storage providers must be Serializable (Flink distributes across cluster)
- Mark non-serializable fields as transient and reinitialize in initialize()
- Tests use JUnit 5 with randomized data, not hardcoded values