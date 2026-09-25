# Agentic Streaming (formerly Agentic Flink)

Agentic Streaming is a library + example pack for building agents as streaming,
stateful, **event-sourced** systems, an agent's state is a materialized view over an
ordered log of events, with CQRS (command = process a turn; query = read the view) and
single-writer-per-conversation. Apache Flink is the **first-class runtime** (this main
module). The same `agentic/v1` spec is conformance tested on the bindings listed in the
generated `docs/capabilities.md` (reference, jvm-core, flink, pekko, clojure, python, pyflink,
python-jvm, python-flink). The engine adapters under `ports/experimental/` are experimental:
they predate agentic/v1, run the banking example, are not conformance tested, and may be
removed. Design docs are under `docs/portability/`.

This main module is the Flink framework: a standalone agentic framework for Apache Flink
with LangChain4J integration. Java 21 target, Flink 2.2.1 (native FLIP-27 sources /
FLIP-143 sinks), LangChain4J 1.16.3 (managed via the langchain4j-bom). PyFlink path targets
apache-flink 2.x (see docs/python.md).

`ports/` holds the conformance tested cores (`ports/jagentic-core`, `ports/pyagentic`) and the
portable pipeline CLI (`ports/agentic-pipeline`); the multi-engine adapters + gateways live under
`ports/experimental/` (see `ports/experimental/README.md`, and `ports/README.md` for the
comparison). The engine-agnostic "essence" and per-engine design notes are in `docs/portability/`.

## Project Structure

- `src/main/java/org/agentic/flink/` -- main source root
  - `config/` -- AgenticFlinkConfig, ConfigKeys (env-var-based configuration)
  - `core/` -- ToolDefinition, shared model classes
  - `dsl/` -- AgentBuilder fluent DSL for defining agents
  - `tools/` -- ToolExecutor interface, AbstractToolExecutor, built-in tools
  - `tool/` -- ToolRegistry (central tool registry with builder)
  - `langchain/` -- ToolAnnotationRegistry, LangChainToolAdapter (@Tool bridge)
  - `memory/` -- Flink-state-first short-term memory (ShortTermMemory, FlinkStateShortTermMemory), the **default** for in-job memory
  - `memory/conversation/` -- **Per-conversation memory shared across operators**: `ConversationStore` SPI (multi-turn ChatMessage transcript + scalar workflow attributes, keyed by conversationId, also indexable by userId). `InMemoryConversationStore` (default, process-wide `shared()` singleton for embedded single-JVM; bounded transcript); `ConversationStores.discover()` (ServiceLoader → else shared in-JVM). This is the layer between per-operator short-term Flink state and the long-term store, what a routed graph (router→path→verifier) needs to progress across turns. Wired via `AgentBuilder.withConversationStore(...)`.
  - `memory/vector/` -- In-JVM vector memory backed by Flink state (FlinkStateVectorMemory, brute-force KNN); external VectorStore SPI for HNSW backends
  - (memory feeds moved to `channel/`, see below; `Channel<KeyedContextItem>` replaces the old MemoryFeed)
  - `storage/` -- Long-term store SPI (LongTermMemoryStore) for resumption + archival; ServiceLoader-based discovery
  - `storage/memory/` -- InMemoryLongTermStore for tests/dev; legacy InMemoryShortTermStore (deprecated path)
  - `storage/postgres/` -- PostgresConversationStore (production default for long-term)
  - `storage/redis/` -- RedisConversationStore, optional, no longer default; Jedis dep is optional
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
- `a2a-gateway/` -- Optional standalone Quarkus module: inbound A2A gateway (Agent Card + JSON-RPC, with `message/stream` over SSE; no gRPC or REST server is implemented) bridging external A2A callers into a Flink job. Built on its own (`./mvnw -f a2a-gateway/pom.xml package`) or with `./mvnw -f reactor/pom.xml verify -P a2a-gateway`; not part of the default reactor build. See `a2a-gateway/README.md`.
- `python/` -- JPype-backed Python facade (`agentic-flink` PyPI package). In-process JVM, `@tool` decorator for Python functions, examples mirror the Java ones. See `docs/python.md`.

## Build

Use the committed wrapper (`./mvnw`); `reactor/pom.xml` (the parent of every module) enforces
Maven 3.9+ and Java 21, so a system `mvn` 3.6 fails. All first-class JVM modules share one
groupId (`org.jagentic`) and one version (`1.0.0-SNAPSHOT`).

```
./mvnw -f reactor/pom.xml verify         # whole reactor in dependency order (what CI gates on):
                                         # jagentic-core, Flink framework, agentic-pekko, pyflink/java,
                                         # tool-services-packs, tool-services-app, banking-job
./mvnw -f reactor/pom.xml verify -P a2a-gateway   # plus the Quarkus A2A gateway (slow, opt-in)
./mvnw -f reactor/pom.xml install -DskipTests     # compile + install everything, no tests
```

The root `pom.xml` stays the Flink framework module, so the per-module commands still work.
A module built on its own needs its upstream modules in the local repository; installing
`ports/jagentic-core` also installs the reactor parent pom the modules refer to.

```
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests   # first, when building modules one at a time
./mvnw clean test                        # unit tests (Flink framework)
./mvnw test -P integration-tests         # Testcontainers suites; with Podman export
                                         # DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
                                         # and TESTCONTAINERS_RYUK_DISABLED=true (docs/getting-started.md)
./mvnw clean package -P flink-agents     # build with optional Flink Agents plugin
./mvnw install -DskipTests && ./mvnw -f a2a-gateway/pom.xml package   # build the A2A gateway
```

`ports/experimental/*` are not modules of the reactor. CI (`.github/workflows/ci.yml`) runs the
reactor once with the backing services (Postgres, Redis, Kafka, Qdrant, a Python with the MCP
SDK) and audits every skipped test against `tools/ci/skip-allowlist.txt`; a service-backed test
that skips in CI fails the build. `PythonExecutorTest` (PEMJA has no CPython 3.12 wheel) and
`ZeroMqChannelTest` (no timeout, hangs on some hosts) are excluded there by name.

The `plugins/flintagents/` directory is excluded from the default Maven compiler configuration.
Enable it with `-P flink-agents` after building Flink Agents from source.

### Other first-class runtimes (part of the reactor; on their own after `./mvnw -f ports/jagentic-core/pom.xml install -DskipTests`)

```
# Agentic Pekko (actors); `compile` must be in the same invocation as exec:java
./mvnw -f agentic-pekko/pom.xml test
./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"   # any spec on the actor runtime
./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo  # durability/recovery

# Agentic Clojure (Datomic) - in agentic-clj/
clojure -X:test          # full suite
clojure -M:run           # banking demo
clojure -M:time-travel   # Datomic transcript time-travel (as-of)
```

The cross-runtime parity story (one `pipeline.yaml`, every runtime) is documented in
`docs/examples/banking-everywhere.md`; the Flink-only examples under `docs/examples/` are labelled
"Flink-runtime showcase".

## Key Patterns

- **AgentBuilder DSL**: `Agent.builder().withId(...).withSystemPrompt(...).withTools(...).withConversationStore(...).build()`. Four builder methods (`withShortTermTtl`, `withVectorMemory`, `withLongTermStore`, `withMemoryChannel`) store a value on `Agent` that no operator in this repository reads; do not document them as working until they are wired or removed.
- **Flink-state-first memory**: Short-term memory is `FlinkStateShortTermMemory`, built in `RichFunction.open()` from `ShortTermMemorySpec`. No external HOT tier required.
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