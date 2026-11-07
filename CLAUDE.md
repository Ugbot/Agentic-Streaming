# Agentic Streaming (formerly Agentic Flink)

Experimental and pre-release: no published packages, and not safe for real payments or other
non-idempotent actions yet (see the status block in `README.md`).

Agentic Streaming is a portable spec and conformance suite for building agents as streaming, stateful,
**event-sourced** systems: an agent's state is a fold over an ordered log of events, with CQRS
(command = process a turn; query = read the view) and single-writer-per-conversation. The supported
runtimes are Apache Flink (Java, the primary one, this main module), plain Python and Apache Pekko,
built on the Java core, the Python core and the reference spec. The conformance bindings are
reference, jvm-core, flink, pekko and python. Everything else moved out of this repository; the code
as it stood is at tag `pre-cornerstone`.

This main module is the Flink adapter plus the deprecated legacy DSL. Java 21 target, Flink 2.2.1
(native FLIP-27 sources / FLIP-143 sinks), LangChain4J 1.16.3 (managed via the langchain4j-bom).

`ports/` holds the conformance tested cores (`ports/jagentic-core`, `ports/pyagentic`) and the
portable pipeline CLI (`ports/agentic-pipeline`, whose only backend is `local`; see
`ports/README.md`). The engine-agnostic "essence" and design notes are in `docs/portability/`.

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
  - `a2a/` -- Outbound A2A (Agent2Agent) support, used by the legacy DSL: `RemoteAgentSpec`, `A2AClient` SPI + `SdkA2AClient` (official a2a-java SDK, optional dep, isolated behind the SPI), `A2AToolExecutor` (peer-as-tool), `A2AStep` (explicit pipeline step), plus the `a2a/bridge/` and `a2a/storage/` packages. See `docs/a2a.md`.
  - `example/` -- Working examples (SimpleCalculatorTool, ToolAnnotationExample)

## Build

Use the committed wrapper (`./mvnw`); `reactor/pom.xml` (the parent of every module) enforces
Maven 3.9+ and Java 21, so a system `mvn` 3.6 fails. All first-class JVM modules share one
groupId (`org.jagentic`) and one version (`1.0.0-SNAPSHOT`).

```
./mvnw -f reactor/pom.xml verify         # whole reactor in dependency order (what CI gates on):
                                         # jagentic-core, Flink framework, agentic-pekko
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
```

CI (`.github/workflows/ci.yml`) runs the
reactor once with the backing services (Postgres, Redis, Kafka, Qdrant, a Python with the MCP
SDK) and audits every skipped test against `tools/ci/skip-allowlist.txt`; a service-backed test
that skips in CI fails the build. `ZeroMqChannelTest` (no timeout, hangs on some hosts) is excluded
there by name.

### Agentic Pekko (part of the reactor; on their own after `./mvnw -f ports/jagentic-core/pom.xml install -DskipTests`)

```
# Agentic Pekko (actors); `compile` must be in the same invocation as exec:java
./mvnw -f agentic-pekko/pom.xml test
./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
  -Dexec.args="examples/pipelines/banking.yaml --text 'what is my balance?'"   # any spec on the actor runtime
./mvnw -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.RecoveryDemo  # durability/recovery
```

The cross-runtime parity story (one `pipeline.yaml`, each runtime) is documented in
`docs/examples/banking-everywhere.md`; the Flink-only examples under `docs/examples/` are labelled
"Flink-runtime showcase".

## Key Patterns

- **AgentBuilder DSL** (legacy and deprecated, outside the spec): `Agent.builder().withId(...).withSystemPrompt(...).withTools(...).withConversationStore(...).build()`. Every public `withX` method is read by an operator: identity and prompt (`withId`, `withName`, `withDescription`, `withType`, `withSystemPrompt`, `withSkill`), chat (`withChatConnection`, `withChatSetup`, `withOutputSchema`), tools (`withTools`, `withRequiredTools`, `withToolDefaults`, `withToolTimeout`, `withMcpServer`, `withRemoteAgent`, `withA2AClientFactory`, `withInferenceTool`), budgets (`withMaxIterations`, `withTimeout`, `withMaxValidationAttempts`, `withMaxCorrectionAttempts`, `withStateMachine`), sagas (`withCompensationEnabled`, `withCompensatingTool`), memory (`withShortTermTtl` drives the TTL of the keyed dedup state in `TurnDispatchDedupFunction`, `withConversationStore`, `withLongTermStore`), and interceptors (`withGuardrail`, `withListener`). `src/test/java/org/agentic/flink/dsl/AgentBuilderTest` asserts each one; add a test there before adding a builder method. The job graph is `AgentTurnDispatcher` (CEP match, stateless) -> `TurnDispatchDedupFunction` (keyed; drops a turn redelivered after a restore) -> `stream.AgentExecutionFunction` (async LLM/tool loop) -> `stream.CompensationFunction` (only when compensation is enabled; runs the registered compensating tools in reverse call order) -> `AgentResultRouter`.
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
- Docker or Podman: scripts pick the running engine through `tools/container-engine.sh`
- All storage providers must be Serializable (Flink distributes across cluster)
- Mark non-serializable fields as transient and reinitialize in initialize()
- Tests use JUnit 5 with randomized data, not hardcoded values