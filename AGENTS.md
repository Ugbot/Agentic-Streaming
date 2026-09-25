# Agentic Flink

Standalone agentic framework for Apache Flink with LangChain4J integration.
Java 21 target, Flink 2.2.1, LangChain4J 1.16.3.

## Project Structure

- `src/main/java/org/agentic/flink/` -- main source root
  - `config/` -- AgenticFlinkConfig, ConfigKeys (env-var-based configuration)
  - `core/` -- ToolDefinition, shared model classes
  - `dsl/` -- AgentBuilder fluent DSL for defining agents
  - `tools/` -- ToolExecutor interface, AbstractToolExecutor, built-in tools
  - `tool/` -- ToolRegistry (central tool registry with builder)
  - `langchain/` -- ToolAnnotationRegistry, LangChainToolAdapter (@Tool bridge)
  - `storage/` -- Multi-tier storage: StorageProvider, StorageFactory, ShortTermMemoryStore, LongTermMemoryStore
  - `storage/memory/` -- InMemoryShortTermStore, InMemoryLongTermStore (reference implementations)
  - `storage/redis/` -- Redis-backed storage
  - `storage/postgres/` -- PostgreSQL-backed storage
  - `context/` -- Context management (ContextItem, ContextWindowManager)
  - `statemachine/` -- AgentStateMachine for workflow state transitions
  - `cep/` -- Flink CEP integration for event-driven patterns
  - `compensation/` -- Saga compensation/rollback support
  - `example/` -- Working examples (SimpleCalculatorTool, ToolAnnotationExample)
  - `plugins/flintagents/` -- Optional Apache Flink Agents integration (excluded from default build)

## Build

Use the committed wrapper. `reactor/pom.xml` is the parent and aggregator of every first-class
JVM module (`org.jagentic`, one version); it enforces Maven 3.9+ and Java 21, so a system
`mvn` 3.6 fails. The root pom stays the Flink framework module.

```
./mvnw -f reactor/pom.xml verify         # every module in dependency order (what CI gates on)
./mvnw -f reactor/pom.xml verify -P a2a-gateway   # plus the Quarkus A2A gateway (opt-in)
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests   # first, when building one module at a time
./mvnw clean test                        # unit tests (Flink framework)
./mvnw test -P integration-tests         # Testcontainers suites; with Podman set DOCKER_HOST to the
                                         # podman socket and TESTCONTAINERS_RYUK_DISABLED=true
./mvnw clean package -P flink-agents     # build with optional Flink Agents plugin
```

`ports/experimental/*` stay outside the reactor. CI audits skipped tests against
`tools/ci/skip-allowlist.txt`; a test that skips because a service is missing fails the build.

The `plugins/flintagents/` directory is excluded from the default Maven compiler configuration.
Enable it with `-P flink-agents` after building Flink Agents from source.

## Key Patterns

- **AgentBuilder DSL**: `Agent.builder().withId(...).withSystemPrompt(...).withTools(...).build()`
- **StorageFactory**: `StorageFactory.createLongTermStore("postgres", config)` -- factory for long-term backends (`memory`, `postgres`, `postgresql`). `createShortTermStore` accepts only `"memory"` and throws for anything else; short-term memory is Flink state (`FlinkStateShortTermMemory`)
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