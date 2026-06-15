# Agentic Flink

Standalone agentic framework for Apache Flink with LangChain4J integration.
Java 17 target, Flink 2.2.1, LangChain4J 1.16.3.

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

```
mvn clean test                        # unit tests
mvn test -P integration-tests         # integration tests (requires containers)
mvn clean package -P flink-agents     # build with optional Flink Agents plugin
```

The `plugins/flintagents/` directory is excluded from the default Maven compiler configuration.
Enable it with `-P flink-agents` after building Flink Agents from source.

## Key Patterns

- **AgentBuilder DSL**: `Agent.builder().withId(...).withSystemPrompt(...).withTools(...).build()`
- **StorageFactory**: `StorageFactory.createShortTermStore("redis", config)` -- factory for pluggable storage backends
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