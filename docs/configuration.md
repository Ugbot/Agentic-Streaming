# Configuration Reference

All configuration in Agentic Flink is managed through `AgenticFlinkConfig`, which resolves values from a priority chain: explicit properties, environment variables, system properties, and built-in defaults.

## Resolution Order

When you request a configuration key (e.g., `ollama.base.url`), the value is resolved in this order (highest priority first):

1. **Explicit properties** -- values passed via `AgenticFlinkConfig.fromMap(props)` or the constructor.
2. **Environment variables** -- the key is transformed to the form `AGENTIC_FLINK_OLLAMA_BASE_URL` (uppercased, dots replaced with underscores, prefixed with `AGENTIC_FLINK_`).
3. **System properties** -- the key is prefixed with `agentic.flink.` (e.g., `agentic.flink.ollama.base.url`).
4. **Default values** -- hard-coded in `ConfigKeys`.

The `forTesting()` factory skips steps 2 and 3, returning only defaults and explicit properties. This isolates tests from host environment variables.

## Environment Variable Naming Convention

Configuration keys use dot-separated lowercase notation. The corresponding environment variable is derived by:

1. Uppercasing the key
2. Replacing dots with underscores
3. Prepending `AGENTIC_FLINK_`

| Config Key | Environment Variable |
|---|---|
| `ollama.base.url` | `AGENTIC_FLINK_OLLAMA_BASE_URL` |
| `ollama.model` | `AGENTIC_FLINK_OLLAMA_MODEL` |
| `redis.host` | `AGENTIC_FLINK_REDIS_HOST` |
| `redis.port` | `AGENTIC_FLINK_REDIS_PORT` |
| `redis.password` | `AGENTIC_FLINK_REDIS_PASSWORD` |
| `postgres.url` | `AGENTIC_FLINK_POSTGRES_URL` |
| `postgres.user` | `AGENTIC_FLINK_POSTGRES_USER` |
| `postgres.password` | `AGENTIC_FLINK_POSTGRES_PASSWORD` |
| `qdrant.host` | `AGENTIC_FLINK_QDRANT_HOST` |
| `qdrant.port` | `AGENTIC_FLINK_QDRANT_PORT` |
| `openai.api.key` | `AGENTIC_FLINK_OPENAI_API_KEY` |
| `openai.model` | `AGENTIC_FLINK_OPENAI_MODEL` |

System properties use the prefix `agentic.flink.` followed by the key verbatim (e.g., `-Dagentic.flink.ollama.base.url=http://my-ollama:11434`).

## Configuration Keys

### Ollama (Local LLM)

| Key | Default | Description |
|---|---|---|
| `ollama.base.url` | `http://localhost:11434` | Base URL of the Ollama API server |
| `ollama.model` | `qwen2.5:3b` | Model name for Ollama inference |

### Redis

| Key | Default | Description |
|---|---|---|
| `redis.host` | `localhost` | Redis server hostname |
| `redis.port` | `6379` | Redis server port |
| `redis.password` | _(none)_ | Redis authentication password. No default; omit for unauthenticated connections |

### PostgreSQL

| Key | Default | Description |
|---|---|---|
| `postgres.url` | `jdbc:postgresql://localhost:5432/agentic_flink` | JDBC connection URL |
| `postgres.user` | `flink_user` | Database username |
| `postgres.password` | `flink_password` | Database password |

### Qdrant (Vector Database)

| Key | Default | Description |
|---|---|---|
| `qdrant.host` | `localhost` | Qdrant server hostname |
| `qdrant.port` | `6333` | Qdrant gRPC port |

### OpenAI (Optional)

| Key | Default | Description |
|---|---|---|
| `openai.api.key` | _(none)_ | OpenAI API key. No default; required only if using OpenAI models |
| `openai.model` | `gpt-4` | OpenAI model name |

## Creating a Configuration Instance

### Production (reads environment)

```java
AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();
String ollamaUrl = config.get(ConfigKeys.OLLAMA_BASE_URL, ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
String model = config.get(ConfigKeys.OLLAMA_MODEL);
int redisPort = config.getInt(ConfigKeys.REDIS_PORT, 6379);
```

`fromEnvironment()` resolves all four levels of the priority chain.

### Programmatic (explicit overrides)

```java
Map<String, String> props = new HashMap<>();
props.put(ConfigKeys.OLLAMA_BASE_URL, "http://my-ollama:11434");
props.put(ConfigKeys.OLLAMA_MODEL, "qwen2.5:7b");
props.put(ConfigKeys.REDIS_HOST, "redis.internal");

AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
```

Explicit properties take highest priority, but environment variables and system properties still serve as fallback for keys not present in the map.

### Testing (isolated from host)

```java
AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
```

`forTesting()` does not read environment variables or system properties. Only built-in defaults are returned, ensuring tests are deterministic regardless of the host environment.

You can also combine testing mode with explicit overrides:

```java
// forTesting() does not accept a map, but you can use fromMap() with
// resolveEnv=false by constructing directly if needed.
// The simplest pattern is to rely on forTesting() defaults or set env vars
// in the test harness.
AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
// Defaults will be used for all keys
assertEquals("http://localhost:11434", config.get(ConfigKeys.OLLAMA_BASE_URL));
assertEquals("qwen2.5:3b", config.get(ConfigKeys.OLLAMA_MODEL));
assertEquals("localhost", config.get(ConfigKeys.REDIS_HOST));
```

### Exporting as a Map

Some subsystems (e.g., `StorageFactory`) accept `Map<String, String>`. Use `toMap()` to export all resolved values:

```java
AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();
Map<String, String> allValues = config.toMap();

ShortTermMemoryStore store = StorageFactory.createShortTermStore("redis", allValues);
```

The returned map contains every key that has a resolved non-null value, combining explicit properties, environment overrides, and defaults.

## Accessor Methods

| Method | Return Type | Behavior |
|---|---|---|
| `get(key)` | `String` | Returns resolved value or `null` |
| `get(key, defaultValue)` | `String` | Returns resolved value or the provided default |
| `getInt(key, defaultValue)` | `int` | Parses as integer; returns default on missing or unparseable values |
| `toMap()` | `Map<String, String>` | Exports all resolved key-value pairs as an unmodifiable map |

## Example .env File

For local development, set environment variables in a `.env` file (loaded by your shell or process runner):

```bash
# Ollama (local LLM)
AGENTIC_FLINK_OLLAMA_BASE_URL=http://localhost:11434
AGENTIC_FLINK_OLLAMA_MODEL=qwen2.5:3b

# Redis
AGENTIC_FLINK_REDIS_HOST=localhost
AGENTIC_FLINK_REDIS_PORT=6379
# AGENTIC_FLINK_REDIS_PASSWORD=

# PostgreSQL
AGENTIC_FLINK_POSTGRES_URL=jdbc:postgresql://localhost:5432/agentic_flink
AGENTIC_FLINK_POSTGRES_USER=flink_user
AGENTIC_FLINK_POSTGRES_PASSWORD=flink_password

# Qdrant
AGENTIC_FLINK_QDRANT_HOST=localhost
AGENTIC_FLINK_QDRANT_PORT=6333

# OpenAI (optional -- only if using OpenAI models)
# AGENTIC_FLINK_OPENAI_API_KEY=sk-...
# AGENTIC_FLINK_OPENAI_MODEL=gpt-4
```

## File Locations

- `ConfigKeys` constants: `src/main/java/org/agentic/flink/config/ConfigKeys.java`
- `AgenticFlinkConfig` class: `src/main/java/org/agentic/flink/config/AgenticFlinkConfig.java`
