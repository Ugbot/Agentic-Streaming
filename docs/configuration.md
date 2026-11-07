# Configuration Reference

An agent is configured by its workflow document ([`pipeline.yaml`](portability/pipelines.md)). What the Flink job itself reads is its checkpoint settings, through `AgenticFlinkConfig`, which resolves values from a priority chain: explicit properties, environment variables, system properties, and built-in defaults.

## Resolution Order

When you request a configuration key (e.g., `checkpoint.interval.ms`), the value is resolved in this order (highest priority first):

1. **Explicit properties** -- values passed via `AgenticFlinkConfig.fromMap(props)` or the constructor.
2. **Environment variables** -- the key is transformed to the form `AGENTIC_FLINK_CHECKPOINT_INTERVAL_MS` (uppercased, dots replaced with underscores, prefixed with `AGENTIC_FLINK_`).
3. **System properties** -- the key is prefixed with `agentic.flink.` (e.g., `agentic.flink.checkpoint.interval.ms`).
4. **Default values** -- hard-coded in `ConfigKeys`.

The `forTesting()` factory skips steps 2 and 3, returning only defaults and explicit properties. This isolates tests from host environment variables.

## Environment Variable Naming Convention

Configuration keys use dot-separated lowercase notation. The corresponding environment variable is derived by:

1. Uppercasing the key
2. Replacing dots with underscores
3. Prepending `AGENTIC_FLINK_`

<!-- env-vars: ConfigKeys -->
| Config Key | Environment Variable |
|---|---|
| `checkpoint.enabled` | `AGENTIC_FLINK_CHECKPOINT_ENABLED` |
| `checkpoint.interval.ms` | `AGENTIC_FLINK_CHECKPOINT_INTERVAL_MS` |
| `checkpoint.min.pause.ms` | `AGENTIC_FLINK_CHECKPOINT_MIN_PAUSE_MS` |
| `checkpoint.timeout.ms` | `AGENTIC_FLINK_CHECKPOINT_TIMEOUT_MS` |
| `checkpoint.retention` | `AGENTIC_FLINK_CHECKPOINT_RETENTION` |
| `checkpoint.storage.dir` | `AGENTIC_FLINK_CHECKPOINT_STORAGE_DIR` |
| `checkpoint.state.backend` | `AGENTIC_FLINK_CHECKPOINT_STATE_BACKEND` |
<!-- /env-vars -->

System properties use the prefix `agentic.flink.` followed by the key verbatim (e.g., `-Dagentic.flink.checkpoint.interval.ms=30000`).

## Configuration Keys

### Checkpointing and State Backend (Fault Tolerance)

`LocalWorkflowSession` and the `FlinkPipelineRunner` main apply
`org.agentic.flink.job.FlinkJobDefaults` to their `StreamExecutionEnvironment`. The defaults
configure exactly-once checkpointing with the values below, read from the process environment.

| Key | Default | Description |
|---|---|---|
| `checkpoint.enabled` | `true` | Set to `false` to run without checkpoints. This is the only way to disable them, it is logged at WARN level on every job start, and any value other than `true` or `false` is rejected |
| `checkpoint.interval.ms` | `10000` | Time between checkpoints in milliseconds. Must be positive |
| `checkpoint.min.pause.ms` | `1000` | Minimum pause between the end of one checkpoint and the start of the next. Must not be negative |
| `checkpoint.timeout.ms` | `600000` | A checkpoint that takes longer is aborted. Must be positive |
| `checkpoint.retention` | `retain` | Externalized checkpoint retention: `retain` keeps the last checkpoint when the job is cancelled, `delete` removes it on cancellation, `none` writes no externalized checkpoints |
| `checkpoint.storage.dir` | _(none)_ | Checkpoint directory URI (`file:///...`, `s3://...`, `hdfs://...`). Without it the framework uses `${java.io.tmpdir}/agentic-flink/checkpoints` and logs the location; that is only adequate for recovery on the same host, so production deployments set a shared durable location |
| `checkpoint.state.backend` | `hashmap` | `hashmap` (heap), `rocksdb` or `forst`. The non-default backends need their Flink artifact on the job classpath, see below |
| `checkpoint.state.backend.incremental` | `true` | Incremental checkpoints for backends that support them (`rocksdb`, `forst`). Ignored by `hashmap` |

Malformed numbers, negative or zero durations, unknown retention or backend names, and a
missing backend artifact make `FlinkJobDefaults.fromConfig` throw at job construction time.
Nothing falls back to running without checkpoints or to a different backend without saying so.

State backend artifacts (the framework does not ship them, add the one you select to the job
classpath at the Flink version in use, 2.2.1):

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-statebackend-rocksdb</artifactId>
  <version>2.2.1</version>
</dependency>
```

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-statebackend-forst</artifactId>
  <version>2.2.1</version>
</dependency>
```

When `checkpoint.state.backend=rocksdb` (or `forst`) is set and the artifact is missing, job
construction fails with a message that names the artifact. The `hashmap` backend keeps state on
the JVM heap and needs no extra dependency.

## Creating a Configuration Instance

### Production (reads environment)

```java
AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();
long interval = config.getLong(ConfigKeys.CHECKPOINT_INTERVAL_MS, 10_000L);
String backend = config.get(ConfigKeys.CHECKPOINT_STATE_BACKEND);
```

`fromEnvironment()` resolves all four levels of the priority chain.

### Programmatic (explicit overrides)

```java
Map<String, String> props = new HashMap<>();
props.put(ConfigKeys.CHECKPOINT_INTERVAL_MS, "30000");
props.put(ConfigKeys.CHECKPOINT_STATE_BACKEND, "rocksdb");

AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
```

Explicit properties take highest priority, but environment variables and system properties still serve as fallback for keys not present in the map.

### Testing (isolated from host)

```java
AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
```

`forTesting()` does not read environment variables or system properties. Only built-in defaults are returned, ensuring tests are deterministic regardless of the host environment.

### Exporting as a Map

`toMap()` exports all resolved values:

```java
Map<String, String> allValues = AgenticFlinkConfig.fromEnvironment().toMap();
```

The returned map contains every key that has a resolved non-null value, combining explicit properties, environment overrides, and defaults.

## Accessor Methods

| Method | Return Type | Behavior |
|---|---|---|
| `get(key)` | `String` | Returns resolved value or `null` |
| `get(key, defaultValue)` | `String` | Returns resolved value or the provided default |
| `getInt(key, defaultValue)` | `int` | Parses as integer; returns default on missing or unparseable values |
| `getLong(key, defaultValue)` | `long` | Parses as long; returns default when missing, throws `IllegalArgumentException` when unparseable |
| `getBoolean(key, defaultValue)` | `boolean` | Accepts `true` or `false` (case insensitive); returns default when missing, throws `IllegalArgumentException` for any other value |
| `toMap()` | `Map<String, String>` | Exports all resolved key-value pairs as an unmodifiable map |

## Example .env File

For local development, set environment variables in a `.env` file (loaded by your shell or process runner):

```bash
# Checkpointing (exactly-once, enabled by default)
AGENTIC_FLINK_CHECKPOINT_INTERVAL_MS=10000
AGENTIC_FLINK_CHECKPOINT_STORAGE_DIR=file:///var/lib/agentic-flink/checkpoints
# AGENTIC_FLINK_CHECKPOINT_STATE_BACKEND=rocksdb   # needs flink-statebackend-rocksdb on the classpath
# AGENTIC_FLINK_CHECKPOINT_ENABLED=false           # explicit opt-out, logged at WARN
```

## File Locations

- `ConfigKeys` constants: `src/main/java/org/agentic/flink/config/ConfigKeys.java`
- `AgenticFlinkConfig` class: `src/main/java/org/agentic/flink/config/AgenticFlinkConfig.java`
