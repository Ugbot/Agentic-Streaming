# Pluggable Storage Quick Start Guide

This guide runs `StorageIntegratedFlinkJob`, the example that drives the pluggable
storage SPI from a Flink keyed operator. Everything below was executed from a clean
checkout with the commands shown. Everything the job supports is described; anything it does
not support is stated as unsupported instead of documented as an option.

## What the job does

`org.agentic.flink.example.StorageIntegratedFlinkJob` builds a local Flink job:

1. A `PollingSource` emits simulated conversation events for four users
   (`alice`, `bob`, `charlie`, `diana`), paced 100 to 500 ms apart, up to 100 events, then idles.
2. A `KeyedProcessFunction` keyed by user id loads the per-flow context from a HOT tier
   (`ShortTermMemoryStore`), hydrates it from the WARM tier (`LongTermMemoryStore`) when the
   HOT tier is empty, appends the new `ContextItem`, writes the HOT tier back, and persists an
   `AgentContext` to the WARM tier every five messages.
3. Each processed event is printed as
   `[flow-<user>] User <user>: <message> (Context size: <n>)`.
4. A storage metrics report is written to the SLF4J logger every 30 seconds.

The storage instances are created in `open()` from a `StorageConfiguration` through
`StorageFactory`, not from Flink state. The production short-term memory of this framework is
Flink state (`FlinkStateShortTermMemory`); this example exercises the legacy `ShortTermMemoryStore`
path only to demonstrate the SPI. See [Storage Architecture](../reference/storage-architecture.md).

## Run it

Flink is a `provided` dependency of the root module, so a plain `mvn exec:java` does not have Flink
on its classpath. The root `pom.xml` ships an `examples` profile that forks a JVM on the test-scope
classpath. `compile` must be part of the same invocation. From the repository root:

```bash
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -q -P examples compile exec:exec \
  -Dexec.mainClass=org.agentic.flink.example.StorageIntegratedFlinkJob \
  -Dexec.args="memory"
```

The job is unbounded; stop it with Ctrl+C. Output observed on a clean checkout (the order of
users is random):

```
[flow-alice] User alice: Can you check the status? (Context size: 1)
[flow-bob] User bob: What are the options? (Context size: 1)
[flow-bob] User bob: Thanks! (Context size: 2)
[flow-alice] User alice: What about customs fees? (Context size: 2)
```

The run also prints `SLF4J(W): No SLF4J providers were found`. The example classpath has no
SLF4J 2 provider, so the `LOG.info` lines (hydration messages and the 30 second metrics report)
are dropped; only the `print()` sink output appears. To see the log lines, add an SLF4J 2
provider such as `org.slf4j:slf4j-simple` to the classpath of your own job.

## Backend argument

The first program argument selects the backend. The job recognizes exactly two cases:

| Argument | Behaviour |
|---|---|
| `memory` (or no argument, or any value other than `redis`) | HOT and WARM tiers are `memory`: `InMemoryShortTermStore` and `InMemoryLongTermStore`. Nothing survives a restart. |
| `redis` | Builds a `StorageConfiguration` with `withHotTier("redis", ...)`. `StorageFactory.createShortTermStore` accepts only `memory`, so `open()` fails with `IllegalStateException: Backend 'redis' not available for tier: HOT` and the job exits with status 1. This was verified with `-Dexec.args="redis"`. |

There is no `postgresql` case in this example. Passing `postgresql` runs the in-memory
configuration. To use the PostgreSQL long-term store, create it in your own job:

```java fragment
Map<String, String> warm = new HashMap<>();
warm.put("postgres.url", "jdbc:postgresql://localhost:5432/agentic");
warm.put("postgres.user", "agentic");
warm.put("postgres.password", "agentic");
LongTermMemoryStore store = StorageFactory.createLongTermStore("postgres", warm);
```

The configuration keys and the `AGENTIC_FLINK_*` environment variables that feed them are listed
in [Configuration](../configuration.md); the environment names are generated from
`ConfigKeys.java` and checked by `docs/tools/check_env_vars.py`.

## Configuration maps used by the memory backend

```java fragment
Map<String, String> hot = new HashMap<>();
hot.put("cache.max.size", "10000");
hot.put("cache.ttl.seconds", "3600");

Map<String, String> warm = new HashMap<>();
warm.put("cache.max.size", "5000");
warm.put("cache.ttl.seconds", "86400");

StorageConfiguration config = StorageConfiguration.builder()
    .withHotTier("memory", hot)
    .withWarmTier("memory", warm)
    .build();
```

`StorageConfiguration.validate()` checks each tier name against
`StorageFactory.isBackendAvailable(tier, name)`; `createShortTermStore()` and
`createLongTermStore()` on the configuration call the factory with the stored maps.

## Troubleshooting

`NoClassDefFoundError: org/apache/flink/...`: the command omitted `-P examples` or used
`exec:java`. Flink is `provided`; only the `examples` profile puts it on the classpath.

`Could not find or load main class`: `compile` was not part of the same invocation, or the
`jagentic-core` module has not been installed to the local repository yet.

`Backend 'redis' not available for tier: HOT`: expected, see the table above.

## Next steps

- [Storage Architecture](../reference/storage-architecture.md): what `META-INF/services`
  registers, what `StorageFactory` accepts, and how `FlinkStateShortTermMemory` is the default.
- [Creating Storage Backends](creating-storage-backends.md): register your own
  `LongTermMemoryStore` or `VectorStore` through `ServiceLoader`.
- [Memory](../memory.md): the Flink-state-first short-term memory used by `AgentBuilder` jobs.
