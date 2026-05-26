# Channels

A `Channel<T>` is the framework's continuous-input primitive: a serializable
factory that produces a `DataStream<T>` at job-graph time. Channels are
unified: a Kafka topic, an HTTP webhook, a Postgres CDC poll, an LLM tool
invocation, and a static seed list are all `Channel<T>` impls.

The point of the abstraction: anywhere you'd otherwise hand-roll source
plumbing, you instead union one or more channels together. The crawler is
the canonical example — its frontier is whatever channels you wire in, and
the crawler operator doesn't care which they are.

## The contract

```java
public interface Channel<T> extends Serializable {
    DataStream<T> open(StreamExecutionEnvironment env) throws Exception;
    TypeInformation<T> elementType();
    default String providerName() { return getClass().getSimpleName(); }
}
```

Implementations must be `Serializable` (they ride in the Flink job graph);
the live transport — Kafka consumer, HTTP server, JDBC handle — is built
inside `open(env)` when the job graph is constructed.

## Built-in channels

| Class | Transport | Notes |
|-------|-----------|-------|
| `StaticSeedChannel<T>` | in-process | Wraps `env.fromCollection`. Tests + seed lists. |
| `KafkaChannel<T>` | Kafka | Generic JSON-deserialized payload of `T`. |
| `KafkaContextChannel` | Kafka | `Channel<KeyedContextItem>` for the memory feed path. |
| `PostgresChangeChannel` | Postgres polling | Single-parallelism. Polls `agent_facts`. |
| `RedisPubSubChannel` | Redis pub/sub | Single-parallelism. Requires Jedis (optional dep). |
| `WebhookChannel<T>` | HTTP `POST` | JDK `HttpServer`. Single-parallelism. |
| `ToolInvocationChannel<T>` | three options — see below | LLM-driven. |

## `ToolInvocationChannel<T>`

A tool the LLM can call that **also** materializes each invocation as a
stream element for some downstream operator to consume. Three transports:

| Static factory | Transport | Cross-TM | Cross-job | Use for |
|----------------|-----------|----------|-----------|---------|
| `ToolInvocationChannel.sideOutput(toolId, type, mapper)` | Flink side-output | ✅ | ❌ | Default. Single-job, exactly-once with checkpoints. |
| `ToolInvocationChannel.via(toolId, type, mapper, wrapped, publisher)` | Wrapped `Channel<T>` (Kafka, …) | ✅ | ✅ | Multiple Flink jobs share the tool stream. |
| `ToolInvocationChannel.inJvm(toolId, type, mapper)` | per-task `BlockingQueue` | ❌ | ❌ | Unit tests / single-JVM dev only. |

For side-output to actually emit via Flink's network stack, the **agent
operator** must set the current emit-context per call via
`ToolInvocationChannel.setCurrentContext(...)` before invoking the LLM. The
framework's agent operators handle this automatically; if you're writing
your own operator that hosts tool calls, mirror the pattern.

## Writing your own channel

```java
public final class WebSocketChannel<T> implements Channel<T> {
    private final String url;
    private final Class<T> type;

    @Override
    public DataStream<T> open(StreamExecutionEnvironment env) {
        return env.addSource(new MyWebsocketSource<>(url, type), TypeInformation.of(type))
            .name("websocket[" + url + "]");
    }

    @Override
    public TypeInformation<T> elementType() {
        return TypeInformation.of(type);
    }
}
```

Register through `ChannelRegistry.builder().add("name", channel).build()` —
or pass directly into `CrawlerCore.builder().frontier(...)`,
`IngestionPipeline.from(channel.open(env)).…`, etc.

## When to use which

- **`StaticSeedChannel`** — tests, seed URLs, single-shot demos.
- **`KafkaChannel<T>`** — production. Cross-job, cross-TM, backpressure-aware.
- **`KafkaContextChannel`** — when the payload is specifically the framework's
  `KeyedContextItem` (memory-feed use case).
- **`WebhookChannel<T>`** — accept events from upstream systems (GitHub
  events, Slack, custom integrations) without a message broker in the loop.
- **`PostgresChangeChannel` / `RedisPubSubChannel`** — when those are already
  in your stack.
- **`ToolInvocationChannel<T>`** — when an LLM tool call should *also* drive
  a continuous operator (the agent fires a crawl, the crawler picks it up).

The crawler in the [live-research example](examples/live-research.md) wires
four of these into the same operator's input — seed + LLM tool + (optional
Kafka) + discovered-links recursion. Same pattern works for any operator
that wants a unified "things to do next" frontier.
