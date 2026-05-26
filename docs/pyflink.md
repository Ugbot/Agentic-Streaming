# PyFlink notes for Agentic Flink

> **Looking for the Python API?** The canonical entry point is now
> [`docs/python.md`](python.md), which documents the JPype-backed
> `agentic-flink` package and the PyFlink integration path. This page kept
> for background on the underlying state-primitive mapping and the future
> native-port option.

Agentic Flink is Java-first, but a PyFlink user can adopt it today by driving the existing Java operators from Python. A native Python port of the memory primitives is mechanical and well-defined; it just hasn't been written yet.

## State primitives

Every Java primitive this framework relies on has a 1:1 PyFlink equivalent:

| Java                                           | PyFlink                                                |
|------------------------------------------------|--------------------------------------------------------|
| `ValueState<T>`                                | `pyflink.datastream.state.ValueState`                  |
| `MapState<K, V>`                               | `pyflink.datastream.state.MapState`                    |
| `ListState<T>`                                 | `pyflink.datastream.state.ListState`                   |
| `StateTtlConfig`                               | `pyflink.datastream.state.StateTtlConfig`              |
| `KeyedProcessFunction`                         | `pyflink.datastream.functions.KeyedProcessFunction`    |
| `RichFunction.open(RuntimeContext)`            | `Function.open(RuntimeContext)`                        |

A native port of `FlinkStateShortTermMemory` is a straight translation. The hard parts (TTL config, descriptor names, state scoping) carry over without conceptual change.

## Adoption path A — reuse Java operators from Python

The simplest way to use Agentic Flink today:

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:///path/to/agentic-flink-1.0.0-SNAPSHOT.jar")

gw = get_gateway()
Agent = gw.jvm.org.agentic.flink.dsl.Agent
PostgresStore = gw.jvm.org.agentic.flink.storage.postgres.PostgresConversationStore

postgres = PostgresStore()
postgres.initialize({"postgres.url": "jdbc:postgresql://...", ...})

agent = (Agent.builder()
    .withId("research-bot")
    .withSystemPrompt("...")
    .withLongTermStore(postgres)
    .build())
```

The agent operator runs in the JVM under the same checkpoint guarantees as a Java job. Python is only used as the job-graph driver. This is the recommended path until there is a real need for native Python operators (e.g. arbitrary Python pre-processing inside the agent loop).

## Adoption path B — native PyFlink port (future)

A native port would mean reimplementing:

1. `ShortTermMemory` / `FlinkStateShortTermMemory` — straightforward translation.
2. `VectorMemory` / `FlinkStateVectorMemory` — brute-force KNN in NumPy; trivial.
3. `Channel<T>` — wraps `KafkaSource`, `JdbcSource`, etc. directly (this is
   what `MemoryFeed` became after the rename).
4. The agent process function (compaction, relevancy scoring, sync-to-long-term).

The LangChain4J `@Tool` bridge has no direct Python equivalent. Substitute LangChain Python or pydantic-ai for tool calling — the rest of the framework is provider-agnostic.

No native Python code ships in this repository yet. The decision deliberately defers the cost of maintaining a second codebase until the user demand is clear.
