# PyFlink

Three Python paths touch Flink. This page tells them apart and points at the page that documents
each one.

| Path | Package | What runs | Conformance |
|---|---|---|---|
| `agentic/v1` on PyFlink | `agentic-pyflink` (`pyflink/`) | PyFlink authors the job graph; the Java `WorkflowTurnFunction` executes turns | column `pyflink` in [capabilities.md](capabilities.md#capabilities) |
| `agentic/v1` through the facade | `agentic-flink` (`python/`), runtime name `flink-jvm` | a Flink MiniCluster job inside the facade's in-process JVM | column `python-flink` in [capabilities.md](capabilities.md#capabilities) |
| PyFlink-native agent plan | `agentic_flink.pyflink` (`python/`) | decorated Python agents compiled to an `AgentPlan` and attached to a PyFlink job through `CompileUtils.attachAgent`; Python tools run in the JVM through PEMJA | none; this is the legacy Flink framework, outside `agentic/v1` |

The first two take the same workflow document and return the same normalized result; the
runnable examples are on [python.md](python.md), run on both. The runtime itself, its
configuration, connectors and cluster submission are on the [PyFlink runtime page](runtimes/pyflink.md).

The third is the Python front end of the legacy Flink DSL. It is supported and kept, and it does
not take an `agentic/v1` workflow. It is documented on [pyflink-integration.md](pyflink-integration.md).

## State primitives

The `agentic/v1` Flink adapter (`src/main/java/org/agentic/flink/runtime/`) is Java, so the pure
PyFlink package never reimplements Flink state in Python. For readers who want to see how the Java
side maps onto Flink, and what a Python reimplementation would use, the Java primitives and their
PyFlink names are:

| Java | PyFlink |
|---|---|
| `ValueState<T>` | `pyflink.datastream.state.ValueState` |
| `MapState<K, V>` | `pyflink.datastream.state.MapState` |
| `ListState<T>` | `pyflink.datastream.state.ListState` |
| `StateTtlConfig` | `pyflink.datastream.state.StateTtlConfig` |
| `KeyedProcessFunction` | `pyflink.datastream.functions.KeyedProcessFunction` |
| `RichFunction.open(RuntimeContext)` | `Function.open(RuntimeContext)` |

`KeyedConversationLog` keeps the event log in a `ListState<LogEvent>` plus a `ValueState<Long>`
sequence counter, keyed by `conversation_id`; see the [Flink runtime page](runtimes/flink.md).
