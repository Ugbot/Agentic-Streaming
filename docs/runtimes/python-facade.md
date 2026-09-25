# Python facade (JVM-backed)

`python/` (import package `agentic_flink`, distribution `agentic-flink`) starts a JVM in the
Python process with JPype and drives the Java runtimes through it. For `agentic/v1` it registers
three runtime names in the `agentic.runtimes` entry point group; two of them are conformance
tested and have their own columns in the [matrix](../capabilities.md):

| Runtime name | Python class | Java behind it | Matrix column |
|---|---|---|---|
| `local-jvm` | `agentic_flink.JvmLocalRuntime` | `org.jagentic.core.LocalRuntime` (`ports/jagentic-core`) | `python-jvm` |
| `flink-jvm` | `agentic_flink.FlinkRuntime` | `org.agentic.flink.runtime.LocalWorkflowSession` and `WorkflowTurnFunction` | `python-flink` |
| `pekko` | `agentic_flink.PekkoRuntime` | `org.jagentic.pekko.runtime.PekkoBackendProvider` | none, `not_tested` |

Every public method returns Python values: results are the Java `TurnResult.toMap()` converted
to a `dict`, Java exceptions are translated to the `agentic` error classes, and Java futures are
exposed as `concurrent.futures.Future`. The two-level API on top of these classes (`load`,
`WorkflowAgent`, `AgentSpec.run(runtime=...)` above; `deploy`, `submit`, `restart`, `close`
below) is documented with executed examples in [docs/python.md](../python.md).

## `local-jvm`

`JvmLocalRuntime.deploy(spec)` builds the graph from the workflow document with the core's
`GraphBuilder` and wraps a Java `LocalRuntime` over in-memory stores. The conversation log is the
only state that survives `restart()`, which replaces the Java runtime in place and replays the
log, so the Python object stays valid. `submit_async` maps onto `LocalRuntime.submitAsync`: one
serial writer per conversation, different conversations in flight at once
(`python/tests/test_parallel_conversations_jvm.py`). Everything this runtime proves is the
`jvm-core` column's `RoutedGraph`, reached from Python; the `python-jvm` column measures the
facade end to end.

## `flink-jvm`

`FlinkRuntime.deploy(spec)` starts one streaming job on an in-process local Flink environment
through `LocalWorkflowSession` and keeps it up until `close()`; `submit` feeds it turn by turn, so
keyed state (the conversation log in `KeyedConversationLog`) persists across calls. `restart()`
stops the job with a savepoint under `savepoint_dir` and starts a new job restored from it, in
place; the recorded turns are rebuilt from the log and not executed again
(`python/tests/test_flink_durable_restart.py`). With `durable=False`, each `submit` runs one
bounded job instead and `restart()` is refused. The operator is the one described on the
[Flink page](flink.md).

The facade cannot ship Python code into a Flink job graph, so a workflow with `kind: function`
(Python) tools is rejected at `deploy` with a `CapabilityError` listing the tool, rather than run
partially. The same workflow runs on `local-jvm`. This boundary is shown with its exact error in
[docs/python.md](../python.md#level-1-the-high-level-api).

## Restart semantics

Both runtimes restart in place and return `None`; the same Python object is used afterwards.
The pure Python `LocalRuntime.restart()` on the [pure Python page](python.md#restart) returns a
new object instead, which is why the shared full-control example writes `rt = rt.restart() or rt`.

## Pekko: unsupported

`PekkoRuntime` exists so the `pekko` name resolves, but every capability is declared
`not_tested` and the runtime has no matrix column. The reason is a Jackson version conflict:
Pekko's Jackson Scala module needs a newer Jackson than the shaded Flink jar bundles, so the two
cannot share a classpath unless the Pekko jars are prepended and `AGENTIC_PEKKO_CLASSPATH`
points at a full dependency classpath built from `agentic-pekko/pom.xml`. That is not done for
conformance, and this documentation does not work around it: Pekko through the facade is
unsupported. The Pekko runtime of record is `agentic-pekko` itself, described on the
[Pekko page](pekko.md) and measured by the `pekko` column.

## Jars

The facade loads `target/agentic-flink-*-uber.jar` and needs the Flink distribution jars on the
classpath for `flink-jvm` and for the legacy DSL, because the shaded jar excludes Flink's
provided dependencies; `agentic_flink.flink_jars()` resolves them from the installed
`apache-flink` distribution. Build steps and troubleshooting are in
[docs/python.md](../python.md#build-and-install).

## Where it stands

<!-- matrix: python-jvm, python-flink -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T17:15:17+00:00, commit `eef9c63ea3ff`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `python-jvm`: 24 passed, 0 failed, 0 skipped.

Binding `python-flink`: 21 passed, 0 failed, 3 skipped: `timer-fires` skipped, `event-time-timer` skipped, `timer-survives-restart` skipped.

| Capability | python-jvm | python-flink |
|---|---|---|
| `timers` | [supported](../capabilities.md#capabilities) | [unsupported](../capabilities.md#python-flink) |
| `checkpoint_recovery` | [supported](../capabilities.md#capabilities) | [unsupported](../capabilities.md#python-flink) |
<!-- /matrix -->

## Legacy Flink DSL

The same package also exposes `agentic_flink.Agent.builder()` and `@tool`, a JPype handle on the
Java `org.agentic.flink.dsl.Agent`. That is the pure-Flink legacy DSL: supported, kept, and
outside `agentic/v1` and the matrix. It is described in
[docs/python.md](../python.md#the-legacy-flink-dsl-pure-flink-flavor) and in the
[Flink page](flink.md#legacy-flink-dsl-pure-flink-flavor).

## Running it

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw package -DskipTests
cd python && python -m venv .venv && .venv/bin/pip install -e '.[flink,test]'
.venv/bin/pytest tests/test_conformance_jvm.py     # python-jvm and python-flink columns
```
