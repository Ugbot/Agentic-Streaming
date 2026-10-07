# PyFlink runtime

`pyflink/` (import package `agentic_pyflink`, distribution `agentic-pyflink`) runs `agentic/v1`
workflows as PyFlink jobs. It is the `pyflink` column of the [matrix](../capabilities.md). Python
contributes the workflow document, the Flink configuration and the connectors; the operator in
the middle of the job is the Java `WorkflowTurnFunction` of the [Flink page](flink.md), reached
through a thin bridge jar (`pyflink/java`, class `org.agentic.pyflink.PyFlinkJob`). No Python code
runs inside the job graph.

## The job graph

```text
source -> keyBy(conversation_id) -> json->event -> WorkflowTurnFunction -> result->json -> sink
```

`PyFlinkJob.assemble` keys the JSON turn stream by `conversation_id` before decoding, so turns of
one conversation travel through one subtask and reach the identically keyed workflow operator in
delivery order even when the decoder runs at a higher parallelism than the source. The workflow
document is validated on the Java side (`WorkflowValidator`) and `runtime.flink` knobs are
honoured exactly as in the JVM runner. The event log is therefore the same `KeyedConversationLog`
keyed state, and a restart is the same savepoint restore, as on the Flink page.

## Two ways to run

`FlinkRuntime.run(spec, turns)` is bounded: a list of turns goes in, the normalized results come
back, and the job ends when the turns are drained. The CLI (`python -m agentic_pyflink run
<workflow> --text "..."`) uses it.

`FlinkRuntime.deploy(spec)` then `submit(turn)` is streaming: the job stays up, turns are handed
to it one or one batch at a time through a `FileSource`, and each call blocks until the
normalized result of every turn in the call is visible in the `FileSink`. `restart()` stops the
job with a savepoint and starts a fresh job restored from it (`execution.state-recovery.path`),
mutating the runtime in place. That is how the conformance binding runs `restart_runtime`, so the
`pyflink` column's `replay-after-restart` and `suspend-resume` passes are savepoint restores.
The streaming path needs source and sink directories this process can read and write, a local
spool in `local` mode.

## Configuration

`FlinkConfig(mode="local", parallelism=1, checkpoint_interval="1s")` is the configuration used
for the executed examples in [docs/python.md](../python.md): the job runs on a MiniCluster inside
the Py4J gateway JVM that PyFlink starts. `mode="cluster"` with `rest_address` submits the job
graph to an existing Flink cluster over REST; that mode is not exercised by the test suite.
`agentic_pyflink.jars` resolves the bridge jar and the framework uber jar for the job's classpath;
both must be built first.

## Capabilities

`agentic_pyflink/capabilities.py` declares a capability `supported` only when a test in
`pyflink/tests` proves it, and lists that test in `PROOF`. Anything the Java adapter implements
but this package has no test for is `not_tested`, and nothing is inferred from another runtime's
results. `timers` and `checkpoint_recovery` are in that second group: the binding skips the three
timer fixtures, and the matrix records both as unsupported for this column
([pyflink notes](../capabilities.md#pyflink)). This page does not claim timers on PyFlink.

## Where it stands

<!-- matrix: pyflink -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T17:15:17+00:00, commit `eef9c63ea3ff`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `pyflink`: 21 passed, 0 failed, 3 skipped: `timer-fires` skipped, `event-time-timer` skipped, `timer-survives-restart` skipped.

| Capability | pyflink |
|---|---|
| `timers` | [unsupported](../capabilities.md#pyflink) |
| `checkpoint_recovery` | [unsupported](../capabilities.md#pyflink) |
<!-- /matrix -->

## Related surfaces

`agentic-pyflink` is one of three ways Flink is reachable from Python; the table in
[docs/pyflink.md](../pyflink.md) tells them apart. The facade's `flink-jvm` runtime is on the
[Python facade page](python-facade.md). `agentic_flink.pyflink`, the declarative plan compiler for
the legacy DSL described in [docs/pyflink-integration.md](../pyflink-integration.md), is outside
`agentic/v1` and has no matrix column.

## Running it

```bash
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw install -DskipTests                 # the framework jar the bridge depends on
./mvnw -f pyflink/java/pom.xml package     # the bridge jar
cd pyflink && python -m venv .venv && .venv/bin/pip install -e '.[test]' 'apache-flink==2.2.1'
.venv/bin/pytest tests/test_conformance.py  # the pyflink column
.venv/bin/python -m agentic_pyflink run ../spec/conformance/v1/workflows/support.yaml --text "what is my balance?"
```
