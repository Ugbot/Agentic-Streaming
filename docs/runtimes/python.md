# Pure Python runtime

`ports/pyagentic` (import package `agentic`, distribution `pyagentic`) is a pure Python
implementation of `agentic/v1` with no JVM. Its `LocalRuntime` is the `python` column of the
[matrix](../capabilities.md) and the runtime behind `run(runtime="local")` in
[docs/python.md](../python.md). The module README (`ports/pyagentic/README.md`) has the layout and
the `Agent` builder; this page is about how the primitives map.

## The log

`agentic/events.py` is the closed v1 event set, the dense per-conversation `sequence`, the
`reduce_state` fold, and two event logs: `InMemoryEventLog` (the default) and `FileEventLog`
(JSONL with fsync, selected by a `stores.conversation` block of kind `file` or by `store_dir=`).
Unknown event types survive a new instance over the same file and are ignored by the fold
(`tests/test_agentic_runtime.py::test_file_event_log_survives_a_new_instance_and_preserves_unknown_types`).

## The engine

`agentic/engine.py` holds the semantics shared by every fixture: guardrails, keyword and static
routing, the rule brain and the scripted `stub` LLM brain, verification bounds, structured tool
calls and retry, saga compensation, memory, retrieval, suspend and resume, A2A delegation, CEP
sequence patterns, the context window, idempotency and replay. It is the code the fixtures
exercise, through `LocalRuntime`.

## One writer per conversation

`LocalRuntime` (`agentic/runtime.py`) gives each conversation an arrival gate: `submit` and
`submit_async` take a ticket on the caller's thread, so turns of one conversation run one at a
time in call order while turns of different conversations run at the same time on a worker pool.
That is what the `ordered-concurrent-turns` and `parallel-conversations` fixtures check. A redelivered `turn_id` is
answered from the log as `duplicate` without running the engine.

## Restart

`LocalRuntime.restart()` closes the instance and returns a fresh `LocalRuntime` over the same log,
replayed. It returns the new object rather than mutating in place, so callers must rebind:
`rt = rt.restart()`. Passing the same `log` to a second `LocalRuntime` is the same operation
spelled out. The conformance verb `restart_runtime` maps onto it, so the `python` column's
`replay-after-restart` and `suspend-resume` passes are replays into a new instance.

## Capabilities

`LocalRuntime.capabilities()` is the runtime's own declaration. `deploy(spec)` compares the
workflow's requirements against it and raises `CapabilityError` listing every unsupported
requirement instead of running a subset. The declaration says `timers` and `checkpoint_recovery`
are unsupported and `llm_brain` is partial (only the scripted `stub` provider runs locally); the
matrix agrees on the first two and, because the one `llm_brain` fixture uses the stub, records
`llm_brain` as supported. What counts is the matrix, derived from fixture outcomes.

## Where it stands

<!-- matrix: python -->
Derived from [capabilities.md](../capabilities.md) (run 2026-09-14T16:33:10+00:00, commit `ec936052943c`) by
`docs/tools/matrix_excerpt.py`; do not edit by hand. Every capability not listed below is
[supported](../capabilities.md#capabilities) for the binding, meaning every fixture that requires it passed.

Binding `python`: 21 passed, 0 failed, 3 skipped: `timer-fires` skipped, `event-time-timer` skipped, `timer-survives-restart` skipped.

| Capability | python |
|---|---|
| `timers` | [unsupported](../capabilities.md#python) |
| `checkpoint_recovery` | [unsupported](../capabilities.md#python) |
<!-- /matrix -->

## Running it

```bash
cd ports/pyagentic
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/python -m agentic.conformance    # the fixtures against LocalRuntime, the python column
```

The two-level API (`Agent` builder and `load(...).run(runtime="local")` on top,
`LocalRuntime().deploy(spec)` underneath) is documented with executed examples in
[docs/python.md](../python.md). Other runtime names (`local-jvm`, `flink-jvm`, `pyflink`) are
resolved through the `agentic.runtimes` entry point group when the corresponding package is
installed; selecting one that is not installed raises `RuntimeNotAvailableError` naming the
package.
