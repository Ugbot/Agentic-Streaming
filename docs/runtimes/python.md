# Pure Python runtime

`ports/pyagentic` (import package `agentic`, distribution `pyagentic`) is a pure Python
implementation of `agentic/v1` with no JVM. Its `LocalRuntime` is the `python` binding of the
[conformance runner](README.md#conformance-results) and the runtime behind `run(runtime="local")` in
[docs/python.md](../python.md). The module README (`ports/pyagentic/README.md`) has the layout and
the `Agent` builder; this page is about how the primitives map.

## The log

`agentic/events.py` is the closed v1 event set, the dense per-conversation `sequence`, the
`reduce_state` fold, and two event logs: `InMemoryEventLog` (the default) and `FileEventLog`
(JSONL with fsync, selected by a `stores.conversation` block of kind `file` or by `store_dir=`).
Unknown event types survive a new instance over the same file and are ignored by the fold
(`tests/test_agentic_runtime.py::test_file_event_log_survives_a_new_instance_and_preserves_unknown_types`).

`FileEventLog` takes an OS lock on `.writer.lock` in its directory, so a second writer, in this
process or another, is refused rather than interleaving sequences; the OS releases the lock if a
writer dies. A final line without its newline was never acknowledged, and is cut on load. Anything
else that does not read back, or a gap in `sequence`, raises `CorruptLog` for that conversation
alone: the runtime still deploys, and turns on that conversation end `failed` with class `fatal`
(`tests/test_file_event_log.py`).

## The engine

`agentic/engine.py` holds the semantics shared by every fixture: guardrails, keyword and static
routing, the rule brain and the scripted `stub` LLM brain, verification bounds, structured tool
calls and retry, saga compensation, memory, retrieval, suspend and resume, A2A delegation, CEP
sequence patterns, the context window, idempotency and replay. It is the code the fixtures
exercise, through `LocalRuntime`. Beyond the fixtures it runs the real model clients
(`agentic/llm.py`: OpenAI, Ollama, Claude and Gemini), tools from MCP stdio servers (`agentic/mcp.py`),
peers over HTTP, the HNSW knowledge base (`agentic/hnsw.py`), the lexicon classifier guardrail
(`agentic/classifier.py`), skill prompts, a token budget on what the model sees, and `cep` patterns
that inject a derived turn. The `pipeline.yaml` CLI (`ports/agentic-pipeline`) runs every example on
this engine.

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
spelled out. The conformance verb `restart_runtime` maps onto it, so the `python` binding's
`replay-after-restart` and `suspend-resume` passes are replays into a new instance.

## Capabilities

`LocalRuntime.capabilities()` is the runtime's own declaration. `deploy(spec)` compares the
workflow's requirements against it and raises `CapabilityError` listing every unsupported
requirement instead of running a subset. The declaration says `llm_brain` is partial: the one
`llm_brain` fixture uses the scripted `stub` provider, so a pass there says nothing about a real model.
The real clients are proven against local servers speaking each provider's format, not a live model. `timers` read the runtime's `clock` (wall time by default,
or a `ManualClock` whose `advance(ms)` moves it) and the conversation watermark; pending timers
and a manual clock's reading survive `restart()` because both are rebuilt from the log. What counts is fixture outcomes, not the
declaration.

## Running it

```bash
cd ports/pyagentic
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/python -m agentic.conformance    # the fixtures against LocalRuntime
```

The two-level API (`Agent` builder and `load(...).run(runtime="local")` on top,
`LocalRuntime().deploy(spec)` underneath) is documented with executed examples in
[docs/python.md](../python.md). Other runtime names resolve through the `agentic.runtimes` entry
point group; this repository registers only `local`, and any other name raises
`RuntimeNotAvailableError`.
