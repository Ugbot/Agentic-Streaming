# agentic-pipeline: declarative YAML to a running agentic system

Describe the agentic system in a `pipeline.yaml` and the loader builds it. The core
`GraphBuilder` compiles the spec into the engine-agnostic `RoutedGraph` + tools + retriever,
and the `local` backend hosts that graph in-process.

## Install

`agentic-pipeline` is a regular package. It depends on `pyagentic` (the pure core, not on
PyPI) and `PyYAML`, so install the core from the checkout first:

```bash
pip install -e ports/pyagentic -e ports/agentic-pipeline
```

No `PYTHONPATH` is needed for the package itself. Tests run with
`python -m pytest ports/agentic-pipeline`.

```bash
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
# backend=local path=payments ok=True
# reply: [payments] get_balance returned 1234.56
# tools: ['get_balance']
```

## What the YAML expresses

See [`examples/pipelines/banking.yaml`](../../examples/pipelines/banking.yaml) (rule
brains) and [`banking-llm.yaml`](../../examples/pipelines/banking-llm.yaml) (an LLM
ReAct path). Sections: `backend`, optional `llm` (`provider` plus a required `model` for `ollama` and
`openai`; there is no default model name, and `stub` for offline runs),
`agent.router` (keyword rules), `agent.paths` (per-path brain `rule|llm`, prompt, tools,
`tool_triggers`), `agent.verifier`, `tools` (`constant`/`http`), `retrieval` (hashing
embedder + KB), `guardrails` (regex deny-lists).

## Pieces

| Module | Role |
|--------|------|
| `pyagentic.builder` | `build(spec, chat_client_factory) -> (graph, tools, retriever)`, the core GraphBuilder (engine-agnostic) |
| `agentic_pipeline.backends` | `make_backend(name, graph, tools, retriever)`, the shim: `local` (uniform `submit(Event)`) |
| `agentic_pipeline.loader` | `load(path)` / `build_system(spec)` → a `PipelineSystem` with `submit(Event)` on the chosen backend |
| `agentic_pipeline.__main__` | the `run` CLI |

## Backends

`local` is the only backend. It runs in-process and is always available.

The experimental adapters (`celery`, `nats`, `ray` and the others) have moved out of this
repository; the code as it stood is at
[tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/ports/experimental).
Any other name, on `--backend` or in a spec's `backend:`, raises `ValueError: unknown backend
'x'; supported backends: local. ...` with that same pointer, and the CLI exits with status 2.

Java has a sibling loader against the same schema (`jagentic-core` + Jackson-YAML).

Tested in `tests/test_pipeline.py`: `banking.yaml` runs on `local`; the registry is exactly
that one name and every other is refused with the message above; the LLM variant runs a
deterministic ReAct turn via the stub provider; guardrails block; a missing `llm.model` is
refused.
