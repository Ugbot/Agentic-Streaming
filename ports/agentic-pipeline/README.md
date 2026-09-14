# agentic-pipeline: declarative YAML to a running agentic system

Describe the agentic system in a `pipeline.yaml`, choose a backend, and the loader builds
it. The same spec runs on every registered backend because the core `GraphBuilder`
compiles the spec into the engine-agnostic `RoutedGraph` + tools + retriever and each
backend only hosts that graph.

## Install

`agentic-pipeline` is a regular package. It depends on `pyagentic` (the pure core, not on
PyPI) and `PyYAML`, so install the core from the checkout first:

```bash
pip install -e ports/pyagentic -e ports/agentic-pipeline
# optional engines: pip install -e 'ports/agentic-pipeline[celery]'  or  [nats]  or  [redis]
```

No `PYTHONPATH` is needed for the package itself. Tests run with
`python -m pytest ports/agentic-pipeline`.

```bash
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
# backend=local path=payments ok=True
# reply: [payments] get_balance returned 1234.56
# tools: ['get_balance']

# same YAML, different backend - nothing else changes:
... run examples/pipelines/banking.yaml --backend celery --text "tell me about crypto cash-back"
... run examples/pipelines/banking.yaml --backend nats   --text "what is my balance?"
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
| `agentic_pipeline.backends` | `make_backend(name, graph, tools, retriever)`, the shim: `local` / `celery` / `nats` (uniform `submit(Event)`) |
| `agentic_pipeline.loader` | `load(path)` / `build_system(spec)` → a `PipelineSystem` with `submit(Event)` on the chosen backend |
| `agentic_pipeline.__main__` | the `run` CLI |

## Backends

Exactly three names are registered: `local`, `celery` and `nats`. Any other name raises
`ValueError: unknown backend 'x'; supported backends: celery, local, nats`.

* `local` runs in-process and is always available.
* `celery` runs the graph in eager Celery tasks. It needs the `celery` extra and the
  adapter module `ports/experimental/celery/agentic_celery.py` importable (that directory on
  `PYTHONPATH`; the adapter is a single file and is not packaged).
* `nats` needs the `nats` extra, `ports/experimental/nats/agentic_nats.py` importable, and a
  JetStream server (`podman run -p 4222:4222 nats:latest -js`, or `AGENTIC_NATS_URL`).

The `celery` and `nats` adapters live under `ports/experimental/` because they predate the
`agentic/v1` spec and are not conformance tested; `local` is the only backend on the
acceptance path. When the engine or adapter for `celery` or `nats` is missing, `make_backend`
raises `BackendUnavailableError` naming the exact install step. Other engines under
`ports/experimental/` (Faust, Ray, Dask, Airflow) are separate ports with their own entry
points and are not selectable through this loader. Java and Go have sibling loaders against the same schema
(`jagentic-core` + Jackson-YAML, `goagentic` + yaml.v3).

Tested in `tests/test_pipeline.py`: the same `banking.yaml` runs on local and on celery
(skipped with the install message when the celery adapter is absent) and nats (skipped when
no server is up) with identical routing; the LLM variant runs a deterministic ReAct turn via
the stub provider; guardrails block; unknown backends and missing `llm.model` fail with
the messages above.
