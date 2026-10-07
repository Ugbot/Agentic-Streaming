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
# non-local backends: the engine extra here plus the adapter package from ports/experimental
pip install -e 'ports/agentic-pipeline[celery]' -e 'ports/experimental/celery[celery]'
pip install -e 'ports/agentic-pipeline[nats]'   -e 'ports/experimental/nats[nats]'
pip install -e 'ports/agentic-pipeline[ray]'    -e 'ports/experimental/ray[ray]'
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
... run examples/pipelines/banking.yaml --backend ray    --text "what is my balance?"
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
| `agentic_pipeline.backends` | `make_backend(name, graph, tools, retriever)`, the shim: `local` / `celery` / `nats` / `ray` (uniform `submit(Event)`) |
| `agentic_pipeline.loader` | `load(path)` / `build_system(spec)` → a `PipelineSystem` with `submit(Event)` on the chosen backend |
| `agentic_pipeline.__main__` | the `run` CLI |

## Backends

Exactly four names are registered: `local`, `celery`, `nats` and `ray`. A backend is
registered only if `tests/test_pipeline.py` runs `examples/pipelines/banking.yaml` end to end
on the real engine, locally or in a Podman container.

* `local` runs in-process and is always available.
* `celery` runs the graph in eager Celery tasks. It needs the `celery` extra and the
  `agentic-celery` package (`pip install -e 'ports/experimental/celery[celery]'`).
* `nats` needs the `nats` extra, the `agentic-nats` package, and a JetStream server
  (`podman run -d --name nats-js -p 4222:4222 nats:latest -js`, or `AGENTIC_NATS_URL`).
* `ray` needs the `ray` extra and the `agentic-ray` package; it starts a local Ray cluster
  in-process (`AGENTIC_RAY_ADDRESS` points it at an existing one) and `close()` shuts it down.

An unknown name raises `ValueError: unknown backend 'x'; supported backends: celery, local,
nats, ray`. The other Python adapters under `ports/experimental/` are demonstration-only and
are rejected by name with the reason: `--backend airflow` (an orchestration plane, one DAG run
per turn), `dask` (a batch data plane), `faust` (a Kafka worker with no Kafka-backed end to end
test in this repository) and `gateway-fastapi` (an HTTP edge over the local, celery and nats
backends). The message lists every demonstration-only adapter and the supported backends;
`agentic_pipeline.backends.demonstration_only_names()` returns the same list.

The `celery`, `nats` and `ray` adapters live under `ports/experimental/` because they predate
the `agentic/v1` spec and are not conformance tested; `local` is the only backend on the
acceptance path. When an engine or adapter package is missing, `make_backend` raises
`BackendUnavailableError` naming the exact install step. Java and Go have sibling loaders
against the same schema (`jagentic-core` + Jackson-YAML, `goagentic` + yaml.v3).

Tested in `tests/test_pipeline.py`: the same `banking.yaml` runs on local, celery (skipped with
the install message when the adapter is absent), nats (skipped when no server is up) and ray
(skipped when Ray is not installed) with identical routing; the registry is exactly the four
names above and every demonstration-only adapter is rejected with the message naming it; the
LLM variant runs a deterministic ReAct turn via the stub provider; guardrails block; unknown
backends and missing `llm.model` fail with the messages above.
