# agentic-pipeline — declarative YAML → the agentic system of your choice

Build the agentic system you want in a `pipeline.yaml`, choose a backend, and the rest
falls into place. The same spec runs on **any** backend because every engine adapter is
injectable and the core `GraphBuilder` compiles the spec into the engine-agnostic
`RoutedGraph` + tools + retriever.

```bash
PYTHONPATH=ports/agentic-pipeline:ports/pyagentic \
  python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
# backend=local path=payments ok=True
# reply: [payments] get_balance returned 1234.56
# tools: ['get_balance']

# same YAML, different backend — nothing else changes:
... run examples/pipelines/banking.yaml --backend celery --text "tell me about crypto cash-back"
... run examples/pipelines/banking.yaml --backend nats   --text "what is my balance?"
```

## What the YAML expresses

See [`examples/pipelines/banking.yaml`](../../examples/pipelines/banking.yaml) (rule
brains) and [`banking-llm.yaml`](../../examples/pipelines/banking-llm.yaml) (an LLM
ReAct path). Sections: `backend`, optional `llm` (provider + model; `stub` for offline),
`agent.router` (keyword rules), `agent.paths` (per-path brain `rule|llm`, prompt, tools,
`tool_triggers`), `agent.verifier`, `tools` (`constant`/`http`), `retrieval` (hashing
embedder + KB), `guardrails` (regex deny-lists).

## Pieces

| Module | Role |
|--------|------|
| `pyagentic.builder` | `build(spec, chat_client_factory) -> (graph, tools, retriever)` — the core GraphBuilder (engine-agnostic) |
| `agentic_pipeline.backends` | `make_backend(name, graph, tools, retriever)` — the shim: `local` / `celery` / `nats` (all injectable, uniform `submit(Event)`) |
| `agentic_pipeline.loader` | `load(path)` / `build_system(spec)` → a `PipelineSystem` with `submit(Event)` on the chosen backend |
| `agentic_pipeline.__main__` | the `run` CLI |

`local` and `celery` run offline; `nats` needs a JetStream server
(`podman run -p 4222:4222 nats:latest -js`). Faust/Ray/Dask/Airflow host the same built
graph via their own injectable seams (Phase 2). Java and Go get sibling loaders against
the same schema (`jagentic-core` + Jackson-YAML, `goagentic` + yaml.v3).

Tested in `tests/test_pipeline.py`: the same `banking.yaml` runs on local + celery (and
nats when a server is up) with identical routing; the LLM variant runs a deterministic
ReAct turn via the stub provider; guardrails block.
