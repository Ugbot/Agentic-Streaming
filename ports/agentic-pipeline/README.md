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

## Which engine runs a pipeline

A pipeline runs on the `agentic` core, the one the conformance fixtures hold to account. A turn is on
the event log, deduplicated by `turn_id`, and its effects are journaled:

```bash
python -m agentic_pipeline run examples/pipelines/banking.yaml --store-dir state --turn t1 --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --store-dir state --turn t1 --text "what is my balance?"
# the second run prints status=duplicate and the recorded reply
```

Without `--turn` every run is a new turn. `--store-dir` keeps the log and the journal on disk.

The core does not have everything. A document that declares any of these runs on the legacy
`pyagentic` engine instead, which has no event log, no `turn_id` and no effect journal, and the CLI
says so on standard error: `mcp`, an `a2a` peer that is not `inproc`, a conversation store other than
`memory` or `file`, `stores.long_term`, `stores.keyed_state`, `stores.vector`, a guardrail other than
`regex` or one with `check_outputs`, `skills`, `retrieval.vector_store`, embeddings other than
`hashing`, context compaction other than `none` or `window`, `context.max_tokens`, an `output_schema`
on a model path, tools of kind `agent` or `mcp`, and a `cep` rule that does anything but call a tool
on plain text conditions. `agentic_pipeline.legacy_features(spec)` lists what a document needs. Of the
examples, `banking` and `banking-llm` run on the core; `banking-rag`, `banking-mcp`, `tools-mcp`,
`multiagent` and `incident` do not.

## Pieces

| Module | Role |
|--------|------|
| `agentic_pipeline.backends` | `CoreBackend`, the `agentic` local runtime, and `make_backend(name, graph, tools, retriever)` for the legacy engine |
| `agentic_pipeline.loader` | `load(path)` / `build_system(spec)` → a `PipelineSystem` with `submit(Event)`; `legacy_features(spec)` |
| `pyagentic.builder` | `build(spec, chat_client_factory) -> (graph, tools, retriever)`, the legacy GraphBuilder |
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
