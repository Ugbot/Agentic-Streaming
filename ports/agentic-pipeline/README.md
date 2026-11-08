# agentic-pipeline: declarative YAML to a running agentic system

Describe the agentic system in a `pipeline.yaml` and the loader runs it on the `agentic` core, the
local runtime the conformance fixtures hold to account.

## Install

`agentic-pipeline` is a regular package. It depends on the `pyagentic` distribution, which holds the
`agentic` core (not on PyPI), and `PyYAML`, so install the core from the checkout first:

```bash
pip install -e ports/pyagentic -e ports/agentic-pipeline
```

No `PYTHONPATH` is needed for the package itself. Tests run with
`python -m pytest ports/agentic-pipeline`.

```bash
python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"
# backend=local path=payments ok=True status=completed
# reply: [payments] get_balance returned 1234.56
# tools: ['get_balance']
```

## What the YAML expresses

The examples in [`examples/pipelines/`](../../examples/pipelines/) cover it: `banking.yaml` (rule
brains), `banking-llm.yaml` (a model drives the tools), `banking-rag.yaml` (an HNSW knowledge base, a
classifier guardrail, skills and a token budget), `tools-mcp.yaml` and `banking-mcp.yaml` (tools from
an MCP server), `multiagent.yaml` (a peer agent over A2A) and `incident.yaml` (a `cep` pattern that
injects a turn). Sections: `backend`, `llm` (`provider` `ollama`, `openai`, `anthropic`, `gemini` or
`stub`; `model` is required except for `anthropic`, which defaults to `claude-sonnet-5-5`),
`agent.router`, `agent.paths`, `agent.verifier`, `tools`, `mcp`, `a2a`, `skills`, `retrieval`,
`guardrails`, `context`, `cep`, `policies` and `stores`. The schema is
[`spec/v1/workflow.schema.json`](../../spec/v1/workflow.schema.json).

## Every pipeline runs on the core

A turn is on the event log, deduplicated by `turn_id`, under the tool policy, and its effects are
journaled:

```bash
python -m agentic_pipeline run examples/pipelines/banking.yaml --store-dir state --turn t1 --text "what is my balance?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --store-dir state --turn t1 --text "what is my balance?"
# the second run prints status=duplicate and the recorded reply
```

Without `--turn` every run is a new turn. `--store-dir` keeps the log and the journal on disk.
`--meta key=value` sets turn metadata, such as the event time `incident.yaml` reads:

```bash
for n in 0 1 2; do
  python -m agentic_pipeline run examples/pipelines/incident.yaml --store-dir state --conv host-7 \
    --turn a$n --meta ts=$((n * 60000)) --text "anomaly: cpu high"
done
# the third prints derived=a2/cep/host_incident:completed: the injected turn opened a ticket
```

A document that names something the core cannot do is refused when it loads, with the reason: a
conversation store other than `memory` or `file`, a `long_term`, `keyed_state` or `vector` store other
than `memory`, an embedder other than `hashing`, a vector store other than `memory` or `hnsw`, a
classifier other than `lexicon`, an MCP transport other than `stdio`. `on_unavailable: degrade` on a
store takes memory instead, and says so in the result.

An `mcp:` server starts only if the operator lists its command in `AGENTIC_MCP_ALLOWED_COMMANDS`. An
`a2a` peer over `http` is called at `<url>/agent`; a `${NAME}` url is expanded at the call, so a turn
that never delegates needs no peer.

## Pieces

| Module | Role |
|--------|------|
| `agentic_pipeline.backends` | `CoreBackend`, the `agentic` local runtime, and the backend registry |
| `agentic_pipeline.loader` | `load(path)` / `build_system(spec)` → a `PipelineSystem` with `submit(Event)` |
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
