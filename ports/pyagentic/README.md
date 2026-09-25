# pyagentic

Pure-Python implementation of the Agentic Streaming v1 model (`spec/v1`). One workflow IR,
one user-facing `agentic` API, and the local runtime that is the Python reference for the
shared conformance fixtures. It is the `python` column of the generated
[capability matrix](../../docs/capabilities.md); what the runtime is made of and where it
stands is on the [pure Python runtime page](../../docs/runtimes/python.md), and the two-level
Python API (this package, the JVM facade and PyFlink on the same workflow) is in
[docs/python.md](../../docs/python.md).

```python
from agentic import Agent, load

agent = (Agent("support")
         .route(kind="keyword", rules={"billing": ["refund", "charge"]}, default="general")
         .path("billing", brain="billing-brain", tools=["issue_refund"],
               guardrails=["authenticated"], verifier="billing-verifier")
         .path("general")
         .brain("billing-brain", lambda ctx: f"[billing] {ctx.invoke('issue_refund', {'user': ctx.user_id})}")
         .verifier("billing-verifier", fn=lambda reply: reply.startswith("[billing]"))
         .guardrail("authenticated", fn=lambda text: None if "anonymous" not in text else "sign in first")
         .use_tool("issue_refund", lambda user: "refunded")
         .with_memory("memory")
         .policies(ordering="per-conversation", idempotency="turn-id", retry="exponential"))

spec = agent.build()                                   # AgentSpec, valid against workflow.schema.json
spec = load("spec/conformance/v1/workflows/support.yaml")
result = spec.run(runtime="local", text="refund me", conversation_id="c1", turn_id="t1")
# result is a mapping valid against spec/v1/result.schema.json
```

Full control:

```python
from agentic.runtime import Runtime, get_runtime, register_runtime

rt = get_runtime("local")          # entry-point group `agentic.runtimes`, or register_runtime(name, factory)
rt.capabilities()                  # {capability: "supported" | "partial" | "unsupported" | "not_tested"}
rt.deploy(spec)                    # CapabilityError lists every unsupported requirement
rt.submit(Turn("c1", "t1", "refund me"))
rt.close()
```

Selecting a runtime whose package is not installed raises `RuntimeNotAvailableError` naming the
extra (`pip install 'pyagentic[flink]'`); nothing falls back silently.

## Layout

- `agentic/ir.py` -- YAML/JSON loading, schema validation (bundled copies of `spec/v1/*.schema.json`,
  kept byte-identical by `tests/test_agentic_ir.py`), cross-field rules, unknown-field policy.
- `agentic/events.py` -- closed v1 event set, dense per-conversation sequences, `reduce_state` fold,
  in-memory and file-backed (JSONL, fsync) event logs.
- `agentic/engine.py` -- the semantics: guardrails, routing, brains, verification bounds, structured
  tool calls, retry, saga compensation, memory, retrieval, suspend/resume, A2A, idempotency, replay.
- `agentic/runtime.py` -- `Runtime` ABC, registry, `LocalRuntime` (single writer per conversation).
- `agentic/spec.py` -- `Agent` builder, `AgentSpec`, `load()`; implemented on top of `Runtime`.
- `agentic/conformance.py` -- runs `spec/conformance/v1/fixtures/*.yaml` in place
  (`python -m agentic.conformance`); `matrix_binding` is the `agentic.conformance` entry point
  that `spec/tools/conformance_matrix.py` drives.
- `pyagentic/` -- the earlier engine-agnostic essence kept for the Faust/Ray/Dask/Airflow ports.

## Classes named `Agent`

Several Python classes are called `Agent`. None is an alias of another; each has a distinct job,
so they keep their names and are told apart by module.

| Class | What it is | Use it when |
|---|---|---|
| `agentic.Agent` (`agentic/spec.py`) | Fluent builder for a v1 workflow document. `build()` returns an `AgentSpec`; `AgentSpec.run(...)` deploys it on a `Runtime`. | You write a workflow in Python instead of YAML. This is the canonical API. |
| `pyagentic.core.Agent` | A path handler: `agent_id`, `system_prompt`, a `Brain`. `RoutedGraph` maps each path name to one of these. | You build a `RoutedGraph` by hand or through `pyagentic.builder.build()` for the Faust, Ray, Dask, Airflow, Celery and NATS ports. |
| `agentic_flink.Agent` (`python/agentic_flink/agent.py`) | JPype handle on the Java `org.agentic.flink.dsl.Agent`, built with `Agent.builder()`. | You drive the Flink framework in process through the `agentic-flink` package. |
| `agentic_flink.workflow.Agent` (exported as `agentic_flink.WorkflowAgent`) | The same fluent builder API as `agentic.Agent`, producing a document the JVM runtimes (`local-jvm`, `flink-jvm`, `pekko`) run. | You want the builder syntax and a JVM runtime without installing `pyagentic`. |
| `agentic_flink.pyflink.Agent` | Declarative base class whose subclass attributes describe a PyFlink job plan. | You compile an agent into a PyFlink `DataStream` plan. |

Routing is the same everywhere: `router.kind: keyword` matches `router.rules` case-insensitively as a
substring of the turn text, the first path in declaration order wins, and no match goes to
`router.default` (`agentic.engine.Engine._route`, `pyagentic.core.keyword_router`). `router.kind: static`
sends every turn to `router.default`. The `llm` and `classifier` kinds are valid in the schema but no
Python runtime implements them; both engines fail with a message that lists the supported kinds.

## Develop

```
uv sync                                             # .venv from uv.lock (or: python -m venv .venv && .venv/bin/pip install -e '.[dev]')
.venv/bin/pytest
.venv/bin/ruff check agentic tests/test_agentic_*.py
.venv/bin/mypy
.venv/bin/python -m agentic.conformance             # the fixtures against the local runtime (the `python` column)
uv lock                                             # refresh uv.lock after changing dependencies
```

The version is not written in `pyproject.toml`; setuptools-scm derives it from the repository's
release tag (see `docs/release.md`).
