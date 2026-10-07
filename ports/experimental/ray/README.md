# agentic-ray: Agentic Streaming on Ray

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

One Ray actor per conversation: a single-writer, ordered, in-memory state holder running the
router -> path -> verifier graph from the pure `pyagentic` core. Design:
[`../../../docs/portability/ray.md`](../../../docs/portability/ray.md). Ray runs a local cluster
in-process, so it is a `backend:` of `ports/agentic-pipeline` (`backend: ray`).

| Symbol | Role |
|--------|------|
| `agentic_ray.RayRuntime` | `pyagentic.Runtime` over Ray; `deps_factory` injects the graph, `namespace` isolates actors, `close()` kills them |
| `agentic_ray.ConversationAgent` | the per-conversation actor; its fields are the keyed state |
| `agentic_ray.banking_deps` | the default factory: the banking graph, tools and retriever |

## Run

```bash
pip install -e ../../pyagentic -e '.[ray]'
python -m agentic_ray
AGENTIC_RAY_ADDRESS=ray://head:10001 python -m agentic_ray   # against an existing cluster
```

Tests: `pytest ports/experimental/tests/test_ray.py` from the repo root. The actor tests are
marked `ray` and skip, with the reason printed, only when Ray is not installed.
