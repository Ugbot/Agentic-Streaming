# agentic-faust: Agentic Streaming on Faust

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

A keyed Faust agent (faust-streaming) hosting the router -> path -> verifier graph, with
Faust Tables as the durable per-conversation `ConversationStore`. Design:
[`../../../docs/portability/faust.md`](../../../docs/portability/faust.md). The worker needs a
Kafka broker, and no Kafka-backed end to end test ships in this repository, so it is
demonstration-only and not a `backend:` of `ports/agentic-pipeline`.

| Symbol | Role |
|--------|------|
| `agentic_faust.FaustTableConversationStore` | `ConversationStore` over two mutable mappings (Faust Tables in the worker, dicts in tests) |
| `agentic_faust.handle_turn` | load -> handle -> save for one record of a conversation's partition |
| `agentic_faust.configure` | inject a graph, tools, retriever or keyed state |
| `agentic_faust.app` | the Faust `App`, topics, tables and the `banking_agent` (needs the `faust` extra) |

## Run

```bash
pip install -e ../../pyagentic -e '.[faust]'
python -m agentic_faust                        # the turn logic over in-memory tables, no broker

AGENTIC_FAUST_BROKER=kafka://localhost:9092 faust -A agentic_faust.app worker -l info
# then produce JSON {"conversation_id": "c1", "user_id": "u", "text": "..."} to agentic.requests
```

Tests: `pytest ports/experimental/tests/test_faust.py` from the repo root. The app-wiring test
is marked `faust` and skips, with the reason printed, only when faust-streaming is not installed.
