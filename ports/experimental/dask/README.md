# agentic-dask: Agentic Streaming on Dask

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

The batch data plane: parallel RAG ingestion, retrieval eval (recall@k) and replay of the
routed graph over many transcripts, reusing the pure `pyagentic` core. Design:
[`../../../docs/portability/dask.md`](../../../docs/portability/dask.md). Dask is not a live
conversation runtime, so it is demonstration-only and not a `backend:` of `ports/agentic-pipeline`.

| Symbol | Role |
|--------|------|
| `agentic_dask.ingest_corpus` | embed a corpus in parallel, build the cold index |
| `agentic_dask.eval_recall` | recall@k over a list of `EvalCase`, parallel over queries |
| `agentic_dask.replay_graph` | replay (conversation_id, text) turns; parallel across conversations, ordered within one |
| `agentic_dask.parallel_backend` | `"dask"` or `"sequential"` (the optional extra is not installed) |

## Run

```bash
pip install -e ../../pyagentic -e '.[dask]'
python -m agentic_dask
```

Tests: `pytest ports/experimental/tests/test_dask.py` from the repo root. The Dask-bag test is
marked `dask` and skips, with the reason printed, only when Dask is not installed.
