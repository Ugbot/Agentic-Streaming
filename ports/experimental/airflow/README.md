# agentic-airflow: Agentic Streaming on Apache Airflow

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

The router -> path -> verifier graph as an Airflow 3 branching DAG, reusing the pure
`pyagentic` core. Design: [`../../../docs/portability/airflow.md`](../../../docs/portability/airflow.md).
Airflow is the orchestration plane (one DAG run per turn, batch RAG ingestion), not a live
conversation runtime, so it is demonstration-only and not a `backend:` of `ports/agentic-pipeline`.

| Symbol | Role |
|--------|------|
| `agentic_airflow.classify` / `run_path` | the branch task body and the path task body, pure functions over the core |
| `agentic_airflow.simulate` | the DAG's logic end to end without a scheduler (what the test runs) |
| `agentic_airflow.configure` | inject a graph, tools, retriever or a durable `ConversationStore` |
| `agentic_airflow.dags` | `routed_triage` and `agentic_ingestion`, on the Airflow 3 task SDK (`airflow.sdk`) |

## Run

```bash
pip install -e ../../pyagentic -e '.[airflow]'
python -m agentic_airflow                      # simulate routing + ingestion, no scheduler

export AIRFLOW__CORE__DAGS_FOLDER=$(python -c 'import agentic_airflow.dags as d, os; print(os.path.dirname(d.__file__))')
airflow dags test routed_triage --conf '{"text": "what is my balance?", "conversation_id": "c1"}'
```

Tests: `pytest ports/experimental/tests/test_airflow.py` from the repo root. The DAG-structure
test is marked `airflow` and skips, with the reason printed, only when Airflow 3 is not installed.
