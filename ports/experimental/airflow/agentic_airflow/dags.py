"""The two Airflow DAGs, on the Airflow 3 task SDK (``airflow.sdk``; TaskFlow operators
come from ``apache-airflow-providers-standard``, which the ``airflow`` extra pulls in).

Point ``AIRFLOW__CORE__DAGS_FOLDER`` at this package directory, or copy this file into
your dags folder with ``agentic-airflow`` installed. Trigger ``routed_triage`` with a
conf ``{"text": "...", "conversation_id": "..."}``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import agentic_airflow

try:
    from airflow.sdk import TriggerRule, dag, get_current_context, task
except ImportError as exc:
    raise ImportError(
        "agentic-airflow DAGs need the optional 'airflow' extra (Airflow 3): "
        "pip install 'agentic-airflow[airflow]'"
    ) from exc

__all__ = ["agentic_ingestion", "agentic_ingestion_dag", "routed_triage", "routed_triage_dag"]

START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _run_conf() -> Dict[str, str]:
    dag_run = get_current_context()["dag_run"]
    return dict(dag_run.conf or {})


def _turn() -> Dict[str, object]:
    conf = _run_conf()
    return agentic_airflow.run_path(conf.get("text", ""), conf.get("conversation_id", "af"))


@dag(dag_id="routed_triage", schedule=None, start_date=START_DATE, catchup=False, tags=["agentic"])
def routed_triage():
    """Router (branch) -> one path task -> verifier (fan-in)."""

    @task.branch
    def route() -> str:
        return agentic_airflow.classify(_run_conf().get("text", ""))

    @task(task_id="path_cards")
    def path_cards() -> Dict[str, object]:
        return _turn()

    @task(task_id="path_payments")
    def path_payments() -> Dict[str, object]:
        return _turn()

    @task(task_id="path_general")
    def path_general() -> Dict[str, object]:
        return _turn()

    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def verify(*paths):
        # fan-in: pick the path that ran, assert the verifier passed.
        result = next((p for p in paths if p), None)
        if not result or not result["ok"]:
            raise ValueError(f"verification failed: {result}")
        return result

    branch = route()
    cards, payments, general = path_cards(), path_payments(), path_general()
    branch >> [cards, payments, general]
    verify(cards, payments, general)


@dag(dag_id="agentic_ingestion", schedule="@daily", start_date=START_DATE, catchup=False, tags=["agentic", "rag"])
def agentic_ingestion():
    """RAG cold-index build: load -> embed -> index. Sensors/retries/backfill are
    exactly what Airflow is for."""

    @task
    def load() -> Dict[str, str]:
        return agentic_airflow.load_corpus()

    @task
    def embed(docs: Dict[str, str]) -> Dict[str, list]:
        return agentic_airflow.embed_corpus(docs)

    @task
    def build_index(vectors: Dict[str, list], docs: Dict[str, str]) -> int:
        return agentic_airflow.build_index(vectors, docs)

    docs = load()
    build_index(embed(docs), docs)


routed_triage_dag = routed_triage()
agentic_ingestion_dag = agentic_ingestion()
