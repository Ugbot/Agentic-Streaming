"""Agentic-Flink on **Apache Airflow** — pure Python, the ORCHESTRATION plane.

See ../../docs/portability/airflow.md. Airflow isn't streaming, but the
router->path->verifier topology maps cleanly to a branching DAG:
  router = @task.branch  ->  path tasks (cards/payments/general)  ->  verifier (fan-in).
Per-conversation state lives in an external store (Redis/Postgres) via the
``pyagentic.ConversationStore`` SPI; XCom carries only the small per-run payload.

This module defines two DAGs when Airflow is installed (a routed-triage DAG and a
RAG-ingestion DAG) and also exposes a model-free ``simulate()`` that runs the same
routing logic through ``pyagentic`` so the wiring is verifiable without a scheduler.

Deploy: drop this file in your Airflow ``dags/`` folder (with pyagentic installed).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pyagentic"))

from pyagentic.banking import (  # noqa: E402
    KB,
    banking_router,
    build_banking_graph,
    default_tools,
    seed_kb,
)
from pyagentic.core import AgentContext, Event  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever, hashing_embedder  # noqa: E402

_EMBED = hashing_embedder(64)


def classify(text: str) -> str:
    """The router as a pure function — used both by the DAG's branch task and by
    ``simulate``. Returns the task id to branch to."""
    ctx = AgentContext("airflow", "airflow", InMemoryConversationStore(), InMemoryKeyedStateStore(),
                       default_tools(), None)
    return "path_" + banking_router(Event("airflow", text), ctx)


def run_path(path: str, text: str, conversation_id: str) -> Dict[str, object]:
    """Run a single path agent + verifier through the portable graph (one path
    actually executes per run; the branch skips the others)."""
    graph = build_banking_graph()
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    ctx = AgentContext(conversation_id, "airflow", InMemoryConversationStore(),
                       InMemoryKeyedStateStore(), default_tools(), TwoTierRetriever(hot, None, 4, 4))
    res = graph.handle(Event(conversation_id, text, "airflow"), ctx)
    return {"path": res.path, "reply": res.reply, "ok": res.ok}


def simulate(text: str, conversation_id: str = "sim") -> Dict[str, object]:
    """Model-free end-to-end of the DAG's logic without a scheduler (for tests)."""
    branch = classify(text)
    return run_path(branch.removeprefix("path_"), text, conversation_id)


# ---- Airflow DAG definitions (only when Airflow is importable) ----
try:
    from airflow.decorators import dag, task
    from airflow.utils.dates import days_ago
    _HAS_AIRFLOW = True
except ImportError:
    _HAS_AIRFLOW = False


if _HAS_AIRFLOW:

    @dag(schedule=None, start_date=days_ago(1), catchup=False, tags=["agentic"])
    def routed_triage():
        """Router (branch) -> one path task -> verifier (fan-in). Trigger with a
        conf {"text": "...", "conversation_id": "..."}."""

        @task.branch
        def route(**ctx) -> str:
            text = (ctx["dag_run"].conf or {}).get("text", "")
            return classify(text)

        @task
        def path_cards(**ctx):
            conf = ctx["dag_run"].conf or {}
            return run_path("cards", conf.get("text", ""), conf.get("conversation_id", "af"))

        @task
        def path_payments(**ctx):
            conf = ctx["dag_run"].conf or {}
            return run_path("payments", conf.get("text", ""), conf.get("conversation_id", "af"))

        @task
        def path_general(**ctx):
            conf = ctx["dag_run"].conf or {}
            return run_path("general", conf.get("text", ""), conf.get("conversation_id", "af"))

        @task(trigger_rule="one_success")
        def verify(*paths):
            # fan-in: pick the path that ran, assert the verifier passed.
            result = next((p for p in paths if p), None)
            assert result and result["ok"], f"verification failed: {result}"
            return result

        branch = route()
        cards, payments, general = path_cards(), path_payments(), path_general()
        branch >> [cards, payments, general] >> verify(cards, payments, general)

    @dag(schedule="@daily", start_date=days_ago(1), catchup=False, tags=["agentic", "rag"])
    def agentic_ingestion():
        """RAG cold-index build: (load -> chunk -> embed -> index). Sensors/retries/
        backfill are exactly what Airflow is for."""

        @task
        def load() -> Dict[str, str]:
            return dict(KB)

        @task
        def embed(docs: Dict[str, str]) -> Dict[str, list]:
            return {doc_id: _EMBED(text) for doc_id, text in docs.items()}

        @task
        def build_index(vectors: Dict[str, list], docs: Dict[str, str]) -> int:
            index = InMemoryHotVectorIndex(max_entries=10_000)
            for doc_id, vec in vectors.items():
                index.upsert(doc_id, vec, docs[doc_id])
            # production: persist to pgvector/Qdrant/Fluss here.
            return index.size()

        docs = load()
        build_index(embed(docs), docs)

    routed_triage_dag = routed_triage()
    agentic_ingestion_dag = agentic_ingestion()


if __name__ == "__main__":  # pragma: no cover
    for text in ["what card types do you offer?", "dispute a charge", "hello"]:
        print(text, "->", simulate(text))
