"""agentic-airflow: the router/path/verifier graph as an Airflow 3 branching DAG.

The task bodies (``classify``, ``run_path``, the ingestion steps) are plain functions on
the core and always run; ``simulate`` executes the DAG's logic without a scheduler. The
DAG module itself imports ``airflow.sdk`` (Airflow 3), so its structure is asserted only
when Airflow is installed.
"""

from __future__ import annotations

import random

import pytest
from adapter_support import (
    BALANCE_TEXTS,
    CARD_TEXTS,
    FRAUD_TEXTS,
    GENERAL_TEXTS,
    import_adapter,
    pick,
    random_conversation_id,
)
from pyagentic.memory import InMemoryConversationStore

pytestmark = pytest.mark.airflow


@pytest.fixture
def af():
    return import_adapter("agentic_airflow")


def test_classify_returns_a_path_task_id_for_every_route(af):
    assert af.classify(pick(CARD_TEXTS)) == "path_cards"
    assert af.classify(pick(BALANCE_TEXTS)) == "path_payments"
    assert af.classify(pick(GENERAL_TEXTS)) == "path_general"
    assert set(af.path_task_ids()) == {"path_cards", "path_payments", "path_general"}


def test_simulate_runs_the_branch_and_the_path_consistently(af):
    cid = random_conversation_id("af")
    pay = af.simulate(pick(BALANCE_TEXTS), conversation_id=cid)
    assert pay["path"] == "payments" and pay["ok"] is True and "get_balance" in pay["tool_calls"]
    assert af.simulate(pick(CARD_TEXTS), conversation_id=cid)["path"] == "cards"
    assert af.simulate(pick(GENERAL_TEXTS), conversation_id=cid)["path"] == "general"


def test_injected_store_carries_the_transcript_across_dag_runs(af):
    cid = random_conversation_id("mem")
    store = InMemoryConversationStore()
    af.configure(store=store)
    try:
        for texts in (BALANCE_TEXTS, CARD_TEXTS, GENERAL_TEXTS):
            af.simulate(pick(texts), conversation_id=cid)
        assert store.message_count(cid) == 6
        assert af._store() is store
    finally:
        af.configure()
    assert af._store().message_count(cid) == 0


def test_simulate_fails_loudly_when_branch_and_path_disagree(af, monkeypatch):
    monkeypatch.setattr(af, "classify", lambda text, conversation_id="airflow", user_id="airflow": "path_general")
    with pytest.raises(RuntimeError, match="disagrees"):
        af.simulate(pick(BALANCE_TEXTS), conversation_id=random_conversation_id("x"))


def test_extended_core_graph_adds_a_branch_target(af, extension):
    graph, tools = extension
    af.configure(graph=graph, tools=tools)
    try:
        assert "path_fraud" in af.path_task_ids()
        res = af.simulate(pick(FRAUD_TEXTS), conversation_id=random_conversation_id("fr"))
        assert res["path"] == "fraud" and "freeze_card" in res["tool_calls"] and "FRZ-" in res["reply"]
        assert af.simulate(pick(BALANCE_TEXTS), conversation_id=random_conversation_id("ok"))["path"] == "payments"
    finally:
        af.configure(graph=af.build_banking_graph(), tools=af.default_tools())


def test_ingestion_steps_embed_and_index_the_corpus(af):
    docs = af.load_corpus()
    sample = dict(random.sample(sorted(docs.items()), k=random.randint(1, len(docs))))
    vectors = af.embed_corpus(sample)
    assert set(vectors) == set(sample)
    assert {len(v) for v in vectors.values()} == {64}
    assert af.build_index(vectors, sample) == len(sample)
    assert af.build_index({}, {}) == 0


def test_dag_module_declares_the_branching_and_ingestion_dags(af, monkeypatch, tmp_path):
    pytest.importorskip(
        "airflow.sdk", reason="Airflow 3 (airflow.sdk) not installed; the DAG module needs the 'airflow' extra"
    )
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
    from agentic_airflow import dags

    triage = dags.routed_triage_dag
    assert triage.dag_id == "routed_triage"
    assert set(triage.task_ids) == {"route", "verify", *af.path_task_ids()}
    assert triage.get_task("route").downstream_task_ids == set(af.path_task_ids())
    for task_id in af.path_task_ids():
        assert triage.get_task(task_id).downstream_task_ids == {"verify"}
    assert triage.get_task("verify").trigger_rule == dags.TriggerRule.ONE_SUCCESS

    ingestion = dags.agentic_ingestion_dag
    assert ingestion.dag_id == "agentic_ingestion"
    assert list(ingestion.task_ids) == ["load", "embed", "build_index"]
    assert ingestion.get_task("load").downstream_task_ids == {"embed", "build_index"}
    assert ingestion.get_task("embed").downstream_task_ids == {"build_index"}


def test_airflow_import_resolves_to_the_installed_distribution(af):
    airflow = pytest.importorskip("airflow", reason="apache-airflow not installed")
    assert airflow.__file__ is not None and "agentic_airflow" not in airflow.__file__
    assert af.__file__.endswith("agentic_airflow/__init__.py")
