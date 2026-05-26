"""Smoke tests for the PyFlink example scripts: build the plan offline and
verify the shape. We don't try to spin up a real PyFlink job here — that
requires the apache-flink wheel and a working JVM on PATH, which are CI-only
concerns. The plan-builder test gives us 95% of the coverage value with
zero infrastructure.
"""

from __future__ import annotations

import pytest

pytest.importorskip("cloudpickle")

from agentic_flink.examples.pyflink_quickstart import TriageAgent  # noqa: E402
from agentic_flink.examples.pyflink_research import ResearchAgent  # noqa: E402
from agentic_flink.pyflink.plan import build_plan  # noqa: E402
from agentic_flink.pyflink.serialize import decode  # noqa: E402


def test_quickstart_plan_shape():
    plan = build_plan(TriageAgent())
    assert plan["agent_id"] == "triage-quickstart"
    assert plan["chat_connection"]["fqn"].endswith("LangChain4jChatConnection")
    assert {t["name"] for t in plan["tools"]} == {"classify_intent"}
    assert {a["name"] for a in plan["actions"]} == {"draft_reply"}


def test_quickstart_classifier_callable():
    plan = build_plan(TriageAgent())
    fn = decode(
        next(t for t in plan["tools"] if t["name"] == "classify_intent")["cloudpickle_b64"]
    )
    assert fn("Please refund me") == "billing"
    assert fn("App is broken") == "technical"
    assert fn("How do I switch plans?") == "general"


def test_research_plan_carries_embedder_resource():
    plan = build_plan(ResearchAgent())
    assert "embedder" in plan["resources"]
    assert plan["resources"]["embedder"]["fqn"].endswith("DjlEmbeddingConnection")


def test_research_action_runs_locally():
    plan = build_plan(ResearchAgent())
    answer = next(a for a in plan["actions"] if a["name"] == "answer")
    fn = decode(answer["cloudpickle_b64"])
    out = fn(
        {"id": "q-x", "query": "Compare BERT encoder embed models"},
        {"agent_id": "research-bot"},
    )
    assert out["topic"] == "ml"
    assert out["n_hits"] >= 1
    assert out["agent"] == "research-bot"
