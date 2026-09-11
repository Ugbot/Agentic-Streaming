"""The high-level API: the fluent builder and ``load`` produce ``agentic/v1`` documents that
validate against ``spec/v1/workflow.schema.json``; ``spec.run`` executes on a named runtime."""

from __future__ import annotations

import json
import random
import string
from pathlib import Path

import jsonschema
import pytest

from agentic_flink import AgentSpec, Event, WorkflowAgent as Agent, WorkflowError, load, loads
from agentic_flink.workflow import workflow_requirements

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_SCHEMA = json.loads((REPO_ROOT / "spec" / "v1" / "workflow.schema.json").read_text())
RESULT_SCHEMA = json.loads((REPO_ROOT / "spec" / "v1" / "result.schema.json").read_text())
SUPPORT_YAML = REPO_ROOT / "spec" / "conformance" / "v1" / "workflows" / "support.yaml"


def _rand(n: int = 6) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


def _validate_workflow(doc) -> None:
    jsonschema.Draft202012Validator(WORKFLOW_SCHEMA).validate(doc)


def _builder(agent_id: str, keyword: str) -> Agent:
    def issue_refund(user: str, amount: float = 10.0) -> dict:
        return {"ok": True, "user": user, "amount": amount}

    return (
        Agent(agent_id)
        .route(kind="keyword", rules={"billing": [keyword, "charge"]}, default="general")
        .path("billing", brain="rule", prompt="Billing.", tools=["issue_refund"],
              tool_triggers={keyword: "issue_refund"}, guardrails=["no-secrets"])
        .path("general", brain="rule", prompt="General.")
        .verify("prefix")
        .use_tool("issue_refund", issue_refund)
        .guardrail("regex", name="no-secrets", deny=["password"], reason="secrets")
        .with_memory(conversation="memory")
        .policies(ordering="per-conversation", idempotency="turn-id", retry="exponential")
    )


def test_builder_document_validates_against_workflow_schema():
    keyword = _rand()
    spec = _builder(f"support-{_rand()}", keyword).build()
    assert isinstance(spec, AgentSpec)
    doc = spec.to_dict()
    _validate_workflow(doc)
    assert doc["spec_version"] == "agentic/v1"
    assert doc["agent"]["router"]["rules"]["billing"] == [keyword, "charge"]
    assert doc["agent"]["verifier"] == {"kind": "prefix"}
    assert doc["policies"]["retry"] == {"kind": "exponential"}
    assert [t["kind"] for t in doc["tools"]] == ["function"]
    assert set(spec.bindings) == {"issue_refund"}


def test_builder_rejects_values_outside_the_ir():
    with pytest.raises(WorkflowError, match="brain"):
        Agent("a").path("p", brain="billing-brain")
    with pytest.raises(WorkflowError, match="router kind"):
        Agent("a").route(kind="magic")
    with pytest.raises(WorkflowError, match="ordering"):
        Agent("a").policies(ordering="global")
    with pytest.raises(WorkflowError):
        Agent("a").route().build()  # a path is required


def test_spec_is_a_read_only_mapping_and_roundtrips_yaml_and_json(tmp_path: Path):
    spec = _builder("support", "refund").build()
    assert spec["agent"]["id"] == "support"
    assert spec.agent_id == "support"
    assert len(spec) == len(spec.to_dict())
    with pytest.raises(TypeError):
        spec["agent"] = {}  # type: ignore[index]

    y = tmp_path / "w.yaml"
    j = tmp_path / "w.json"
    y.write_text(spec.to_yaml())
    j.write_text(spec.to_json())
    assert load(y).to_dict() == spec.to_dict()
    assert load(j).to_dict() == spec.to_dict()
    assert loads(spec.to_yaml()).to_dict() == spec.to_dict()


def test_load_shared_support_workflow_and_requirements():
    spec = load(SUPPORT_YAML)
    _validate_workflow(spec.to_dict())
    req = workflow_requirements(spec.to_dict())
    assert req["routing"].startswith("agent.router")
    assert "rule_brain" in req and "tools" in req


def test_load_rejects_non_v1_documents(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("spec_version: agentic/v2\nagent: {id: a, paths: {p: {}}}\n")
    with pytest.raises(WorkflowError, match="spec_version"):
        load(bad)
    assert AgentSpec({"agent": {"id": "a", "paths": {"p": {}}}})["spec_version"] == "agentic/v1"
    with pytest.raises(WorkflowError, match="agent"):
        AgentSpec({"spec_version": "agentic/v1"})


def test_event_factories():
    e = Event.turn("c1", "t1", "hi")
    assert (e.conversation_id, e.turn_id, e.text, e.user_id, e.is_resume) == ("c1", "t1", "hi", "anonymous", False)
    r = Event.resume("c1", "t2", {"kind": "approval", "approved": True})
    assert r.is_resume and r.signal == {"kind": "approval", "approved": True}
    with pytest.raises(WorkflowError):
        Event(conversation_id="", turn_id="t")


def test_spec_run_on_local_jvm_returns_normalized_result(af):
    keyword = _rand()
    spec = _builder(f"support-{_rand()}", keyword).build()
    result = spec.run(runtime="local-jvm", text=f"please {keyword} me", conversation_id="c1", turn_id="t1")
    jsonschema.Draft202012Validator(RESULT_SCHEMA).validate(result)
    assert result["status"] == "completed"
    assert result["path"] == "billing"
    assert result["tool_calls"][0]["tool"] == "issue_refund"
    assert result["tool_calls"][0]["result"] == {"ok": True, "user": "anonymous", "amount": 10.0}
    assert [e["type"] for e in result["events"]][:3] == ["turn_received", "routed", "brain_started"]
    assert type(result["state"]["turn_count"]) is int


def test_spec_run_local_alias_and_guardrail(af):
    spec = _builder("support", "refund").build()
    result = spec.run(runtime="local", text="my password is hunter2", conversation_id="c2", turn_id="t1")
    jsonschema.Draft202012Validator(RESULT_SCHEMA).validate(result)
    assert result["status"] == "rejected"
    assert result["error"]["class"] == "guardrail"
