"""The high-level surface: `Agent` builder, `AgentSpec`, `load()`, `run()`."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from agentic import Agent, AgentSpec, ValidationError, get_runtime, load, register_runtime
from agentic.conformance import fixtures_dir
from agentic.ir import validate_document, validate_result
from agentic.runtime import LocalRuntime, unregister_runtime


def rid(prefix: str = "id") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def test_contract_example_builds_a_schema_valid_document_and_runs():
    refunds = []

    def issue_refund(user: str) -> str:
        refunds.append(user)
        return "refunded"

    agent = (Agent("support")
             .route(kind="keyword", rules={"billing": ["refund", "charge"]}, default="general")
             .path("billing", brain="billing-brain", tools=["issue_refund"], guardrails=["authenticated"],
                   verifier="billing-verifier")
             .path("general")
             .brain("billing-brain", lambda ctx: f"[billing] {ctx.invoke('issue_refund', {'user': ctx.user_id})}")
             .verifier("billing-verifier", fn=lambda reply: reply.startswith("[billing]"))
             .guardrail("authenticated", fn=lambda text: None if "anonymous" not in text else "sign in first")
             .use_tool("issue_refund", issue_refund)
             .with_memory("memory")
             .policies(ordering="per-conversation", idempotency="turn-id", retry="exponential"))
    spec = agent.build()
    assert isinstance(spec, AgentSpec)
    validate_document(spec.document)
    assert spec.document["policies"]["retry"] == {"kind": "exponential", "max_attempts": 3}
    result = spec.run(runtime="local", text="refund me", conversation_id="c1", turn_id="t1", user_id="u1")
    validate_result(result)
    assert result["status"] == "completed" and result["path"] == "billing" and refunds == ["u1"]
    assert spec.run(runtime="local", text="refund me", conversation_id="c1", turn_id="t1")["status"] == "duplicate"
    blocked = spec.run(runtime="local", text="refund anonymous", conversation_id="c1", turn_id="t2")
    assert blocked["status"] == "rejected"
    spec.close()


def test_run_requires_a_turn_id():
    spec = Agent(rid("a")).path("main").build()
    with pytest.raises(ValidationError, match="turn_id"):
        spec.run(text="hi", conversation_id="c")


def test_single_path_gets_a_static_router_and_build_needs_a_path():
    spec = Agent(rid("a")).path("only").build()
    assert spec.document["agent"]["router"] == {"kind": "static", "default": "only"}
    assert spec.run(text="anything", conversation_id=rid("c"), turn_id="t")["path"] == "only"
    with pytest.raises(ValidationError, match="path"):
        Agent(rid("a")).build()


def test_builder_errors_are_validation_errors_with_pointers():
    with pytest.raises(ValidationError) as info:
        Agent(rid("a")).route(rules={"ghost": ["x"]}, default="main").path("main").build()
    assert info.value.pointer == "/agent/router/rules/ghost"
    with pytest.raises(ValidationError, match="deny=, allow=, or fn="):
        Agent(rid("a")).guardrail("empty")


def test_load_from_yaml_and_json_round_trip(tmp_path: Path):
    spec = (Agent(rid("a")).route(rules={"b": ["x"]}, default="g").path("b").path("g")
            .tool("t", "constant", value=1).build())
    for name in ("w.yaml", "w.json"):
        target = tmp_path / name
        spec.save(str(target))
        again = load(target)
        assert again.document == spec.document
    assert load(spec.to_yaml()).document == spec.document
    assert load(spec.to_json()).document == spec.document


def test_load_binds_function_tools_and_peers_by_name(tmp_path: Path):
    doc = (Agent(rid("a")).path("main", tool_triggers={"go": "py", "call": "peer"})
           .tool("py", "function", description="python").peer("peer", "inproc").verifier("none").document())
    path = tmp_path / "w.yaml"
    AgentSpec(doc).save(str(path))
    with pytest.raises(ValidationError, match="no bound Python function"):
        get_runtime("local").deploy(load(path))
    spec = load(path, py=lambda user: f"py:{user}", peer=lambda args, delegation: f"peer:{delegation.turn_id}")
    assert spec.run(text="go", conversation_id="c", turn_id="t1")["tool_calls"][0]["result"] == "py:anonymous"
    assert spec.run(text="call", conversation_id="c", turn_id="t2")["tool_calls"][0]["result"] == "peer:t2"
    with pytest.raises(ValidationError, match="neither"):
        load(path, ghost=lambda: None)


def test_shared_support_workflow_loads_and_runs_in_place():
    spec = load(fixtures_dir().parent / "workflows" / "support.yaml")
    assert spec.id == "support"
    result = spec.run(text="what is my balance?", conversation_id=rid("c"), turn_id="t1")
    assert result["path"] == "billing" and result["tool_calls"][0]["result"] == 42.5


def test_run_goes_through_the_registered_runtime_api():
    name = rid("rt")
    deployed = []

    class Recording(LocalRuntime):
        def deploy(self, spec):
            deployed.append(spec)
            super().deploy(spec)

    register_runtime(name, Recording)
    try:
        spec = Agent(rid("a")).path("main").build()
        spec.run(runtime=name, text="hi", conversation_id="c", turn_id="t")
        spec.run(runtime=name, text="hi", conversation_id="c", turn_id="t2")
        assert deployed == [spec]
        assert isinstance(spec.runtime(name), Recording)
    finally:
        unregister_runtime(name)
        spec.close()


def test_repr_is_notebook_friendly():
    spec = Agent(rid("a")).path("main").build()
    text = repr(spec)
    assert "paths=[main]" in text and "requires=" in text
