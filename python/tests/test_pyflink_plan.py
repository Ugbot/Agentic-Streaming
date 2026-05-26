"""Plan-builder unit tests. No JVM, no PyFlink — pure Python.

Requires the ``[pyflink]`` extra for cloudpickle. We test that:

1. Decorators tag methods with the expected metadata.
2. ``build_plan`` walks the class and produces the canonical JSON shape.
3. Cloudpickled callables round-trip back to working Python.
4. Subclass overrides win.
"""

from __future__ import annotations

import json
import uuid

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

from agentic_flink.pyflink import (  # noqa: E402
    Agent,
    ResourceRef,
    action,
    build_plan,
    tool,
)
from agentic_flink.pyflink.plan import build_plan_json  # noqa: E402
from agentic_flink.pyflink.serialize import decode  # noqa: E402


class _Triage(Agent):
    agent_id = "triage-" + str(uuid.uuid4())[:8]
    system_prompt = "You triage tickets."
    chat_connection = ResourceRef(
        "com.example.Chat", {"k": str(uuid.uuid4())}
    )

    def __init__(self):
        self.chat_setup = {"model": "qwen2.5:3b", "temperature": "0.3"}

    @tool
    def classify(self, text: str) -> str:
        return "refund" if "refund" in text.lower() else "general"

    @tool(name="lookup", description="Find a record")
    def _lookup_impl(self, customer_id: str) -> str:
        return f"customer:{customer_id}"

    @action("ticket")
    def handle(self, event, ctx):
        return {"id": event["id"], "label": self.classify(event["body"])}

    @action(["alert", "incident"])
    def handle_alert(self, event, ctx):
        return {"alert": event, "agent": ctx["agent_id"]}


def test_plan_has_expected_top_level_fields():
    plan = build_plan(_Triage())
    assert plan["agent_id"].startswith("triage-")
    assert plan["system_prompt"] == "You triage tickets."
    assert plan["chat_connection"]["fqn"] == "com.example.Chat"
    assert plan["chat_setup"]["model"] == "qwen2.5:3b"


def test_plan_has_two_python_tools_in_order():
    plan = build_plan(_Triage())
    names = [t["name"] for t in plan["tools"]]
    assert sorted(names) == ["classify", "lookup"]
    for t in plan["tools"]:
        assert t["kind"] == "python"
        assert t["cloudpickle_b64"]
        assert isinstance(t["param_names"], list)


def test_plan_actions_carry_event_lists():
    plan = build_plan(_Triage())
    by_name = {a["name"]: a for a in plan["actions"]}
    assert by_name["handle"]["events"] == ["ticket"]
    assert by_name["handle_alert"]["events"] == ["alert", "incident"]


def test_tool_cloudpickle_round_trips():
    plan = build_plan(_Triage())
    classify = next(t for t in plan["tools"] if t["name"] == "classify")
    fn = decode(classify["cloudpickle_b64"])
    assert fn("please issue a refund") == "refund"
    assert fn("hello world") == "general"


def test_action_cloudpickle_round_trips():
    plan = build_plan(_Triage())
    handle = next(a for a in plan["actions"] if a["name"] == "handle")
    fn = decode(handle["cloudpickle_b64"])
    out = fn({"id": "t-1", "body": "I want a refund"}, {})
    assert out == {"id": "t-1", "label": "refund"}


def test_plan_json_is_valid_json_with_all_keys():
    s = build_plan_json(_Triage())
    obj = json.loads(s)
    for k in ("agent_id", "tools", "actions", "resources", "listeners"):
        assert k in obj


def test_subclass_override_wins():
    class Override(_Triage):
        @tool
        def classify(self, text: str) -> str:
            return "always-billing"

    plan = build_plan(Override())
    classify = next(t for t in plan["tools"] if t["name"] == "classify")
    fn = decode(classify["cloudpickle_b64"])
    assert fn("anything") == "always-billing"


def test_imperative_builder_works_without_subclass():
    a = Agent().with_id("calc-" + uuid.uuid4().hex[:6])
    a.with_system_prompt("be terse")
    a.with_resource("embedder", ResourceRef("com.example.E", {"dim": "384"}))
    plan = build_plan(a)
    assert plan["agent_id"].startswith("calc-")
    assert plan["system_prompt"] == "be terse"
    assert plan["resources"]["embedder"]["fqn"] == "com.example.E"
    assert plan["tools"] == []
    assert plan["actions"] == []


def test_resource_ref_roundtrip():
    r = ResourceRef("a.b.C", {"x": "y"})
    assert r.to_dict() == {"fqn": "a.b.C", "config": {"x": "y"}}
