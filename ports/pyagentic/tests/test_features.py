"""Phase B feature tests: skills, structured output, richer listeners."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.core import Agent, AgentContext, Event, RoutedGraph  # noqa: E402
from pyagentic.listeners import CompositeListener, MetricsListener  # noqa: E402
from pyagentic.llm import ChatResult, LlmBrain, StubChatClient  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.skills import Skill, SkillRegistry  # noqa: E402
from pyagentic.structured import parse_structured, validate  # noqa: E402
from pyagentic.tools import ToolRegistry  # noqa: E402


def _ctx(tools, listeners=None):
    return AgentContext("c1", "alice", InMemoryConversationStore(), InMemoryKeyedStateStore(),
                        tools, None, listeners=listeners or [])


# ---- skills ----

def test_skill_registry_expands_tools_and_prompt():
    reg = SkillRegistry().register(Skill("billing", tools=("get_balance", "refund"),
                                         prompt_fragment="Be precise about amounts.",
                                         required_facts=("account",)))
    tools, fragment, facts = reg.expand(["billing"])
    assert tools == ["get_balance", "refund"]
    assert "precise" in fragment and facts == ["account"]


def test_skill_registry_from_specs_and_unknown():
    reg = SkillRegistry.from_specs([{"name": "kb", "tools": ["search"], "prompt": "Cite sources."}])
    assert reg.get("kb").tools == ("search",)
    try:
        reg.get("missing")
        assert False, "expected KeyError"
    except KeyError:
        pass


# ---- structured output ----

def test_validate_required_and_types():
    schema = {"type": "object", "required": ["category", "amount"],
              "properties": {"category": {"type": "string"}, "amount": {"type": "number"}}}
    assert validate({"category": "refund", "amount": 42.0}, schema) == []
    errs = validate({"category": "refund"}, schema)
    assert any("amount" in e for e in errs)
    errs2 = validate({"category": 1, "amount": "x"}, schema)
    assert len(errs2) == 2


def test_parse_structured_tolerates_prose():
    schema = {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}}
    obj, errs = parse_structured('here you go: {"ok": true} done', schema)
    assert obj == {"ok": True} and errs == []


def test_llm_brain_output_schema_returns_validated_json():
    schema = {"type": "object", "required": ["category"], "properties": {"category": {"type": "string"}}}
    brain = LlmBrain(StubChatClient([ChatResult(text='{"category": "refund"}')]),
                     name="triage", output_schema=schema)
    res = Agent("triage", "p", brain).turn(Event("c1", "I want a refund", "u"), _ctx(ToolRegistry()))
    assert res.reply == '[triage] {"category": "refund"}'


# ---- richer listeners ----

def test_tool_call_and_composite_listener_hooks_fire():
    m1, m2 = MetricsListener(), MetricsListener()
    composite = CompositeListener(m1, m2)
    tools = ToolRegistry().register("get_balance", "balance", lambda p: 1234.56)
    pay = Agent("payments", "p", LlmBrain(
        StubChatClient([ChatResult(tool="get_balance", args={}), ChatResult(text="done")]), name="payments"))
    graph = RoutedGraph(router=lambda ev, ctx: "payments", paths={"payments": pay},
                        verifier=None, listeners=[composite])
    graph.handle(Event("c1", "balance?", "u"), _ctx(tools))
    for m in (m1, m2):  # composite fans out to both
        assert m.turns == 1 and m.tool_calls == 1 and m.paths["payments"] == 1


def test_listener_error_hook_on_tool_failure():
    m = MetricsListener()
    def boom(_p):
        raise RuntimeError("tool exploded")
    tools = ToolRegistry().register("boom", "fails", boom)
    pay = Agent("payments", "p", LlmBrain(
        StubChatClient([ChatResult(tool="boom", args={}), ChatResult(text="x")]), name="payments"))
    graph = RoutedGraph(router=lambda ev, ctx: "payments", paths={"payments": pay},
                        verifier=None, listeners=[m])
    try:
        graph.handle(Event("c1", "go", "u"), _ctx(tools))
    except RuntimeError:
        pass
    assert m.errors == 1
