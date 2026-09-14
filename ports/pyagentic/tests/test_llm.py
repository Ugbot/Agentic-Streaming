"""LLM brain feature tests (offline, deterministic via StubChatClient)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.core import Agent, AgentContext, Event, RoutedGraph  # noqa: E402
from pyagentic.llm import ChatResult, LlmBrain, StubChatClient  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.tools import ToolRegistry  # noqa: E402


def _ctx(tools):
    return AgentContext("c1", "alice", InMemoryConversationStore(), InMemoryKeyedStateStore(), tools, None)


def test_llm_brain_runs_react_tool_then_final():
    tools = ToolRegistry().register("get_balance", "Look up balance", lambda p: 1234.56)
    script = [
        ChatResult(tool="get_balance", args={"user": "alice"}),  # step 1: call the tool
        ChatResult(text="Your balance is 1234.56."),             # step 2: final answer
    ]
    brain = LlmBrain(StubChatClient(script), name="payments", tools=["get_balance"])
    agent = Agent("payments", "You answer payment questions.", brain)
    ctx = _ctx(tools)
    res = agent.turn(Event("c1", "what is my balance?", "alice"), ctx)
    assert "get_balance" in res.tool_calls          # the ReAct loop executed the tool
    assert res.reply == "[payments] Your balance is 1234.56."


def test_llm_brain_direct_final_no_tool():
    tools = ToolRegistry()
    brain = LlmBrain(StubChatClient([ChatResult(text="Hello!")]), name="general")
    res = Agent("general", "p", brain).turn(Event("c1", "hi", "u"), _ctx(tools))
    assert res.reply == "[general] Hello!"
    assert res.tool_calls == []


def test_llm_brain_in_routed_graph():
    tools = ToolRegistry().register("get_balance", "Look up balance", lambda p: 1234.56)
    pay = Agent("payments", "p", LlmBrain(
        StubChatClient([ChatResult(tool="get_balance", args={}), ChatResult(text="It is 1234.56.")]),
        name="payments"))
    general = Agent("general", "p", LlmBrain(StubChatClient([ChatResult(text="hi")]), name="general"))
    graph = RoutedGraph(
        router=lambda ev, ctx: "payments" if "balance" in ev.text.lower() else "general",
        paths={"payments": pay, "general": general},
        verifier=lambda reply, ctx: (reply.startswith("["), reply),
    )
    ctx = _ctx(tools)
    res = graph.handle(Event("c1", "what is my balance?", "alice"), ctx)
    assert res.path == "payments" and res.ok
    assert "get_balance" in res.tool_calls and "1234.56" in res.reply


def test_stub_repeats_last_when_exhausted():
    c = StubChatClient([ChatResult(text="only")])
    assert c.chat([], []).text == "only"
    assert c.chat([], []).text == "only"  # repeats last


def test_real_chat_clients_require_an_explicit_model(monkeypatch):
    import random

    import pytest

    from pyagentic.llm import LiteLLMChatClient, OllamaChatClient, OpenAIChatClient, require_model

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    for client in (OllamaChatClient, OpenAIChatClient, LiteLLMChatClient):
        with pytest.raises(TypeError):
            client()  # type: ignore[call-arg]  # model is positional and required
        for empty in ("", "   ", None):
            with pytest.raises(ValueError, match="explicit model name"):
                client(model=empty)  # type: ignore[arg-type]
    name = "model-" + "".join(random.choice("abcdef0123456789") for _ in range(8))
    assert OllamaChatClient(model=f" {name} ").model == name
    assert OpenAIChatClient(model=name).model == name
    assert require_model(name, "X") == name
    with pytest.raises(ValueError, match="X needs an explicit model name"):
        require_model(None, "X")
