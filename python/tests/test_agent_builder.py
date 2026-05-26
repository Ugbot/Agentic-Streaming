"""AgentBuilder round-trip."""

from __future__ import annotations

import uuid
from datetime import timedelta


def test_minimal_agent_builds(af):
    from agentic_flink import Agent

    agent = (
        Agent.builder()
        .with_id("a-" + uuid.uuid4().hex[:8])
        .with_system_prompt("be helpful")
        .build()
    )
    assert agent.id.startswith("a-")
    assert agent.system_prompt == "be helpful"


def test_full_builder_propagates_fields(af):
    from agentic_flink import Agent, ChatSetup, langchain4j_ollama

    agent_id = "calc-" + uuid.uuid4().hex[:8]
    agent = (
        Agent.builder()
        .with_id(agent_id)
        .with_name("Calculator")
        .with_description("Adds numbers")
        .with_system_prompt("you add numbers")
        .with_chat_connection(langchain4j_ollama("http://example.invalid:11434"))
        .with_chat_setup(ChatSetup(model="qwen2.5:3b", temperature=0.4, max_response_tokens=512))
        .with_tools("calculator", "weather")
        .with_max_iterations(7)
        .with_timeout(timedelta(seconds=45))
        .build()
    )
    assert agent.id == agent_id
    assert agent.name == "Calculator"
    assert agent.max_iterations == 7
    assert {"calculator", "weather"}.issubset(agent.allowed_tools)
    # Java getters survive the round-trip too.
    assert str(agent.java.getAgentName()) == "Calculator"
    assert str(agent.java.getDescription()) == "Adds numbers"


def test_agent_repr_is_informative(af):
    from agentic_flink import Agent

    a = (
        Agent.builder()
        .with_id("r-" + uuid.uuid4().hex[:6])
        .with_system_prompt("x")
        .with_tools("alpha", "beta")
        .build()
    )
    r = repr(a)
    assert a.id in r
    assert "alpha" in r and "beta" in r
