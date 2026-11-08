"""A skill is tools and a prompt fragment a path takes on by naming it (`primitives.md`, the tool boundary
for skills). The tools are granted by the policy; the fragment goes into the model's instructions."""

from __future__ import annotations

from typing import Any

import pytest
from test_agentic_llm import ModelServer, text, tool

from agentic import Turn
from agentic.runtime import LocalRuntime


@pytest.fixture
def model(monkeypatch: Any) -> Any:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-a-real-key")
    server = ModelServer()
    yield server
    server.close()


def document(model: ModelServer) -> Any:
    return {
        "agent": {"id": "a", "router": {"kind": "static", "default": "cards"},
                  "paths": {"cards": {"brain": "llm", "prompt": "You answer card questions.", "skills": ["card_help"]}},
                  "verifier": {"kind": "none"}},
        "llm": {"provider": "openai", "model": "m", "base_url": model.url},
        "skills": [{"name": "card_help", "prompt": "Prefer the knowledge base over guessing.",
                    "tools": ["get_balance"]}],
        "tools": [{"id": "get_balance", "kind": "constant", "effect": "read", "value": 12,
                   "description": "Look up the balance"}],
    }


def test_a_skill_adds_its_prompt_and_its_tools(model):
    model.script = [tool("get_balance"), text("12")]
    rt = LocalRuntime()
    rt.deploy(document(model))
    result = rt.submit(Turn("c1", "t1", "what is my card balance?"))
    assert (result["status"], result["reply"]) == ("completed", "[cards] 12")
    system = model.requests[0]["messages"][0]["content"]
    assert system.startswith("You answer card questions.\nPrefer the knowledge base over guessing.")
    assert "get_balance: Look up the balance" in system
