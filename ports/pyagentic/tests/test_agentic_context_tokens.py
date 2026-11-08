"""`context.max_tokens` bounds what the model sees of the transcript, at about four characters a token;
`compaction: moscow` keeps what the user said before the agent's replies. The log, and the state folded
from it, are never compacted."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from test_agentic_llm import ModelServer, text

from agentic import Turn
from agentic.events import ChatMessage, fit_tokens, tokens_of
from agentic.runtime import LocalRuntime


def messages(*pairs: str) -> List[ChatMessage]:
    return [ChatMessage("user" if i % 2 == 0 else "assistant", t) for i, t in enumerate(pairs)]


def test_a_token_is_about_four_characters():
    assert [tokens_of(t) for t in ("", "a", "abcd", "abcde")] == [1, 1, 1, 2]


def test_without_a_budget_nothing_is_dropped():
    history = messages("u1", "a1", "u2", "a2")
    assert fit_tokens(history, {}) == history
    assert fit_tokens(history, {"compaction": "moscow"}) == history


def test_the_newest_messages_that_fit_are_kept_in_order():
    history = messages("x" * 40, "y" * 40, "z" * 8, "w" * 8)
    assert [m.text for m in fit_tokens(history, {"max_tokens": 5})] == ["z" * 8, "w" * 8]
    assert [m.text for m in fit_tokens(history, {"max_tokens": 14})] == ["y" * 40, "z" * 8, "w" * 8]


def test_moscow_keeps_what_the_user_said_before_the_replies():
    history = messages("u" * 8, "a" * 40, "v" * 8, "b" * 8)
    kept = fit_tokens(history, {"max_tokens": 6, "compaction": "moscow"})
    assert [m.text for m in kept] == ["u" * 8, "v" * 8, "b" * 8]


@pytest.fixture
def model(monkeypatch: Any) -> Any:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-a-real-key")
    server = ModelServer()
    yield server
    server.close()


def test_the_model_sees_the_bounded_transcript_and_the_state_counts_everything(model):
    doc: Dict[str, Any] = {
        "agent": {"id": "a", "router": {"kind": "static", "default": "main"},
                  "paths": {"main": {"brain": "llm", "prompt": "p"}}, "verifier": {"kind": "none"}},
        "llm": {"provider": "openai", "model": "m", "base_url": model.url},
        "context": {"max_tokens": 8, "compaction": "moscow"},
    }
    model.script = [text("first answer, quite a long one")]
    rt = LocalRuntime()
    rt.deploy(doc)
    rt.submit(Turn("c1", "t1", "one"))
    rt.submit(Turn("c1", "t2", "two"))
    rt.submit(Turn("c1", "t3", "three"))
    seen = [(m["role"], m["content"]) for m in model.requests[2]["messages"][1:]]
    assert seen == [("user", "one"), ("user", "two"), ("user", "three")]
    assert rt.state("c1")["transcript_length"] == 6
