"""Claude (the Anthropic Messages API) and Gemini (`generateContent`), against a local server speaking
each provider's documented wire format. Nothing here talks to a live model: these tests prove the
request, the answer, the failure rules, and that a key reaches the provider's header and nowhere else."""

from __future__ import annotations

import json
import time
from typing import Any, Iterator, List

import pytest
from test_agentic_llm import ModelServer, agent, deployed, text, tool, types

from agentic import AgentSpec, Turn
from agentic.conformance import CrashPoint, SimulatedCrash
from agentic.errors import ValidationError
from agentic.llm import DEFAULT_CLAUDE_MODEL, PROVIDERS, make_chat_client
from agentic.runtime import LocalRuntime
from agentic.untrusted import RULE

CLAUDE_KEY = "sk-ant-test-not-a-real-key-0123456789"
GEMINI_KEY = "AIzaTest-not-a-real-key-0123456789abcdef"


@pytest.fixture
def claude(monkeypatch: Any) -> Iterator[ModelServer]:
    monkeypatch.setenv("ANTHROPIC_API_KEY", CLAUDE_KEY)
    model = ModelServer("anthropic")
    yield model
    model.close()


@pytest.fixture
def gemini(monkeypatch: Any) -> Iterator[ModelServer]:
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    model = ModelServer("gemini")
    yield model
    model.close()


def on(server: ModelServer, **llm: Any) -> Any:
    provider = "anthropic" if server.shape == "anthropic" else "gemini"
    llm.setdefault("model", "gemini-test" if provider == "gemini" else "claude-test")
    return agent(server, provider=provider, **llm)


def everything_recorded(rt: LocalRuntime, result: Any) -> str:
    entries = list(rt.journal._entries.values())  # type: ignore[attr-defined]
    return json.dumps(result) + repr(list(rt.events("c1"))) + repr(entries)


def test_the_providers_are_listed():
    assert {"anthropic", "gemini"} <= set(PROVIDERS)


# -- Claude -----------------------------------------------------------------------------

def test_claude_calls_a_tool_and_answers_over_the_messages_api(claude):
    claude.script = [tool("pay", order="77"), text("Paid order 77.")]
    rt = deployed(on(claude))
    result = rt.submit(Turn("c1", "t1", "pay 77", "alice"))
    assert (result["status"], result["reply"]) == ("completed", "[main] Paid order 77.")
    assert types(result).count("llm_called") == 2 and result["tool_calls"][0]["tool"] == "pay"
    assert claude.paths == ["/v1/messages"] * 2
    headers = {k.lower(): v for k, v in claude.headers[0].items()}
    assert headers["x-api-key"] == CLAUDE_KEY and headers["anthropic-version"] == "2023-06-01"
    assert "authorization" not in headers

    first = claude.requests[0]
    assert first["model"] == "claude-test" and first["max_tokens"] == 16000
    assert "You pay invoices." in first["system"] and RULE in first["system"]
    assert first["messages"] == [{"role": "user", "content": "pay 77"}]
    second = claude.requests[1]["messages"]
    assert {m["role"] for m in second} == {"user", "assistant"}
    observed = second[-1]["content"]
    assert observed.startswith('Result of pay: <untrusted source="tool:pay">') and observed.endswith("</untrusted>")
    assert CLAUDE_KEY not in everything_recorded(rt, result)


def test_claude_has_a_default_model_and_the_log_names_it(claude):
    claude.script = [text("ok")]
    spec = agent(claude, provider="anthropic")
    document = {**spec.document, "llm": {k: v for k, v in spec.document["llm"].items() if k != "model"}}
    result = deployed(AgentSpec(document, spec.bindings)).submit(Turn("c1", "t1", "hi"))
    assert DEFAULT_CLAUDE_MODEL == "claude-sonnet-5-5"
    assert claude.requests[0]["model"] == DEFAULT_CLAUDE_MODEL
    called = [e["payload"] for e in result["events"] if e["type"] == "llm_called"]
    assert called[0]["model"] == DEFAULT_CLAUDE_MODEL


def test_claude_gets_sampling_settings_only_when_declared(claude):
    claude.script = [text("ok")]
    deployed(on(claude)).submit(Turn("c1", "t1", "hi"))
    assert "temperature" not in claude.requests[0] and "top_p" not in claude.requests[0]
    deployed(on(claude, temperature=0.3, max_tokens=64)).submit(Turn("c2", "t1", "hi"))
    assert (claude.requests[1]["temperature"], claude.requests[1]["max_tokens"]) == (0.3, 64)


def test_a_claude_refusal_is_fatal_and_not_retried(claude):
    refusal = {"type": "message", "role": "assistant", "stop_reason": "refusal", "content": []}
    claude.script = [(200, json.dumps(refusal))]
    result = deployed(on(claude, retry=3)).submit(Turn("c1", "t1", "hi"))
    assert (result["status"], result["error"]["class"]) == ("failed", "fatal")
    assert "refused" in result["error"]["message"] and len(claude.requests) == 1


# -- Gemini -----------------------------------------------------------------------------

def test_gemini_calls_a_tool_and_answers_over_generate_content(gemini):
    gemini.script = [tool("pay", order="77"), text("Paid order 77.")]
    rt = deployed(on(gemini))
    result = rt.submit(Turn("c1", "t1", "pay 77", "alice"))
    assert (result["status"], result["reply"]) == ("completed", "[main] Paid order 77.")
    assert gemini.paths == ["/models/gemini-test:generateContent"] * 2, "the key is never in the url"
    headers = {k.lower(): v for k, v in gemini.headers[0].items()}
    assert headers["x-goog-api-key"] == GEMINI_KEY

    first = gemini.requests[0]
    system = first["systemInstruction"]["parts"][0]["text"]
    assert "You pay invoices." in system and RULE in system
    assert first["contents"] == [{"role": "user", "parts": [{"text": "pay 77"}]}]
    assert first["generationConfig"] == {"responseMimeType": "application/json"}
    second = gemini.requests[1]["contents"]
    assert [c["role"] for c in second] == ["user", "model", "user"]
    assert second[-1]["parts"][0]["text"].startswith('Result of pay: <untrusted source="tool:pay">')
    assert GEMINI_KEY not in everything_recorded(rt, result)


def test_gemini_gets_sampling_settings_only_when_declared(gemini):
    gemini.script = [text("ok")]
    deployed(on(gemini, temperature=0.1, max_tokens=8)).submit(Turn("c1", "t1", "hi"))
    assert gemini.requests[0]["generationConfig"] == {"responseMimeType": "application/json",
                                                      "temperature": 0.1, "maxOutputTokens": 8}


@pytest.mark.parametrize("answer", [{"promptFeedback": {"blockReason": "SAFETY"}},
                                    {"candidates": [{"finishReason": "SAFETY", "content": {"parts": []}}]}])
def test_a_gemini_block_is_fatal_and_not_retried(gemini, answer):
    gemini.script = [(200, json.dumps(answer))]
    result = deployed(on(gemini, retry=3)).submit(Turn("c1", "t1", "hi"))
    assert (result["status"], result["error"]["class"]) == ("failed", "fatal")
    assert len(gemini.requests) == 1


def test_gemini_needs_a_model(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    with pytest.raises(ValidationError, match="llm.model is required"):
        make_chat_client({"provider": "gemini"})


# -- both -------------------------------------------------------------------------------

@pytest.mark.parametrize("provider,variable", [("anthropic", "ANTHROPIC_API_KEY"), ("gemini", "GEMINI_API_KEY")])
def test_a_missing_key_fails_the_build(provider, variable, monkeypatch):
    monkeypatch.delenv(variable, raising=False)
    with pytest.raises(ValidationError, match=variable):
        make_chat_client({"provider": provider, "model": "m"})


@pytest.mark.parametrize("shape", ["anthropic", "gemini"])
def test_an_error_body_that_echoes_the_key_is_redacted(shape, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", CLAUDE_KEY)
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    server = ModelServer(shape)
    try:
        key = CLAUDE_KEY if shape == "anthropic" else GEMINI_KEY
        server.script = [(401, json.dumps({"error": f"invalid key {key}"}))]
        rt = deployed(on(server))
        result = rt.submit(Turn("c1", "t1", "hi"))
        assert (result["status"], result["error"]["class"]) == ("failed", "fatal")
        assert "401" in result["error"]["message"]
        assert key not in everything_recorded(rt, result)
    finally:
        server.close()


@pytest.mark.parametrize("shape", ["anthropic", "gemini"])
def test_transient_statuses_are_retried_without_repeating_tools(shape, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", CLAUDE_KEY)
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    server = ModelServer(shape)
    calls: List[str] = []
    try:
        server.script = [tool("pay", order="1"), (529, "overloaded"), text("Done.")]
        spec = on(server, fn=lambda order: calls.append(order) or "ok", retry=3)
        result = deployed(spec).submit(Turn("c1", "t1", "pay"))
        assert (result["status"], result["reply"], len(calls)) == ("completed", "[main] Done.", 1)
    finally:
        server.close()


@pytest.mark.parametrize("shape", ["anthropic", "gemini"])
def test_timeout_ms_bounds_the_whole_call(shape, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", CLAUDE_KEY)
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    server = ModelServer(shape)
    try:
        server.script = [("drip", 3.0)]
        started = time.monotonic()
        result = deployed(on(server, timeout_ms=200)).submit(Turn("c1", "t1", "hi"))
        assert (result["status"], result["error"]["class"]) == ("failed", "transient")
        assert time.monotonic() - started < 2.0
    finally:
        server.close()


@pytest.mark.parametrize("shape", ["anthropic", "gemini"])
def test_the_egress_policy_holds_for_a_model_call(shape, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", CLAUDE_KEY)
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    monkeypatch.setenv("AGENTIC_EGRESS_ALLOW_HOSTS", "models.example.org")
    server = ModelServer(shape)
    try:
        server.script = [text("ok")]
        result = deployed(on(server)).submit(Turn("c1", "t1", "hi"))
        assert (result["status"], result["error"]["class"]) == ("failed", "fatal")
        assert "egress" in result["error"]["message"] and server.requests == []
    finally:
        server.close()


@pytest.mark.parametrize("shape", ["anthropic", "gemini"])
def test_a_journaled_answer_is_reused_after_a_crash(shape, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", CLAUDE_KEY)
    monkeypatch.setenv("GEMINI_API_KEY", GEMINI_KEY)
    server = ModelServer(shape)
    calls: List[str] = []
    try:
        server.script = [tool("pay", order="77"), text("Paid.")]
        rt = deployed(on(server, fn=lambda order: calls.append(order) or "ok"))
        assert rt.engine is not None
        rt.engine.probe = CrashPoint({"event": "reply_drafted", "when": "before"})
        with pytest.raises(SimulatedCrash):
            rt.submit(Turn("c1", "t1", "pay"))
        rt = rt.restart()
        result = rt.submit(Turn("c1", "t1", "pay"))
        assert (result["status"], result["reply"]) == ("completed", "[main] Paid.")
        assert (len(server.requests), len(calls)) == (2, 1)
    finally:
        server.close()
