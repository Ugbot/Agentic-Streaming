"""The real model client, against a real local HTTP server speaking the provider's format. Nothing
here is a mock of the client, and nothing here talks to a live model: these tests prove the wire
format, the loop and the failure rules, not what any model would actually say."""

from __future__ import annotations

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List, Tuple, Union

import pytest

from agentic import Agent, Turn
from agentic.conformance import CrashPoint, SimulatedCrash
from agentic.errors import TransientError, ValidationError
from agentic.llm import is_openai_reasoning, make_chat_client, parse_chat_json
from agentic.runtime import LocalRuntime

Answer = Union[str, Tuple[int, str], Tuple[str, float]]


class ModelServer:
    """Answers each POST from `script`, repeating the last. A string is the model's content, a
    `(status, body)` pair an HTTP failure, and `("drip", seconds)` a body that never quite ends."""

    def __init__(self, shape: str = "openai") -> None:
        self.script: List[Answer] = []
        self.requests: List[Dict[str, Any]] = []
        self.headers: List[Dict[str, str]] = []
        self.paths: List[str] = []
        self.shape = shape
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the stdlib names it
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                outer.requests.append(json.loads(body))
                outer.headers.append(dict(self.headers.items()))
                outer.paths.append(self.path)
                answer = outer.script[min(len(outer.requests) - 1, len(outer.script) - 1)]
                if isinstance(answer, tuple) and answer[0] == "drip":
                    self.send_response(200)
                    self.send_header("Content-Length", "100000")
                    self.end_headers()
                    deadline = time.time() + float(answer[1])
                    try:
                        while time.time() < deadline:
                            self.wfile.write(b" ")
                            self.wfile.flush()
                            time.sleep(0.02)
                    except OSError:
                        pass
                    return
                if isinstance(answer, tuple):
                    self._send(int(answer[0]), str(answer[1]).encode())
                    return
                self._send(200, json.dumps(outer._wrap(answer)).encode())

            def _send(self, code: int, raw: bytes) -> None:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def _wrap(self, content: str) -> Dict[str, Any]:
        if self.shape == "ollama":
            return {"message": {"role": "assistant", "content": content}}
        return {"choices": [{"message": {"role": "assistant", "content": content}}]}

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def server(monkeypatch: Any) -> Iterator[ModelServer]:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-a-real-key")
    model = ModelServer()
    yield model
    model.close()


def rid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def tool(name: str, **args: Any) -> str:
    return json.dumps({"tool": name, "args": args})


def text(reply: str) -> str:
    return json.dumps({"text": reply})


def agent(server: ModelServer, fn: Any = None, retry: int = 1, path: Any = None, **llm: Any) -> Any:
    block = {"model": "local-test", "base_url": server.url, **llm}
    builder = (Agent(rid("a")).path("main", brain="llm", prompt="You pay invoices.", tools=["pay"], **(path or {}))
               .use_tool("pay", fn or (lambda order: f"paid {order}"), description="Pays an invoice")
               .with_llm(block.pop("provider", "openai"), **block).verifier("none"))
    if retry > 1:
        builder = builder.policies(retry={"kind": "fixed", "max_attempts": retry, "initial_delay_ms": 0})
    return builder.build()


def deployed(spec: Any) -> LocalRuntime:
    rt = LocalRuntime()
    rt.deploy(spec)
    return rt


def types(result: Dict[str, Any]) -> List[str]:
    return [e["type"] for e in result["events"]]


# -- the loop ---------------------------------------------------------------------------

def test_a_turn_calls_a_tool_and_answers_over_real_http(server):
    server.script = [tool("pay", order="77"), text("Paid order 77.")]
    result = deployed(agent(server)).submit(Turn("c1", "t1", "pay 77", "alice"))
    assert (result["status"], result["reply"], result["path"]) == ("completed", "[main] Paid order 77.", "main")
    assert types(result) == ["turn_received", "routed", "brain_started", "llm_called", "tool_intent", "tool_called",
                             "llm_called", "reply_drafted", "memory_written", "turn_completed"]
    called = [e["payload"] for e in result["events"] if e["type"] == "llm_called"]
    assert called[0] == {"index": 0, "model": "local-test", "tool": "pay", "args": {"order": "77"}}
    assert called[1] == {"index": 1, "model": "local-test", "text": "Paid order 77."}
    assert server.paths == ["/chat/completions"] * 2

    first = server.requests[0]["messages"]
    assert first[0]["role"] == "system"
    assert "You pay invoices." in first[0]["content"] and "pay: Pays an invoice" in first[0]["content"]
    assert first[-1] == {"role": "user", "content": "pay 77"}
    second = server.requests[1]["messages"]
    assert json.loads(second[-2]["content"]) == {"tool": "pay", "args": {"order": "77"}}
    assert second[-1]["role"] == "user" and "paid 77" in second[-1]["content"]
    assert {m["role"] for m in second} <= {"system", "user", "assistant"}  # `tool` needs a tool_calls id


def test_the_model_sees_the_transcript_of_earlier_turns(server):
    server.script = [text("One."), text("Two.")]
    rt = deployed(agent(server))
    rt.submit(Turn("c1", "t1", "first"))
    rt.submit(Turn("c1", "t2", "second"))
    roles = [(m["role"], m["content"]) for m in server.requests[1]["messages"][1:]]
    assert roles == [("user", "first"), ("assistant", "[main] One."), ("user", "second")]


def test_a_tool_outside_the_path_is_refused_in_the_loop(server):
    server.script = [tool("transfer", to="x"), text("Sorry.")]
    result = deployed(agent(server)).submit(Turn("c1", "t1", "move it"))
    assert (result["status"], result["reply"], result["tool_calls"]) == ("completed", "[main] Sorry.", [])
    assert "not permitted" in server.requests[1]["messages"][-1]["content"]


def test_the_loop_is_bounded(server):
    calls: List[str] = []
    server.script = [tool("pay", order="1")]
    spec = agent(server, lambda order: calls.append(order) or "ok", path={"max_iterations": 3})
    result = deployed(spec).submit(Turn("c1", "t1", "pay"))
    assert (result["status"], result["reply"]) == ("completed", "[main] (stopped after 3 steps)")
    assert len(calls) == 3 and len(server.requests) == 3


def test_a_reply_that_is_not_json_is_the_answer():
    assert parse_chat_json("Just words.") == {"text": "Just words."}
    assert parse_chat_json('```json\n{"text": "fenced"}\n```') == {"text": "fenced"}
    assert parse_chat_json('sure: {"tool": "pay", "args": {"a": 1}}') == {"tool": "pay", "args": {"a": 1}}
    assert parse_chat_json('{"tool": "pay"}') == {"tool": "pay", "args": {}}
    assert parse_chat_json('{"unrelated": 1}') == {"text": '{"unrelated": 1}'}


# -- failure rules ----------------------------------------------------------------------

@pytest.mark.parametrize("status", [408, 425, 429, 500, 503])
def test_a_transient_status_is_retried_without_repeating_tools(server, status):
    calls: List[str] = []
    server.script = [tool("pay", order="1"), (status, "busy"), text("Done.")]
    result = deployed(agent(server, lambda order: calls.append(order) or "ok", retry=3)).submit(Turn("c1", "t1", "pay"))
    assert (result["status"], result["reply"], len(calls)) == ("completed", "[main] Done.", 1)
    assert len(server.requests) == 3
    assert types(result).count("llm_called") == 2


def test_transient_past_the_budget_leaves_the_turn_id_unspent(server):
    server.script = [(503, "busy"), (503, "busy"), text("Back.")]
    rt = deployed(agent(server, retry=2))
    first = rt.submit(Turn("c1", "t1", "hi"))
    assert (first["status"], first["error"]["class"]) == ("failed", "transient")
    again = rt.submit(Turn("c1", "t1", "hi"))
    assert (again["status"], again["reply"]) == ("completed", "[main] Back.")


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_a_refusal_is_fatal_and_not_retried(server, status):
    server.script = [(status, '{"error": "no"}')]
    rt = deployed(agent(server, retry=3))
    result = rt.submit(Turn("c1", "t1", "hi"))
    assert (result["status"], result["error"]["class"]) == ("failed", "fatal")
    assert len(server.requests) == 1
    assert rt.submit(Turn("c1", "t1", "hi"))["status"] == "duplicate"
    assert len(server.requests) == 1


def test_a_server_that_is_gone_is_transient(server):
    client = make_chat_client({"provider": "openai", "model": "m", "base_url": "http://127.0.0.1:9/v1",
                               "timeout_ms": 500})
    with pytest.raises(TransientError):
        client.chat([{"role": "user", "content": "hi"}], [])


def test_a_garbled_answer_is_transient(server):
    server.script = [(200, "<html>a proxy</html>"), (200, '{"choices": []}'), text("Fine.")]
    result = deployed(agent(server, retry=3)).submit(Turn("c1", "t1", "hi"))
    assert (result["status"], result["reply"]) == ("completed", "[main] Fine.")


def test_timeout_ms_bounds_the_whole_call_even_if_bytes_keep_arriving(server):
    server.script = [("drip", 3.0)]
    started = time.monotonic()
    result = deployed(agent(server, timeout_ms=200)).submit(Turn("c1", "t1", "hi"))
    assert (result["status"], result["error"]["class"]) == ("failed", "transient")
    assert time.monotonic() - started < 2.0


# -- the journal ------------------------------------------------------------------------

def test_a_recorded_answer_is_reused_after_a_crash_and_the_model_is_not_asked_again(server):
    calls: List[str] = []
    server.script = [tool("pay", order="77"), text("Paid.")]
    rt = deployed(agent(server, lambda order: calls.append(order) or "ok"))
    assert rt.engine is not None
    rt.engine.probe = CrashPoint({"event": "reply_drafted", "when": "before"})
    with pytest.raises(SimulatedCrash):
        rt.submit(Turn("c1", "t1", "pay"))
    assert len(server.requests) == 2
    rt = rt.restart()
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert (result["status"], result["reply"]) == ("completed", "[main] Paid.")
    assert (len(server.requests), len(calls)) == (2, 1)


def test_an_answer_journaled_before_a_crash_stands_though_the_log_never_saw_it(server):
    server.script = [text("First."), text("Second.")]
    rt = deployed(agent(server))
    assert rt.engine is not None
    rt.engine.probe = CrashPoint({"event": "llm_called", "when": "before"})
    with pytest.raises(SimulatedCrash):
        rt.submit(Turn("c1", "t1", "hi"))
    rt = rt.restart()
    result = rt.submit(Turn("c1", "t1", "hi"))
    assert result["reply"] == "[main] First."
    assert len(server.requests) == 1


def test_a_crash_during_a_model_call_asks_again_at_the_same_index(server):
    server.script = [text("Lost."), text("Second.")]
    rt = deployed(agent(server))
    assert rt.engine is not None and rt.engine.chat_client is not None
    client = rt.engine.chat_client

    class DiesAfterAnswering:
        model = client.model

        def chat(self, messages: Any, tools: Any) -> Any:
            client.chat(messages, tools)
            raise SimulatedCrash()

    rt.engine.chat_client = DiesAfterAnswering()
    with pytest.raises(SimulatedCrash):
        rt.submit(Turn("c1", "t1", "hi"))
    rt = rt.restart()
    result = rt.submit(Turn("c1", "t1", "hi"))
    assert result["reply"] == "[main] Second."
    assert len(server.requests) == 2
    assert [e["payload"]["index"] for e in result["events"] if e["type"] == "llm_called"] == [0]


# -- the request ------------------------------------------------------------------------

def test_sampling_settings_are_sent_only_when_declared(server):
    server.script = [text("ok")]
    deployed(agent(server)).submit(Turn("c1", "t1", "hi"))
    assert "temperature" not in server.requests[0] and "max_tokens" not in server.requests[0]
    assert server.requests[0]["model"] == "local-test"
    assert server.requests[0]["response_format"] == {"type": "json_object"}
    deployed(agent(server, temperature=0.2, max_tokens=64)).submit(Turn("c2", "t1", "hi"))
    assert (server.requests[1]["temperature"], server.requests[1]["max_tokens"]) == (0.2, 64)


@pytest.mark.parametrize("model,reasoning", [("o3-mini", True), ("gpt-5-mini", True), ("gpt-12", True),
                                             ("gpt-4o", False), ("llama3", False)])
def test_reasoning_models_get_the_request_they_accept(server, model, reasoning):
    assert is_openai_reasoning(model) is reasoning
    server.script = [text("ok")]
    deployed(agent(server, model=model, temperature=0.2, max_tokens=64)).submit(Turn("c1", "t1", "hi"))
    body = server.requests[0]
    assert ("temperature" in body) is not reasoning
    assert ("max_completion_tokens" if reasoning else "max_tokens") in body


def test_the_key_goes_in_the_header_and_never_on_the_log(server):
    server.script = [text("ok")]
    rt = deployed(agent(server))
    result = rt.submit(Turn("c1", "t1", "hi"))
    assert server.headers[0]["Authorization"] == "Bearer sk-test-not-a-real-key"
    assert "sk-test-not-a-real-key" not in json.dumps(result)
    assert "sk-test-not-a-real-key" not in repr(list(rt.events("c1")))


def test_ollama_speaks_its_own_format():
    model = ModelServer("ollama")
    try:
        model.script = [text("ok")]
        spec = agent(model, provider="ollama", temperature=0.1, max_tokens=8)
        result = deployed(spec).submit(Turn("c1", "t1", "hi"))
        assert result["reply"] == "[main] ok"
        assert model.paths == ["/api/chat"]
        body = model.requests[0]
        assert (body["stream"], body["format"]) == (False, "json")
        assert body["options"] == {"temperature": 0.1, "num_predict": 8}
        assert "Authorization" not in model.headers[0]
    finally:
        model.close()


# -- refused at build -------------------------------------------------------------------

@pytest.mark.parametrize("provider", ["ollama", "openai"])
@pytest.mark.parametrize("model", [None, "  "])
def test_a_model_name_is_required(provider, model, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    block = {"provider": provider} if model is None else {"provider": provider, "model": model}
    with pytest.raises(ValidationError, match="llm.model is required"):
        make_chat_client(block)


def test_an_unknown_provider_lists_the_supported_ones():
    with pytest.raises(ValidationError, match="ollama.*openai.*stub"):
        make_chat_client({"provider": "mystery", "model": "m"})


def test_a_missing_key_fails_the_build(monkeypatch, server):
    monkeypatch.delenv("OPENAI_API_KEY")
    with pytest.raises(ValidationError, match="OPENAI_API_KEY"):
        LocalRuntime().deploy(agent(server))


def test_a_workflow_must_not_carry_a_credential(server):
    block = {"provider": "openai", "model": "m", "x-api-key": "sk-very-secret"}
    with pytest.raises(ValidationError) as raised:
        make_chat_client(block)
    assert "x-api-key" in str(raised.value) and "sk-very-secret" not in str(raised.value)


def test_a_path_not_on_the_llm_brain_needs_no_client(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    spec = Agent(rid("a")).path("main").with_llm("openai", model="m").verifier("none").build()
    assert deployed(spec).submit(Turn("c1", "t1", "hi"))["status"] == "completed"
