"""OPA can narrow what a document allows and never widen it, and anything but a clear yes is a no."""

import http.server
import json
import threading
from typing import Any, Dict, Iterator, List

import pytest

from agentic.events import Turn
from agentic.opa import OpaToolPolicy, from_environment
from agentic.policy import ALLOW, DENY, ToolRequest
from agentic.runtime import LocalRuntime
from agentic.spec import Agent


class Stub:
    def __init__(self) -> None:
        self.answer: Any = {"result": True}
        self.status = 200
        self.seen: List[Dict[str, Any]] = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                outer.seen.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                body = outer.answer if isinstance(outer.answer, bytes) else json.dumps(outer.answer).encode()
                self.send_response(outer.status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: Any) -> None:
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture
def opa() -> Iterator[Stub]:
    stub = Stub()
    yield stub
    stub.server.shutdown()


REQUEST = ToolRequest("pay", "brain", "main", "non_idempotent", {"amount": 5}, {"subject": "alice", "tenant": "acme"})


def test_a_clear_yes_is_allowed_and_sees_who_is_asking(opa: Stub) -> None:
    assert OpaToolPolicy(opa.url).decide(REQUEST).verdict == ALLOW
    sent = opa.seen[0]["input"]
    assert sent["tool"] == "pay" and sent["principal"]["subject"] == "alice" and sent["args"] == {"amount": 5}
    opa.answer = {"result": {"allow": True}}
    assert OpaToolPolicy(opa.url).decide(REQUEST).verdict == ALLOW


@pytest.mark.parametrize("answer", [
    {"result": False}, {"result": {"allow": False}}, {}, {"result": None}, {"result": {"allow": "yes"}},
    {"result": "true"}, {"result": 1}, {"result": []}, b"not json", b"[]",
])
def test_anything_else_is_a_no(opa: Stub, answer: Any) -> None:
    opa.answer = answer
    assert OpaToolPolicy(opa.url).decide(REQUEST).verdict == DENY


def test_a_reason_is_passed_on(opa: Stub) -> None:
    opa.answer = {"result": {"allow": False, "reason": "not after hours"}}
    assert OpaToolPolicy(opa.url).decide(REQUEST).reason == "not after hours"


def test_an_error_status_is_a_no(opa: Stub) -> None:
    opa.status = 500
    assert OpaToolPolicy(opa.url).decide(REQUEST).verdict == DENY


def test_an_opa_that_is_down_is_a_no() -> None:
    assert OpaToolPolicy("http://127.0.0.1:9", timeout_ms=300).decide(REQUEST).verdict == DENY


def test_no_url_means_no_opa() -> None:
    assert from_environment({}) is None
    assert from_environment({"AGENTIC_OPA_URL": "http://o"}).url == "http://o/v1/data/agentic/tools"


def runtime(monkeypatch: pytest.MonkeyPatch, url: str, tools: List[str]) -> LocalRuntime:
    monkeypatch.setenv("AGENTIC_OPA_URL", url)
    spec = (Agent("opa-test").path("main", tools=tools, tool_triggers={"pay": "pay"})
            .tool("pay", "constant", value="ok", effect="idempotent").verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    return rt


def test_opa_narrows_the_document(opa: Stub, monkeypatch: pytest.MonkeyPatch) -> None:
    opa.answer = {"result": {"allow": False, "reason": "not today"}}
    result = runtime(monkeypatch, opa.url, ["pay"]).submit(Turn("c", "t", "pay"))
    assert (result["status"], result["error"]["class"], result["tool_calls"]) == ("failed", "policy", [])
    refused = next(e for e in result["events"] if e["type"] == "tool_refused")
    assert refused["payload"]["reason"] == "not today"


def test_opa_cannot_widen_the_document(opa: Stub, monkeypatch: pytest.MonkeyPatch) -> None:
    result = runtime(monkeypatch, opa.url, []).submit(Turn("c", "t", "pay"))
    assert (result["status"], result["error"]["class"]) == ("failed", "policy")
    assert opa.seen == [], "a refusal by the document never leaves the process"
