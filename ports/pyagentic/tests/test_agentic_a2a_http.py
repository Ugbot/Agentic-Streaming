"""A peer agent over HTTP, called as a tool by the core: the A2A front door's `POST /agent`, through the
egress policy and the journal, with its bearer named by reference. Each peer here is a local server."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List, Tuple, Union

import pytest

from agentic import Turn
from agentic.conformance import CrashPoint, SimulatedCrash
from agentic.runtime import LocalRuntime
from agentic.tools import idempotency_key

TOKEN = "peer-token-not-a-real-one-0123456789"
Answer = Union[Dict[str, Any], Tuple[int, str]]


class Peer:
    """Answers each POST from `script`, repeating the last. A dict is a 200 JSON body, a pair a status
    and body; a 3xx pair sends `Location` to the pair's body."""

    def __init__(self) -> None:
        self.script: List[Answer] = [{"reply": "[specialist] on it"}]
        self.requests: List[Dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the stdlib names it
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                outer.requests.append({"path": self.path, "headers": dict(self.headers.items()),
                                       "body": json.loads(raw) if raw else None})
                answer = outer.script[min(len(outer.requests) - 1, len(outer.script) - 1)]
                if isinstance(answer, tuple):
                    status, body = answer
                    self.send_response(status)
                    if 300 <= status < 400:
                        self.send_header("Location", body)
                    data = body.encode()
                else:
                    self.send_response(200)
                    data = json.dumps(answer).encode()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def peer(monkeypatch: Any) -> Iterator[Peer]:
    monkeypatch.setenv("AGENTIC_PEER_TOKEN", TOKEN)
    server = Peer()
    monkeypatch.setenv("AGENTIC_PEER_URL", server.url)
    yield server
    server.close()


def document(**peer: Any) -> Dict[str, Any]:
    entry = {"id": "specialist", "transport": "http", "effect": "idempotent", "url": "${AGENTIC_PEER_URL}",
             "auth": {"bearer": "env:AGENTIC_PEER_TOKEN"}, "description": "the specialist", **peer}
    return {
        "agent": {"id": "triage",
                  "router": {"kind": "keyword", "default": "triage", "rules": {"escalations": ["escalate"]}},
                  "paths": {"triage": {"brain": "rule"},
                            "escalations": {"brain": "rule", "tools": ["specialist"],
                                            "tool_triggers": {"escalate": "specialist"}}}},
        "a2a": [entry],
    }


def deployed(doc: Dict[str, Any]) -> LocalRuntime:
    rt = LocalRuntime()
    rt.deploy(doc)
    return rt


def recorded(rt: LocalRuntime, result: Dict[str, Any]) -> str:
    assert rt.journal is not None
    return json.dumps(result) + repr(list(rt.events("c1"))) + repr(list(rt.journal._entries.values()))  # type: ignore[attr-defined]


def test_a_turn_delegates_to_a_peer_over_http(peer):
    rt = deployed(document())
    result = rt.submit(Turn("c1", "t1", "please escalate this", "alice"))
    assert (result["status"], result["path"]) == ("completed", "escalations")
    assert "[specialist] on it" in result["reply"]
    assert [e["type"] for e in result["events"]].count("delegated") == 1
    (request,) = peer.requests
    assert request["path"] == "/agent"
    assert request["body"] == {"conversation_id": "c1", "text": "please escalate this", "user_id": "alice"}
    headers = {k.lower(): v for k, v in request["headers"].items()}
    assert headers["authorization"] == f"Bearer {TOKEN}"
    assert headers["idempotency-key"] == idempotency_key("c1", "t1", 0)
    assert TOKEN not in recorded(rt, result)


def test_a_turn_that_does_not_delegate_needs_no_peer(monkeypatch):
    monkeypatch.delenv("AGENTIC_PEER_URL", raising=False)
    result = deployed(document()).submit(Turn("c1", "t1", "what time do you open?"))
    assert (result["status"], result["path"]) == ("completed", "triage")


def test_an_unset_url_fails_the_call_and_sends_nothing(monkeypatch):
    monkeypatch.delenv("AGENTIC_PEER_URL", raising=False)
    result = deployed(document()).submit(Turn("c1", "t1", "escalate"))
    assert (result["status"], result["error"]["class"]) == ("failed", "tool")
    assert "AGENTIC_PEER_URL" in result["error"]["message"]


def test_a_link_off_the_operators_list_is_not_expanded(peer, monkeypatch):
    monkeypatch.setenv("PEER_ELSEWHERE", peer.url)
    result = deployed(document(url="${PEER_ELSEWHERE}")).submit(Turn("c1", "t1", "escalate"))
    assert (result["status"], result["error"]["class"]) == ("failed", "tool")
    assert "AGENTIC_SECRET_REFS" in result["error"]["message"] and peer.requests == []


def test_a_peer_cannot_redirect_the_call_or_its_bearer(peer):
    elsewhere = Peer()
    try:
        peer.script = [(302, elsewhere.url + "/agent")]
        result = deployed(document(retries=0)).submit(Turn("c1", "t1", "escalate"))
        assert (result["status"], result["error"]["class"]) == ("failed", "tool")
        assert len(peer.requests) == 1 and elsewhere.requests == []
    finally:
        elsewhere.close()


def test_the_egress_policy_holds_for_a_peer(peer, monkeypatch):
    monkeypatch.setenv("AGENTIC_EGRESS_ALLOW_HOSTS", "peers.example.org")
    result = deployed(document()).submit(Turn("c1", "t1", "escalate"))
    assert (result["status"], result["error"]["class"]) == ("failed", "tool")
    assert "egress" in result["error"]["message"] and peer.requests == []


def test_a_peer_error_is_retried_under_the_same_key(peer):
    peer.script = [(503, "busy"), (502, "busy"), {"reply": "[specialist] done"}]
    result = deployed(document(retries=2)).submit(Turn("c1", "t1", "escalate"))
    assert result["status"] == "completed" and "done" in result["reply"]
    keys = {r["headers"]["Idempotency-Key"] for r in peer.requests}
    assert len(peer.requests) == 3 and keys == {idempotency_key("c1", "t1", 0)}


def test_a_refusal_from_the_peer_is_not_retried(peer):
    peer.script = [(403, '{"error": "forbidden"}')]
    result = deployed(document(retries=2)).submit(Turn("c1", "t1", "escalate"))
    assert (result["status"], result["error"]["class"]) == ("failed", "tool")
    assert len(peer.requests) == 1


def test_a_non_idempotent_peer_with_no_answer_waits_for_a_person(peer):
    peer.script = [(503, "busy")]
    result = deployed(document(effect="non_idempotent", retries=2)).submit(Turn("c1", "t1", "escalate"))
    assert result["status"] == "suspended"
    assert len(peer.requests) == 1, "a non_idempotent peer gets no retries"


def test_a_journaled_answer_is_reused_after_a_crash(peer):
    rt = deployed(document())
    assert rt.engine is not None
    rt.engine.probe = CrashPoint({"event": "reply_drafted", "when": "before"})
    with pytest.raises(SimulatedCrash):
        rt.submit(Turn("c1", "t1", "escalate"))
    rt = rt.restart()
    result = rt.submit(Turn("c1", "t1", "escalate"))
    assert result["status"] == "completed" and len(peer.requests) == 1


def test_a_tool_of_kind_agent_is_a_peer_over_http(peer):
    doc = document()
    entry = doc.pop("a2a")[0]
    doc["tools"] = [{"id": "specialist", "kind": "agent", "effect": "idempotent", "url": entry["url"],
                     "auth": entry["auth"]}]
    result = deployed(doc).submit(Turn("c1", "t1", "please escalate", "alice"))
    assert result["status"] == "completed" and "[specialist] on it" in result["reply"]
    assert peer.requests[0]["path"] == "/agent"
    assert peer.requests[0]["headers"]["Authorization"] == f"Bearer {TOKEN}"
