"""Phase D (part 1): saga/compensation, context-window MoSCoW compaction, A2A client."""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.a2a import A2AClient, peer_tool  # noqa: E402
from pyagentic.context import ContextItem, ContextWindowManager, Priority  # noqa: E402
from pyagentic.saga import Saga  # noqa: E402


# ---- saga ----

def test_saga_rolls_back_in_reverse_on_failure():
    log = []
    with pytest.raises(RuntimeError):
        with Saga() as saga:
            saga.step("charge", lambda: log.append("charge"), undo=lambda: log.append("refund"))
            saga.step("ship", lambda: log.append("ship"), undo=lambda: log.append("cancel-ship"))

            def boom():
                raise RuntimeError("inventory gone")
            saga.step("reserve", boom, undo=lambda: log.append("unreserve"))
    # reserve's do failed (no undo recorded for it); ship + charge undo in reverse
    assert log == ["charge", "ship", "cancel-ship", "refund"]


def test_saga_no_rollback_on_success():
    log = []
    with Saga() as saga:
        saga.step("a", lambda: log.append("a"), undo=lambda: log.append("undo-a"))
        saga.step("b", lambda: log.append("b"), undo=lambda: log.append("undo-b"))
    assert log == ["a", "b"]  # no undos


# ---- context window ----

def test_context_compaction_moscow_within_budget():
    items = [
        ContextItem("M" * 40, Priority.MUST),       # ~10 tokens
        ContextItem("S" * 40, Priority.SHOULD),     # ~10 tokens
        ContextItem("C" * 40, Priority.COULD),      # ~10 tokens
        ContextItem("W" * 40, Priority.WONT),       # dropped outright
    ]
    kept = ContextWindowManager(max_tokens=22).compact(items)
    texts = [it.priority for it in kept]
    assert Priority.MUST in texts and Priority.SHOULD in texts
    assert Priority.WONT not in texts          # WON'T always dropped
    assert Priority.COULD not in texts          # budget (22) only fits MUST+SHOULD
    assert ContextWindowManager(1000).total_tokens(kept) <= 22


# ---- A2A ----

class _StubAgent(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence
        pass

    def do_GET(self):
        self._json({"name": "Stub Agent", "skills": [{"id": "echo"}]})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        import json as _j
        body = _j.loads(self.rfile.read(length) or b"{}")
        self._json({"conversation_id": body.get("conversation_id"),
                    "reply": "echo: " + body.get("text", ""), "path": "echo", "ok": True, "tool_calls": []})

    def _json(self, obj):
        import json as _j
        data = _j.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture()
def stub_agent():
    server = HTTPServer(("127.0.0.1", 0), _StubAgent)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_a2a_client_card_and_send(stub_agent):
    client = A2AClient(stub_agent)
    assert client.card()["name"] == "Stub Agent"
    reply = client.send("c1", "hello", "alice")
    assert reply["reply"] == "echo: hello" and reply["ok"]


def test_peer_tool_delegates(stub_agent):
    tool = peer_tool(stub_agent)
    out = tool({"conversation_id": "c1", "text": "delegate this", "user_id": "u"})
    assert out["reply"] == "echo: delegate this"
