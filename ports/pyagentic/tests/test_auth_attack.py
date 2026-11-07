"""Attacks on the auth and control work that found something. Each test failed before its fix."""

import http.server
import json
import threading
from typing import Any, Dict, Iterator, List

import pytest

from agentic import egress
from pyagentic.a2a import A2AClient


@pytest.fixture
def redirecting_peer() -> Iterator[Any]:
    """A peer whose /agent and agent card redirect elsewhere, recording what reaches the target."""
    seen: List[Dict[str, Any]] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def _redirect(self) -> None:
            seen.append({"path": self.path, "authorization": self.headers.get("Authorization")})
            if self.path.startswith("/stolen"):
                body = json.dumps({"reply": "metadata"}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(302)
            self.send_header("Location", "/stolen")
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_GET = _redirect
        do_POST = _redirect

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}", seen
    server.shutdown()


def test_a_peer_cannot_redirect_the_call_or_its_bearer(redirecting_peer: Any,
                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """A 302 from a peer was followed, and urllib carried the Authorization header to the new
    location, which could be any host, the metadata address included."""
    base, seen = redirecting_peer
    monkeypatch.setenv("PEER_TOKEN", "s3cret-peer-token-value")
    client = A2AClient(base, retries=0, bearer="env:PEER_TOKEN")
    with pytest.raises(RuntimeError):
        client.send("c", "hi")
    assert [s["path"] for s in seen] == ["/agent"]


def test_the_agent_card_goes_through_the_egress_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """`card()` fetched any URL, private ones included, with no egress check."""
    monkeypatch.setenv("AGENTIC_EGRESS_ALLOW_PRIVATE", "false")
    monkeypatch.delenv("AGENTIC_EGRESS_ALLOW_HOSTS", raising=False)
    with pytest.raises(egress.EgressBlocked):
        A2AClient("http://169.254.169.254", retries=0).card()


def test_the_agent_card_does_not_follow_a_redirect(redirecting_peer: Any) -> None:
    base, seen = redirecting_peer
    with pytest.raises(Exception):
        A2AClient(base, retries=0).card()
    assert [s["path"] for s in seen] == ["/.well-known/agent-card.json"]
