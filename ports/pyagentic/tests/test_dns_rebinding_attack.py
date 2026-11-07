"""DNS rebinding between the check and the connect.

The egress check resolved a host and found only public addresses, and then urllib resolved it again to
connect. A name whose answer changes in between, which is what a rebinding server does, passed the
check as a public address and was then reached on loopback. The check now resolves once, checks every
address, and the connection goes to an address it checked, with the host name kept for `Host` and for
TLS (SNI and the certificate)."""

from __future__ import annotations

import http.server
import socket
import ssl
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentic import egress  # noqa: E402
from agentic.events import Turn  # noqa: E402
from agentic.llm import OpenAIChatClient, TransientError  # noqa: E402
from agentic.runtime import LocalRuntime  # noqa: E402
from pyagentic.a2a import A2AClient  # noqa: E402

PUBLIC = "8.8.8.8"  # what the rebinding name says first: a public address, which passes the check
_real_getaddrinfo = socket.getaddrinfo


class _Recorder(http.server.BaseHTTPRequestHandler):
    seen: List[Dict[str, Any]] = []

    def _answer(self) -> None:
        type(self).seen.append({"path": self.path, "host": self.headers.get("Host")})
        body = b'{"ok": true, "choices": [{"message": {"content": "{\\"text\\": \\"hi\\"}"}}]}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = _answer

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture()
def internal() -> Iterator[int]:
    """An internal service on loopback, which nothing outside should reach."""
    _Recorder.seen = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1]
    server.shutdown()


@pytest.fixture()
def rebinding(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """`rebind.test` answers a public address the first time it is asked and loopback after that."""
    asked: List[str] = []

    def getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
        if host == "rebind.test":
            asked.append(host)
            address = PUBLIC if len(asked) == 1 else "127.0.0.1"
            return _real_getaddrinfo(address, port, *args, **kwargs)
        return _real_getaddrinfo(host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setenv("AGENTIC_EGRESS_ALLOW_PRIVATE", "false")
    monkeypatch.delenv("AGENTIC_EGRESS_ALLOW_HOSTS", raising=False)
    return asked


def _tool_doc(url: str) -> dict:
    return {"spec_version": "agentic/v1",
            "agent": {"id": "a", "router": {"kind": "static", "default": "main"},
                      "paths": {"main": {"brain": "rule", "tools": ["call"], "tool_triggers": {"go": "call"}}},
                      "verifier": {"kind": "none"}},
            "tools": [{"id": "call", "kind": "http", "effect": "idempotent", "url": url, "timeout_ms": 1500}]}


def test_an_http_tool_does_not_reach_loopback_through_a_rebinding_name(internal: int, rebinding: List[str]) -> None:
    rt = LocalRuntime()
    rt.deploy(_tool_doc(f"http://rebind.test:{internal}/"))
    result = rt.submit(Turn("c", "t", "go"))
    assert _Recorder.seen == [], "the internal service was reached"
    assert result["status"] != "completed"
    assert len(rebinding) == 1, "the name is resolved once"


def test_a_model_call_does_not_reach_loopback_through_a_rebinding_name(internal: int, rebinding: List[str]) -> None:
    client = OpenAIChatClient("m", f"http://rebind.test:{internal}/v1", 1500, api_key="k")
    with pytest.raises(TransientError):
        client.chat([{"role": "user", "content": "hi"}], [])
    assert _Recorder.seen == []


def test_a_peer_does_not_reach_loopback_through_a_rebinding_name(internal: int, rebinding: List[str]) -> None:
    client = A2AClient(f"http://rebind.test:{internal}", retries=0, timeout=1.5)
    with pytest.raises(Exception):
        client.send("c", "hi")
    with pytest.raises(Exception):
        A2AClient(f"http://rebind.test:{internal}", retries=0, timeout=1.5).card()
    assert _Recorder.seen == []


# A pinned connection still speaks to the name: Host, SNI and the certificate are the host's.

@pytest.fixture()
def tls_server(tmp_path: Path) -> Iterator[Dict[str, Any]]:
    key, cert = tmp_path / "key.pem", tmp_path / "cert.pem"
    made = subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                           "-subj", "/CN=pinned.test", "-addext", "subjectAltName=DNS:pinned.test",
                           "-keyout", str(key), "-out", str(cert)], capture_output=True)
    if made.returncode != 0:
        pytest.skip("openssl could not make a certificate")
    names: List[Any] = []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.sni_callback = lambda sock, name, ctx: names.append(name)
    _Recorder.seen = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield {"port": server.server_address[1], "cert": cert, "sni": names}
    server.shutdown()


def test_a_pinned_https_connection_keeps_the_host_for_sni_host_and_the_certificate(tls_server: Dict[str, Any]) -> None:
    import urllib.request

    trust = ssl.create_default_context(cafile=str(tls_server["cert"]))
    port = tls_server["port"]
    request = urllib.request.Request(f"https://pinned.test:{port}/x")
    with egress.urlopen(request, 5, egress.Pin("pinned.test", ["127.0.0.1"]), context=trust) as response:
        assert response.status == 200
    assert tls_server["sni"] == ["pinned.test"]
    assert _Recorder.seen == [{"path": "/x", "host": f"pinned.test:{port}"}]
    # The certificate is checked against the name, so another name's certificate is refused.
    with pytest.raises(Exception):
        egress.urlopen(urllib.request.Request(f"https://other.test:{port}/x"), 5,
                       egress.Pin("other.test", ["127.0.0.1"]), context=trust)
