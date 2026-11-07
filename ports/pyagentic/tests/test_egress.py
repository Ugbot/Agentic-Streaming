"""The egress policy: what leaves the process, with nothing but the operator's settings to loosen it."""

import http.server
import threading
from typing import Any, Iterator, List

import pytest

from agentic import egress
from agentic.errors import ToolError
from agentic.llm import ModelRefused, OllamaChatClient
from agentic.tools import ToolCall, ToolRegistry, ToolSpec


def public(_: str) -> List[str]:
    return ["93.184.216.34"]


@pytest.mark.parametrize("url", [
    "file:///etc/passwd", "ftp://example.com/x", "gopher://example.com/", "http://user:pw@example.com/",
    "http://", "http:///path",
])
def test_what_is_not_a_plain_web_url_is_refused(url: str) -> None:
    with pytest.raises(egress.EgressBlocked):
        egress.check(url, {}, public)


@pytest.mark.parametrize("address", [
    "127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "224.0.0.1", "::1",
    "fc00::1", "fe80::1", "::ffff:10.0.0.1", "::ffff:8.8.8.8", "64:ff9b::a00:1", "2002:a00:1::",
])
def test_every_kind_of_private_address_is_refused(address: str) -> None:
    with pytest.raises(egress.EgressBlocked):
        egress.check("http://example.com/", {}, lambda _: [address])


def test_one_bad_address_among_good_ones_is_enough() -> None:
    with pytest.raises(egress.EgressBlocked):
        egress.check("http://example.com/", {}, lambda _: ["93.184.216.34", "10.0.0.1"])


def test_a_public_host_passes_and_the_allowlist_narrows_it() -> None:
    egress.check("https://example.com/", {}, public)
    only = {"AGENTIC_EGRESS_ALLOW_HOSTS": "*.example.com,api.other.org"}
    egress.check("https://a.example.com/", only, public)
    egress.check("https://api.other.org/", only, public)
    for url in ("https://example.com/", "https://evil.com/", "https://a.example.com.evil.com/"):
        with pytest.raises(egress.EgressBlocked):
            egress.check(url, only, public)


def test_only_the_operator_setting_opens_private_addresses() -> None:
    with pytest.raises(egress.EgressBlocked):
        egress.check("http://127.0.0.1:1/", {}, lambda _: ["127.0.0.1"])
    egress.check("http://127.0.0.1:1/", {"AGENTIC_EGRESS_ALLOW_PRIVATE": "true"}, lambda _: ["127.0.0.1"])


@pytest.fixture
def redirecting() -> Iterator[Any]:
    seen: List[str] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            seen.append(self.path)
            self.send_response(302)
            self.send_header("Location", "/internal")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}", seen
    server.shutdown()


def test_a_redirect_is_not_followed(redirecting: Any) -> None:
    base, seen = redirecting
    registry = ToolRegistry()
    registry.declare(ToolSpec(id="fetch", kind="http", url=f"{base}/start", timeout_ms=2000))
    with pytest.raises(ToolError, match="HTTP 302"):
        registry.execute("fetch", {}, ToolCall("c", "t", 0))
    assert seen == ["/start"]


def test_a_blocked_tool_sends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTIC_EGRESS_ALLOW_PRIVATE", "false")
    monkeypatch.delenv("AGENTIC_EGRESS_ALLOW_HOSTS")
    registry = ToolRegistry()
    registry.declare(ToolSpec(id="fetch", kind="http", url="http://127.0.0.1:9/x", timeout_ms=2000))
    with pytest.raises(ToolError, match="blocked by egress policy"):
        registry.execute("fetch", {}, ToolCall("c", "t", 0))


def test_a_blocked_model_is_a_refusal_not_a_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTIC_EGRESS_ALLOW_PRIVATE", "false")
    monkeypatch.delenv("AGENTIC_EGRESS_ALLOW_HOSTS")
    client = OllamaChatClient("m", "http://169.254.169.254:11434", 2000)
    with pytest.raises(ModelRefused, match="egress policy"):
        client.chat([{"role": "user", "content": "hi"}], [])
