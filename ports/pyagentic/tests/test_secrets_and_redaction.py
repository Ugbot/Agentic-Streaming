"""A secret is named, not written, and it reaches the wire and nothing that is recorded."""

import http.server
import threading
from pathlib import Path
from typing import Any, List

import pytest

from agentic import secret_refs
from agentic.errors import ValidationError
from agentic.events import Turn
from agentic.redact import Redactor
from agentic.runtime import LocalRuntime


def doc(url: str, bearer: str) -> dict:
    return {"spec_version": "agentic/v1",
            "agent": {"id": "a", "router": {"kind": "static", "default": "main"},
                      "paths": {"main": {"brain": "rule", "tools": ["call"], "tool_triggers": {"go": "call"}}},
                      "verifier": {"kind": "none"}},
            "tools": [{"id": "call", "kind": "http", "effect": "idempotent", "url": url, "auth": {"bearer": bearer}}]}


def test_a_secret_in_a_document_is_refused() -> None:
    with pytest.raises(ValidationError):
        LocalRuntime().deploy(doc("http://127.0.0.1:9/", "my-actual-token"))


def test_the_secret_reaches_the_tool_and_nothing_that_is_recorded(tmp_path: Path) -> None:
    value = "tool-secret-value"
    file = tmp_path / "token"
    file.write_text(value + "\n")
    seen: List[Any] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            seen.append(self.headers.get("Authorization"))
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        rt = LocalRuntime()
        rt.deploy(doc(f"http://127.0.0.1:{server.server_address[1]}/", f"file:{file}"))
        result = rt.submit(Turn("c", "t", "go"))
    finally:
        server.shutdown()
    assert result["status"] == "completed"
    assert seen == [f"Bearer {value}"]
    assert value not in str(result)


def test_a_reference_that_names_nothing_says_only_that() -> None:
    with pytest.raises(secret_refs.SecretUnresolved, match="AGENTIC_NOT_SET_ANYWHERE"):
        secret_refs.resolve("env:AGENTIC_NOT_SET_ANYWHERE")
    with pytest.raises(secret_refs.SecretUnresolved):
        secret_refs.resolve("hunter2")


def test_the_redactor_covers_credentials_and_what_the_workflow_names() -> None:
    redactor = Redactor({"policies": {"redact": {"fields": ["ssn"], "patterns": [r"\b\d{3}-\d{2}-\d{4}\b"]}}})
    assert redactor.apply({"SSN": "x", "Password": "y", "ok": 1}) == {"SSN": "[redacted]", "Password": "[redacted]", "ok": 1}
    assert redactor.apply(["id 078-05-1120", {"n": ["Bearer abcdefghijklmnop"]}]) == ["id [redacted]", {"n": ["[redacted]"]}]
    assert redactor.apply("key AKIAABCDEFGHIJKLMNOP and sk-abcdefghijklmnopqrstuvwx") == "key [redacted] and [redacted]"
    assert redactor.apply("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJlLXZhbHVl") == "[redacted]"
    assert redactor.apply(7) == 7
