"""Every tool call is bounded: a hung dependency ends an attempt, never the turn's thread."""

from __future__ import annotations

import threading
import time
import uuid

from agentic.bindings import Bindings
from agentic.events import InMemoryEventLog, Turn
from agentic.runtime import LocalRuntime, _Deployable


def _runtime(tool, fn):
    doc = {
        "spec_version": "agentic/v1",
        "agent": {
            "id": "a",
            "router": {"kind": "static", "default": "p"},
            "verifier": {"kind": "none"},
            "paths": {"p": {"brain": "rule", "tool_triggers": {"go": "call"}}},
        },
        "tools": [tool],
    }
    bindings = Bindings()
    bindings.tools["call"] = fn
    runtime = LocalRuntime(log=InMemoryEventLog())
    runtime.deploy(_Deployable(doc, bindings))
    return runtime


def test_a_function_tool_that_hangs_fails_the_attempt():
    release = threading.Event()

    def hangs(user):
        release.wait(30)
        return "late"

    runtime = _runtime({"id": "call", "kind": "function", "timeout_ms": 50, "effect": "idempotent"}, hangs)
    started = time.monotonic()
    result = runtime.submit(Turn(f"c-{uuid.uuid4()}", f"t-{uuid.uuid4()}", "go"))
    release.set()
    assert time.monotonic() - started < 5
    assert result["status"] == "failed"
    assert result["error"]["class"] == "tool"
    assert "timed out" in result["error"]["message"]


def test_a_function_tool_within_its_limit_still_returns():
    runtime = _runtime({"id": "call", "kind": "function", "timeout_ms": 2000}, lambda user: f"hi {user}")
    result = runtime.submit(Turn(f"c-{uuid.uuid4()}", f"t-{uuid.uuid4()}", "go", user_id="ben"))
    assert result["status"] == "completed"
    assert result["tool_calls"][0]["result"] == "hi ben"
