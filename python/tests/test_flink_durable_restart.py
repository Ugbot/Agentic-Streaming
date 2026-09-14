"""The ``flink-jvm`` lane survives a restart mid-conversation without re-running brains or tools.

``FlinkRuntime.restart()`` stops the streaming job with a savepoint, tears the local cluster down,
and starts a new job restored from the savepoint (the same step the Flink JUnit conformance binding
takes for ``restart_runtime``). These tests count real tool invocations through an HTTP tool served
from the test process, so a re-invocation during recovery would show up as an extra request, and
they run the two restart fixtures (11 and 12) of ``spec/conformance/v1`` end to end.
"""

from __future__ import annotations

import json
import random
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
import yaml

from agentic_flink._contract import get_runtime
from agentic_flink.conformance import default_fixtures_dir, load_comparator, run_fixture
from agentic_flink.runtimes import FlinkRuntime
from agentic_flink.workflow import AgentSpec, Event

pytestmark = pytest.mark.usefixtures("af")

REPO_ROOT = Path(__file__).resolve().parents[2]
SUPPORT = REPO_ROOT / "spec" / "conformance" / "v1" / "workflows" / "support.yaml"


def _rnd(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class _CountingTool:
    """A ``kind: http`` tool endpoint that counts every call and answers a fixed JSON value."""

    def __init__(self) -> None:
        self.calls = 0
        self.value = round(random.uniform(1, 999), 2)
        counter = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 (http.server naming)
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                counter.calls += 1
                body = json.dumps({"value": counter.value}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/lookup"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def counting_tool():
    tool = _CountingTool()
    try:
        yield tool
    finally:
        tool.close()


def _support_with_http_lookup(url: str) -> AgentSpec:
    doc = yaml.safe_load(SUPPORT.read_text())
    doc["tools"] = [
        {"id": "lookup_charge", "kind": "http", "description": "Look up a charge", "url": url}
        if t["id"] == "lookup_charge" else t
        for t in doc["tools"]
    ]
    return AgentSpec(doc)


def test_restart_mid_conversation_keeps_the_log_and_reinvokes_nothing(counting_tool):
    cid, t1, t2, t3 = _rnd("c"), _rnd("t1"), _rnd("t2"), _rnd("t3")
    rt = get_runtime("flink-jvm", parallelism=random.randint(1, 2))
    assert isinstance(rt, FlinkRuntime) and rt.durable
    try:
        rt.deploy(_support_with_http_lookup(counting_tool.url))

        first = rt.submit(Event.turn(cid, t1, "what is my balance?"))
        assert first["status"] == "completed" and first["path"] == "billing"
        assert [c["tool"] for c in first["tool_calls"]] == ["lookup_charge"]
        assert first["tool_calls"][0]["result"] == {"value": counting_tool.value}
        assert counting_tool.calls == 1

        second = rt.submit(Event.turn(cid, t2, "I lost my password"))
        assert second["status"] == "completed" and second["path"] == "account"
        assert second["state"]["turn_count"] == 2

        rt.restart()  # stop with savepoint, cluster down, new job restored from the savepoint
        assert rt.session.restarts() == 1
        assert Path(str(rt.session.lastSavepoint()).removeprefix("file:")).exists()

        replayed = rt.submit(Event.turn(cid, t1, "what is my balance?"))
        assert replayed["status"] == "duplicate"
        assert replayed["events"] == [], "a replayed turn appends nothing to the log"
        assert replayed["state"]["turn_count"] == 2, "the state was rebuilt from the restored log"
        assert counting_tool.calls == 1, "recovery must not call the tool again"

        third = rt.submit(Event.turn(cid, t3, "what is my balance?"))
        assert third["status"] == "completed" and third["path"] == "billing"
        assert third["state"]["turn_count"] == 3
        assert counting_tool.calls == 2, "a genuinely new turn still calls the tool"
    finally:
        rt.close()
    assert rt.session is None


def test_suspended_turn_survives_restart_and_resumes():
    cid, tid = _rnd("c"), _rnd("t")
    doc = {
        "spec_version": "agentic/v1",
        "backend": "local",
        "agent": {
            "id": _rnd("approval"),
            "router": {"kind": "keyword", "default": "main", "rules": {"main": ["refund"]}},
            "paths": {"main": {"brain": "rule", "prompt": "You handle refunds.", "x-suspend-until": "approval"}},
            "verifier": {"kind": "none"},
        },
    }
    with get_runtime("flink-jvm") as rt:
        rt.deploy(AgentSpec(doc))
        parked = rt.submit(Event.turn(cid, tid, "refund my last charge"))
        assert parked["status"] == "suspended"
        assert "turn_suspended" in [e["type"] for e in parked["events"]]

        rt.restart()

        done = rt.submit(Event.resume(cid, tid, {"kind": "approval", "approved": True}))
        assert done["status"] == "completed"
        types = [e["type"] for e in done["events"]]
        assert "turn_resumed" in types and "turn_completed" in types
        assert "turn_received" not in types, "the resumed turn is not received a second time"


def test_bounded_mode_keeps_the_old_contract_and_refuses_restart():
    rt = get_runtime("flink-jvm", durable=False)
    caps = rt.capabilities()
    assert caps["durable_store"] == "unsupported" and caps["replay"] == "unsupported"
    with rt:
        rt.deploy(AgentSpec(yaml.safe_load(SUPPORT.read_text())))
        assert rt.session is None
        result = rt.submit(Event.turn(_rnd("c"), _rnd("t"), "hello there"))
        assert result["status"] == "completed" and result["path"] == "general"
        with pytest.raises(RuntimeError, match="durable=True"):
            rt.restart()


@pytest.mark.parametrize("fixture_name", ["11-replay-after-restart.yaml", "12-suspend-resume.yaml"])
def test_restart_fixtures_pass_on_flink_jvm(fixture_name: str):
    outcome = run_fixture(default_fixtures_dir() / fixture_name, get_runtime("flink-jvm"), load_comparator())
    assert outcome.status == "pass", "\n".join([outcome.fixture_id, outcome.reason or "", *outcome.problems])
