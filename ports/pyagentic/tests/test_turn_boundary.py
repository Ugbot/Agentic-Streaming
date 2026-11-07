"""The turn boundary: nothing a turn does reaches the caller as an exception."""

from __future__ import annotations

import uuid

from agentic.bindings import Bindings
from agentic.events import InMemoryEventLog, Turn
from agentic.runtime import LocalRuntime, _Deployable


def _doc(**extra):
    doc = {
        "spec_version": "agentic/v1",
        "agent": {
            "id": "a",
            "router": {"kind": "static", "default": "p"},
            "verifier": {"kind": "none"},
            "paths": {"p": {"brain": "rule", "x-brain": "broken"}},
        },
    }
    doc.update(extra)
    return doc


def _runtime(log=None):
    def broken(view):
        raise KeyError("a bug in a custom brain")

    bindings = Bindings()
    bindings.brains["broken"] = broken
    runtime = LocalRuntime(log=log or InMemoryEventLog())
    runtime.deploy(_Deployable(_doc(), bindings))
    return runtime


def test_an_unexpected_exception_ends_the_turn_fatal_and_is_recorded():
    log = InMemoryEventLog()
    runtime = _runtime(log)
    cid, tid = f"c-{uuid.uuid4()}", f"t-{uuid.uuid4()}"
    result = runtime.submit(Turn(cid, tid, "hello"))
    assert result["status"] == "failed"
    assert result["error"]["class"] == "fatal"
    assert [e.type for e in log.read(cid)][-1] == "turn_failed"


def test_a_redelivery_of_a_fatal_turn_is_a_duplicate_not_a_rerun():
    log = InMemoryEventLog()
    runtime = _runtime(log)
    cid, tid = f"c-{uuid.uuid4()}", f"t-{uuid.uuid4()}"
    runtime.submit(Turn(cid, tid, "hello"))
    written = len(log.read(cid))
    assert runtime.submit(Turn(cid, tid, "hello"))["status"] == "duplicate"
    assert len(log.read(cid)) == written


def test_without_turn_idempotency_a_redelivered_transient_failure_is_a_new_turn():
    doc = _doc(policies={"idempotency": "none"},
               llm={"provider": "stub", "script": [{"text": "ok", "x-fail-attempts": 1}]})
    doc["agent"]["paths"]["p"] = {"brain": "llm"}
    runtime = LocalRuntime(log=InMemoryEventLog())
    runtime.deploy(_Deployable(doc, Bindings()))
    cid = f"c-{uuid.uuid4()}"
    assert runtime.submit(Turn(cid, "t", "hello"))["status"] == "failed"
    assert runtime.submit(Turn(cid, "t", "hello"))["state"]["turn_count"] == 2
