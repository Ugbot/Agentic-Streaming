"""`on_match.kind: submit`: a pattern that completes on a turn injects a derived turn into the
conversation, right after it and before any later turn. The derived turn has a turn_id fixed by the
turn and the pattern, so it runs once however often the triggering turn is delivered, and it is not
fed back to the patterns, so it cannot set one off again."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from agentic import Turn
from agentic.conformance import SimulatedCrash
from agentic.runtime import LocalRuntime

DOC: Dict[str, Any] = {
    "agent": {"id": "incident",
              "router": {"kind": "keyword", "default": "monitor", "rules": {"escalate": ["incident"]}},
              "paths": {"monitor": {"brain": "rule"},
                        "escalate": {"brain": "rule", "tools": ["open_ticket"],
                                     "tool_triggers": {"incident": "open_ticket"}}}},
    "policies": {"unattended_tools": ["open_ticket"]},
    "tools": [{"id": "open_ticket", "kind": "counting", "effect": "idempotent"}],
    "cep": [{"name": "host_incident", "ts": "metadata.ts", "within": 300000,
             "pattern": [{"stage": "first", "where": {"text_contains": "anomaly"}},
                         {"stage": "second", "where": {"text_contains": "anomaly"}, "contiguity": "followedBy"},
                         {"stage": "third", "where": {"text_contains": "anomaly"}, "contiguity": "followedBy"}],
             "on_match": {"kind": "submit", "text": "incident: 3 anomalies on {key}"}}],
}


def anomaly(n: int, ts: int) -> Turn:
    return Turn("host-7", f"t{n}", "anomaly: cpu high", "monitor", None, {"ts": str(ts)})


def turns(rt: LocalRuntime) -> List[Any]:
    return [(e.turn_id, e.payload.get("text")) for e in rt.events("host-7") if e.type == "turn_received"]


@pytest.fixture
def rt() -> LocalRuntime:
    runtime = LocalRuntime()
    runtime.deploy(DOC)
    return runtime


def test_three_anomalies_inject_an_incident_turn_that_opens_a_ticket(rt):
    rt.submit(anomaly(1, 0))
    rt.submit(anomaly(2, 60_000))
    assert len(turns(rt)) == 2
    third = rt.submit(anomaly(3, 120_000))
    assert third["status"] == "completed" and third["path"] == "monitor"
    derived = "t3/cep/host_incident"
    assert turns(rt)[-1] == (derived, "incident: 3 anomalies on host-7")
    assert third["runtime_detail"]["derived_turns"] == [{"turn_id": derived, "status": "completed"}]
    ticket = [e for e in rt.events("host-7") if e.turn_id == derived and e.type == "tool_called"]
    assert [e.payload["tool"] for e in ticket] == ["open_ticket"]
    assert next(e for e in rt.events("host-7") if e.turn_id == derived and e.type == "routed").payload["path"] == \
        "escalate"


def test_the_derived_turn_comes_before_a_later_one(rt):
    for n, ts in ((1, 0), (2, 1), (3, 2), (4, 3)):
        rt.submit(Turn("host-7", f"t{n}", "anomaly" if n < 4 else "all quiet", "m", None, {"ts": str(ts)}))
    assert [t for t, _ in turns(rt)] == ["t1", "t2", "t3", "t3/cep/host_incident", "t4"]


def test_a_redelivered_trigger_does_not_inject_twice(rt):
    for n in (1, 2, 3):
        rt.submit(anomaly(n, n))
    again = rt.submit(anomaly(3, 3))
    assert again["status"] == "duplicate"
    assert [t for t, _ in turns(rt)].count("t3/cep/host_incident") == 1


def test_a_crash_before_the_derived_turn_injects_it_on_redelivery(rt):
    rt.submit(anomaly(1, 1))
    rt.submit(anomaly(2, 2))
    received: List[str] = []

    def crash_on_the_derived_turn(axis: str, name: str, when: str) -> None:
        """The trigger's turn_received is the first; the derived turn's is the second."""
        if (axis, name, when) == ("event", "turn_received", "before"):
            received.append(name)
            if len(received) == 2:
                raise SimulatedCrash()

    assert rt.engine is not None
    rt.engine.probe = crash_on_the_derived_turn
    with pytest.raises(SimulatedCrash):
        rt.submit(anomaly(3, 3))
    rt = rt.restart()
    assert [t for t, _ in turns(rt)] == ["t1", "t2", "t3"]
    again = rt.submit(anomaly(3, 3))
    assert again["status"] == "duplicate"
    assert [t for t, _ in turns(rt)] == ["t1", "t2", "t3", "t3/cep/host_incident"]


def test_the_derived_turn_is_not_fed_back_to_the_patterns():
    doc = {**DOC, "cep": [{**DOC["cep"][0], "pattern": [{"stage": "only", "where": {"text_contains": "anomal"}}],
                          "on_match": {"kind": "submit", "text": "anomalies again on {key}"}}]}
    rt = LocalRuntime()
    rt.deploy(doc)
    rt.submit(anomaly(1, 1))
    assert [t for t, _ in turns(rt)] == ["t1", "t1/cep/host_incident"]


def test_a_derived_turn_does_not_fire_the_patterns_of_its_trigger_again():
    counted = {"id": "count_match", "kind": "counting", "effect": "idempotent"}
    tool_pattern = {**DOC["cep"][0], "name": "count_it", "on_match": {"kind": "tool", "tool": "count_match"}}
    doc = {**DOC, "tools": [*DOC["tools"], counted], "cep": [*DOC["cep"], tool_pattern],
           "policies": {"unattended_tools": ["open_ticket", "count_match"]}}
    rt = LocalRuntime()
    rt.deploy(doc)
    for n in (1, 2, 3):
        rt.submit(anomaly(n, n))
    calls = [e.turn_id for e in rt.events("host-7") if e.type == "tool_called" and e.payload["tool"] == "count_match"]
    assert calls == ["t3"]
