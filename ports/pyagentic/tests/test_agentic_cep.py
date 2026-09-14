"""Sequence patterns (`cep:`) as a pure fold over the conversation log: the fold against a naive
matcher on random turn sequences, and the in-turn wiring (tool call recorded on the turn that
completes the match)."""

from __future__ import annotations

import random
import uuid
from typing import Any, Dict, List, Optional

import pytest

from agentic import Turn, ValidationError
from agentic.cep import (
    SequencePattern,
    Stage,
    compile_patterns,
    event_time_ms,
    turns_of,
    without_tool_actions,
)
from agentic.cep import Turn as CepTurn
from agentic.events import InMemoryEventLog
from agentic.runtime import LocalRuntime

WORDS = ["anomaly", "heartbeat", "ok", "disk", "cpu", "noise"]


def rid(prefix: str = "id") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# -- a naive matcher --------------------------------------------------------------------

def _naive_completed_indexes(pattern: SequencePattern, turns: List[CepTurn]) -> List[int]:
    """Scan left to right; start an attempt at the first turn matching stage one and walk the
    remaining stages by explicit search. A turn outside the window ends the attempt and is where
    the scan resumes; a turn breaking a `next` stage ends the attempt and is consumed; a completed
    match is consumed whole."""
    stages = pattern.stages
    completed: List[int] = []
    n = len(turns)
    i = 0
    while i < n:
        if not stages[0].matches(turns[i].text):
            i += 1
            continue
        if len(stages) == 1:
            completed.append(i)
            i += 1
            continue
        start_ts = pattern.timestamp(turns[i]) if pattern.ts_key else 0
        stage = 1
        end: Optional[int] = None
        resume_at = n
        for j in range(i + 1, n):
            if pattern.within_ms is not None and pattern.timestamp(turns[j]) - start_ts > pattern.within_ms:
                resume_at = j
                break
            if stages[stage].matches(turns[j].text):
                stage += 1
                if stage == len(stages):
                    end = j
                    break
            elif stages[stage].contiguity == "next":
                resume_at = j + 1
                break
        if end is None:
            i = resume_at
        else:
            completed.append(end)
            i = end + 1
    return completed


def _random_pattern(rng: random.Random, timed: bool) -> SequencePattern:
    stages = tuple(
        Stage(f"s{k}", rng.choice(WORDS + [None]), rng.choice(["next", "followedBy"]))
        for k in range(rng.randint(1, 3)))
    return SequencePattern(rid("p"), stages, "open_ticket",
                           "event_time_ms" if timed else None,
                           rng.randint(1, 50) * 1000 if timed else None)


def _random_turns(rng: random.Random, n: int) -> List[CepTurn]:
    ts = rng.randint(0, 1000)
    turns = []
    for i in range(n):
        # mostly increasing event time; sometimes a late turn arrives with an earlier stamp
        ts += -rng.randint(0, 12) * 1000 if rng.random() < 0.1 else rng.randint(0, 20) * 1000
        text = " ".join(rng.choice(WORDS) for _ in range(rng.randint(0, 3)))
        if rng.random() < 0.3:
            text = text.upper()
        turns.append(CepTurn(f"t{i}", text, {"event_time_ms": str(ts)}))
    return turns


def test_fold_agrees_with_naive_matcher_on_random_sequences():
    rng = random.Random()
    for _ in range(400):
        pattern = _random_pattern(rng, timed=rng.random() < 0.7)
        turns = _random_turns(rng, rng.randint(0, 12))
        expected = set(_naive_completed_indexes(pattern, turns))
        for n in range(len(turns) + 1):
            prefix = turns[:n]
            completes = pattern.completes_on(prefix)
            assert completes == ((n - 1) in expected), (pattern, [t.text for t in prefix], n)


def test_stage_match_is_case_insensitive_substring():
    stage = Stage("s", "ANOMaly", "next")
    assert stage.matches("An Anomaly: cpu")
    assert not stage.matches("heartbeat")
    assert Stage("s", None, "next").matches("")


def test_completed_matches_consume_their_turns():
    pattern = SequencePattern("p", (Stage("a", "x", "next"), Stage("b", "x", "next")), "t")
    turns = [CepTurn(f"t{i}", "x", {}) for i in range(5)]
    completes = [pattern.completes_on(turns[:n]) for n in range(1, 6)]
    assert completes == [False, True, False, True, False]


def test_within_is_measured_from_the_first_matched_turn():
    pattern = SequencePattern("p", (Stage("a", "x", "next"), Stage("b", "x", "followedBy")),
                              "t", "event_time_ms", 1000)
    at = lambda i, ts: CepTurn(f"t{i}", "x", {"event_time_ms": str(ts)})  # noqa: E731
    assert pattern.completes_on([at(0, 0), at(1, 1000)])
    assert not pattern.completes_on([at(0, 0), at(1, 1001)])
    assert pattern.completes_on([at(0, 0), at(1, 5000), at(2, 5500)])


def test_missing_or_bad_timestamp_is_a_validation_error():
    pattern = SequencePattern("p", (Stage("a", "x", "next"), Stage("b", "x", "next")),
                              "t", "event_time_ms", 1000)
    with pytest.raises(ValidationError, match="lacks metadata.event_time_ms"):
        pattern.completes_on([CepTurn("t0", "x", {})])
    with pytest.raises(ValidationError, match="not an integer"):
        pattern.completes_on([CepTurn("t0", "x", {"event_time_ms": "soon"})])
    with pytest.raises(ValidationError, match="within without ts"):
        SequencePattern("p", (Stage("a", None, "next"),), "t", None, 5)
    assert event_time_ms({"event_time_ms": " 42 "}) == 42
    assert event_time_ms(None) is None
    with pytest.raises(ValidationError):
        event_time_ms({"event_time_ms": "later"})


def test_compile_keeps_only_tool_actions_for_the_in_turn_fold():
    tool_rule = {"name": "a", "pattern": [{"stage": "s"}], "on_match": {"kind": "tool", "tool": "t"}}
    other_rule = {"name": "b", "pattern": [{"stage": "s"}], "on_match": {"kind": "emit", "event": "x"}}
    compiled = compile_patterns([tool_rule, other_rule])
    assert [p.name for p in compiled] == ["a"]
    assert without_tool_actions([tool_rule, other_rule]) == [other_rule]
    assert compile_patterns(None) == []
    with pytest.raises(ValidationError, match="without a tool id"):
        compile_patterns([{"name": "c", "pattern": [{"stage": "s"}], "on_match": {"kind": "tool"}}])
    with pytest.raises(ValidationError, match="keyed by"):
        compile_patterns([dict(tool_rule, key="user_id")])
    with pytest.raises(ValidationError, match="metadata.<key>"):
        compile_patterns([dict(tool_rule, ts="payload.ts")])


# -- wiring ----------------------------------------------------------------------------

def _workflow(within: int) -> Dict[str, Any]:
    return {
        "spec_version": "agentic/v1",
        "backend": "local",
        "agent": {
            "id": rid("monitor"),
            "router": {"kind": "keyword", "default": "monitor"},
            "paths": {"monitor": {"brain": "rule", "prompt": "You acknowledge signals."}},
        },
        "tools": [{"id": "open_ticket", "kind": "constant", "value": "TICKET-OPENED"}],
        "cep": [{
            "name": "host_incident",
            "key": "conversation_id",
            "ts": "metadata.event_time_ms",
            "within": within,
            "pattern": [
                {"stage": "first", "where": {"text_contains": "anomaly"}},
                {"stage": "second", "where": {"text_contains": "anomaly"}, "contiguity": "followedBy"},
            ],
            "on_match": {"kind": "tool", "tool": "open_ticket"},
        }],
    }


def _turn(conversation: str, turn_id: str, text: str, ts: int) -> Turn:
    return Turn(conversation, turn_id, text, metadata={"event_time_ms": str(ts)})


def test_match_tool_call_is_recorded_on_the_completing_turn_and_replays_from_the_log():
    rng = random.Random()
    log = InMemoryEventLog()
    rt = LocalRuntime(log=log)
    rt.deploy(_workflow(300000))
    host = rid("host")
    other = rid("host")
    step = rng.randint(1000, 60000)
    r1 = rt.submit(_turn(host, "a1", "Anomaly: cpu", 0))
    r2 = rt.submit(_turn(host, "a2", "heartbeat ok", step))
    rb = rt.submit(_turn(other, "b1", "anomaly: disk", step))
    r3 = rt.submit(_turn(host, "a3", "anomaly: io", 2 * step))
    r4 = rt.submit(_turn(host, "a4", "anomaly: again", 3 * step))
    assert [r["tool_calls"] for r in (r1, r2, rb, r4)] == [[], [], [], []]
    assert [(c["tool"], c["index"], c["args"], c["result"]) for c in r3["tool_calls"]] == [
        ("open_ticket", 0, {"pattern": "host_incident", "key": host}, "TICKET-OPENED")]
    kinds = [e["type"] for e in r3["events"]]
    assert kinds[:4] == ["turn_received", "routed", "tool_called", "brain_started"]
    assert kinds[-1] == "turn_completed"
    received = [e for e in log.read(host) if e.type == "turn_received"]
    assert [e.payload["event_time_ms"] for e in received] == [0, step, 2 * step, 3 * step]

    pattern = compile_patterns(_workflow(300000)["cep"])[0]
    turns = turns_of(log.read(host))
    assert [t.turn_id for t in turns] == ["a1", "a2", "a3", "a4"]
    assert [pattern.completes_on(turns[:n]) for n in range(1, 5)] == [False, False, True, False]
    rt.close()


def test_out_of_window_turn_does_not_complete_a_match():
    rt = LocalRuntime()
    rt.deploy(_workflow(1000))
    host = rid("host")
    rt.submit(_turn(host, "a1", "anomaly one", 0))
    late = rt.submit(_turn(host, "a2", "anomaly two", 1001))
    assert late["tool_calls"] == []
    third = rt.submit(_turn(host, "a3", "anomaly three", 1500))
    assert third["tool_calls"][0]["args"] == {"pattern": "host_incident", "key": host}
    rt.close()


def test_missing_event_time_fails_the_turn_as_validation():
    rt = LocalRuntime()
    rt.deploy(_workflow(1000))
    host = rid("host")
    rt.submit(_turn(host, "a1", "anomaly one", 0))
    result = rt.submit(Turn(host, "a2", "anomaly two"))
    assert result["status"] == "failed"
    assert result["error"]["class"] == "validation"
    assert "event_time_ms" in result["error"]["message"]
    rt.close()


def test_local_runtime_declares_cep_and_event_time():
    caps = LocalRuntime().capabilities()
    assert caps["cep"] == "supported"
    assert caps["event_time"] == "supported"
