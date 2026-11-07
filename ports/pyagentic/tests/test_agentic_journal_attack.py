"""What the journal has to survive that the fixtures do not reach: a store that fails or stalls at
each write, a re-run that is not the run it replays, and more than one thread on a turn. Each test
pins what must never happen: a call with no record behind it, or one call answered with another's
result."""

from __future__ import annotations

import threading
import uuid
from typing import Any, Dict, List, Optional

import pytest

from agentic import Agent, Turn
from agentic.conformance import CrashPoint, SimulatedCrash
from agentic.errors import LogUnavailable, ValidationError
from agentic.events import InMemoryEventLog
from agentic.journal import (
    LLM,
    NON_IDEMPOTENT,
    TOOL,
    UNKNOWN,
    EffectCall,
    Entry,
    InMemoryEffectJournal,
    JournalDivergence,
    JournalKey,
    intent_of,
    model_intent_of,
)
from agentic.runtime import LocalRuntime

OPS = {"subject": "ops", "roles": ["approver"]}


def rid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class Flaky(InMemoryEffectJournal):
    """A store with a switch on each kind of write."""

    def __init__(self) -> None:
        super().__init__()
        self.refuse_intent = False
        self.refuse_outcome = False
        self.refuse_read = False

    def get(self, key: JournalKey) -> Optional[Entry]:
        if self.refuse_read:
            raise LogUnavailable("read refused")
        return super().get(key)

    def put(self, entry: Entry) -> None:
        if self.refuse_outcome:
            raise LogUnavailable("write refused")
        super().put(entry)

    def put_if_absent(self, entry: Entry) -> Optional[Entry]:
        if self.refuse_intent:
            raise LogUnavailable("intent refused")
        return super().put_if_absent(entry)


class Once(EffectCall):
    def __init__(self) -> None:
        self.made = 0

    def budget(self) -> int:
        return 3

    def attempt(self, attempt: int) -> Any:
        self.made += 1
        return "done"


def pay_runtime(journal: Any, log: Any = None, trigger: str = "pay", effect: str = "non_idempotent",
                name: str = "a") -> Any:
    calls: List[Any] = []

    def pay(**_: Any) -> str:
        calls.append(1)
        return f"paid {len(calls)}"

    spec = (Agent(name).approvers(roles=["approver"]).path("main", tools=["pay"], tool_triggers={trigger: "pay"}).use_tool("pay", pay, effect=effect)
            .verifier("none").build())
    rt = LocalRuntime(journal=journal, log=log if log is not None else InMemoryEventLog())
    rt.deploy(spec)
    return rt, calls


# -- a stale replay must not return another call's result ---------------------------------

KEY = JournalKey("c1", "t1", TOOL, 0)


def test_a_call_recorded_as_non_idempotent_stays_so_when_a_new_version_relaxes_the_tool():
    journal = InMemoryEffectJournal()
    journal.put(Entry.pending(KEY, "pay:1", NON_IDEMPOTENT))
    call = Once()
    entry = journal.record_or_get(KEY, "pay:1", "idempotent", call)
    assert entry.state == UNKNOWN
    assert call.made == 0


def test_the_workflow_is_part_of_a_calls_identity():
    assert intent_of("pay", {"a": 1}, "one") != intent_of("pay", {"a": 1}, "two")
    assert intent_of("pay", {"a": 1}) == intent_of("pay", {"a": 1}, "")


def test_a_number_and_its_string_are_different_arguments():
    assert intent_of("pay", {"a": 10}) != intent_of("pay", {"a": "10"})


def test_a_changed_model_request_at_a_recorded_index_diverges():
    journal = InMemoryEffectJournal()
    key = JournalKey("c1", "t1", LLM, 0)
    first = Once()
    journal.record_or_get(key, model_intent_of("", "one"), "read", first)
    again = Once()
    with pytest.raises(JournalDivergence):
        journal.record_or_get(key, model_intent_of("", "two"), "read", again)
    assert again.made == 0


def test_the_same_model_request_reuses_its_answer():
    journal = InMemoryEffectJournal()
    key = JournalKey("c1", "t1", LLM, 0)
    journal.record_or_get(key, model_intent_of("", "one"), "read", Once())
    again = Once()
    assert journal.record_or_get(key, model_intent_of("", "one"), "read", again).result == "done"
    assert again.made == 0


def test_another_workflow_on_the_same_store_does_not_inherit_an_entry():
    journal = InMemoryEffectJournal()
    first, calls = pay_runtime(journal, effect="read", name="one")
    first.submit(Turn("c", "t", "pay"))
    second, others = pay_runtime(journal, effect="read", name="two")
    result = second.submit(Turn("c", "t", "pay"))
    assert result["status"] == "failed" and result["error"]["class"] == "fatal"
    assert others == []


def test_a_rerun_that_stops_short_of_a_recorded_call_is_not_a_quiet_success():
    journal = InMemoryEffectJournal()
    first, calls = pay_runtime(journal)
    assert first.submit(Turn("c", "t", "pay now"))["status"] == "completed"
    changed, _ = pay_runtime(journal, trigger="nothing-matches")
    result = changed.submit(Turn("c", "t", "pay now"))
    assert result["status"] == "failed" and result["error"]["class"] == "fatal", result


# -- a store that fails ---------------------------------------------------------------------

def test_an_intent_that_cannot_be_written_stops_the_call():
    journal = Flaky()
    rt, calls = pay_runtime(journal)
    journal.refuse_intent = True
    with pytest.raises(LogUnavailable):
        rt.submit(Turn("c", "t", "pay"))
    assert calls == []


def test_an_outcome_that_cannot_be_written_is_unknown_and_never_repeated():
    journal = Flaky()
    log = InMemoryEventLog()
    rt, calls = pay_runtime(journal, log)
    journal.refuse_outcome = True
    with pytest.raises(LogUnavailable):
        rt.submit(Turn("c", "t", "pay"))
    assert len(calls) == 1
    journal.refuse_outcome = False
    result = rt.submit(Turn("c", "t", "pay"))
    assert result["status"] == "suspended", result
    assert len(calls) == 1


def test_an_outage_inside_a_saga_is_an_outage_not_a_step_failure():
    class SecondIntentFails(InMemoryEffectJournal):
        intents = 0

        def put_if_absent(self, entry: Entry) -> Optional[Entry]:
            self.intents += 1
            if self.intents == 2:
                raise LogUnavailable("the store went away")
            return super().put_if_absent(entry)

    log = InMemoryEventLog()
    doc: Dict[str, Any] = {
        "spec_version": "agentic/v1",
        "agent": {"id": rid("a"), "router": {"kind": "static", "default": "p"},
                  "paths": {"p": {"brain": "rule", "tools": ["reserve", "post", "release"]}},
                  "verifier": {"kind": "none"}},
        "tools": [{"id": "reserve", "kind": "constant", "value": "r", "effect": "idempotent", "compensation": "release"},
                  {"id": "post", "kind": "constant", "value": "p", "effect": "non_idempotent"},
                  {"id": "release", "kind": "constant", "value": "x", "effect": "idempotent"}],
        "saga": {"steps": [{"name": "reserve", "tool": "reserve"}, {"name": "post", "tool": "post"}]},
    }
    rt = LocalRuntime(journal=SecondIntentFails(), log=log)
    rt.deploy(doc)
    with pytest.raises(LogUnavailable):
        rt.submit(Turn("c", "t", "go"))
    kinds = [e.type for e in log.read("c")]
    assert "compensation_started" not in kinds, "an outage says nothing about the step"
    assert "turn_failed" not in kinds, "the turn id stays unspent"


def test_a_journal_that_cannot_open_is_not_degraded_under_a_non_idempotent_tool(tmp_path):
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("x")
    spec = (Agent(rid("a")).approvers(roles=["approver"]).path("main", tools=["pay"], tool_triggers={"pay": "pay"}).use_tool("pay", lambda **_: "ok", effect="non_idempotent")
            .with_journal("file", url=str(blocked), on_unavailable="degrade").verifier("none").build())
    with pytest.raises((LogUnavailable, ValidationError)):
        LocalRuntime().deploy(spec)


def test_a_journal_may_degrade_when_every_tool_is_safe_to_repeat(tmp_path):
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("x")
    spec = (Agent(rid("a")).approvers(roles=["approver"]).path("main", tools=["look"], tool_triggers={"look": "look"}).use_tool("look", lambda **_: "ok", effect="read")
            .with_journal("file", url=str(blocked), on_unavailable="degrade").verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    assert rt.degradations


def test_an_outage_before_a_resolution_changes_nothing():
    journal = Flaky()
    log = InMemoryEventLog()
    rt, calls = pay_runtime(journal, log)
    assert rt.engine is not None
    rt.engine.probe = CrashPoint({"call": "pay", "when": "during"})
    with pytest.raises(SimulatedCrash):
        rt.submit(Turn("c", "t", "pay"))
    rt.engine.probe = None
    assert rt.submit(Turn("c", "t", "pay"))["status"] == "suspended"
    before = len(log.read("c"))
    journal.refuse_read = True
    with pytest.raises(LogUnavailable):
        rt.submit({"conversation_id": "c", "turn_id": "t", "principal": OPS, "signal": {"resolution": "not_applied"}})
    assert len(log.read("c")) == before
    assert len(calls) == 1


# -- more than one thread -------------------------------------------------------------------

def test_one_turn_delivered_many_times_at_once_makes_one_call():
    rt, calls = pay_runtime(InMemoryEffectJournal())
    futures = [rt.submit_async(Turn("c", "t", "pay")) for _ in range(16)]
    statuses = sorted(f.result()["status"] for f in futures)
    assert statuses == ["completed"] + ["duplicate"] * 15
    assert len(calls) == 1


def test_two_writers_on_one_key_never_both_call():
    journal = InMemoryEffectJournal()
    gate = threading.Barrier(8)
    calls: List[Once] = [Once() for _ in range(8)]

    def race(call: Once) -> None:
        gate.wait()
        journal.record_or_get(KEY, "pay:1", NON_IDEMPOTENT, call)

    threads = [threading.Thread(target=race, args=(c,)) for c in calls]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(c.made for c in calls) == 1
