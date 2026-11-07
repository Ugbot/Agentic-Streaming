"""Effects and recovery past what the fixtures reach: section 10 through `LocalRuntime`, with real
Python tools, real stores and crashes at named points."""

from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import Future
from typing import Any, Dict, List, Optional

import pytest

from agentic import Agent, OutcomeUnknown, Turn, current_call
from agentic.conformance import CrashPoint, SimulatedCrash
from agentic.errors import LogUnavailable, ToolError, ValidationError
from agentic.events import FileEventLog, InMemoryEventLog
from agentic.ir import validate_result
from agentic.journal import FileEffectJournal, InMemoryEffectJournal
from agentic.runtime import LocalRuntime
from agentic.tools import Delegation, idempotency_key
from agentic.workflow_timers import ManualClock


def rid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def crash(rt: LocalRuntime, delivery: Any, **plan: Any) -> LocalRuntime:
    """Deliver a turn, die at `plan`, and come back from what the runtime keeps."""
    assert rt.engine is not None
    rt.engine.probe = CrashPoint(plan)
    with pytest.raises(SimulatedCrash):
        rt.submit(delivery)
    return rt.restart()


def pay_agent(fn: Any, effect: str = "non_idempotent", **path: Any) -> Any:
    return (Agent(rid("a")).path("main", tools=["pay"], tool_triggers={"pay": "pay"}, **path).use_tool("pay", fn, effect=effect)
            .verifier("none").build())


# -- effect classes and the key --------------------------------------------------------

def test_a_function_tool_sees_its_key_and_every_attempt_carries_the_same_one():
    seen: List[Any] = []

    def pay(user: str) -> str:
        call = current_call()
        seen.append((call.index, call.idempotency_key))
        if len(seen) < 2:
            raise ToolError("try again")
        return "ok"

    agent = (Agent(rid("a")).path("main", tools=["pay"], tool_triggers={"pay": "pay"}).use_tool("pay", pay, effect="idempotent")
             .verifier("none"))
    spec = agent.policies(retry={"kind": "fixed", "max_attempts": 2, "initial_delay_ms": 0}).build()
    rt = LocalRuntime()
    rt.deploy(spec)
    assert rt.submit(Turn("c1", "t1", "pay"))["status"] == "completed"
    assert seen == [(0, idempotency_key("c1", "t1", 0))] * 2
    assert current_call() is None


def test_a_peer_is_passed_the_key():
    seen: List[Delegation] = []

    def peer(args: Any, delegation: Delegation) -> str:
        seen.append(delegation)
        return "done"

    spec = (Agent(rid("a")).path("main", tools=["expert"], tool_triggers={"ask": "expert"}).use_peer("expert", peer)
            .verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    rt.submit(Turn("c1", "t1", "ask"))
    assert seen[0].idempotency_key == idempotency_key("c1", "t1", 0)


def test_non_idempotent_gets_one_attempt_whatever_the_retry_policy():
    calls: List[str] = []

    def pay(user: str) -> str:
        calls.append(user)
        raise ValueError("declined")

    agent = (Agent(rid("a")).path("main", tools=["pay"], tool_triggers={"pay": "pay"}).use_tool("pay", pay, effect="non_idempotent")
             .verifier("none"))
    spec = agent.policies(retry={"kind": "fixed", "max_attempts": 5, "initial_delay_ms": 0}).build()
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert (result["status"], result["error"]["class"], len(calls)) == ("failed", "tool", 1)


def test_a_function_that_times_out_is_unknown_and_one_that_raises_is_not():
    release = threading.Event()
    agent = Agent(rid("a")).path("main", tools=["pay"], tool_triggers={"pay": "pay"})
    agent.use_tool("pay", lambda user: release.wait(5), effect="non_idempotent", timeout_ms=20)
    rt = LocalRuntime()
    rt.deploy(agent.verifier("none").build())
    result = rt.submit(Turn("c1", "t1", "pay"))
    release.set()
    assert result["status"] == "suspended"
    assert "unknown_outcome" in [e["type"] for e in result["events"]]


def test_a_tool_may_say_its_own_outcome_is_unknown():
    def pay(user: str) -> str:
        raise OutcomeUnknown("the gateway hung up")

    rt = LocalRuntime()
    rt.deploy(pay_agent(pay))
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert result["status"] == "suspended"
    unknown = next(e for e in result["events"] if e["type"] == "unknown_outcome")
    assert unknown["payload"]["reason"] == "the gateway hung up"


# -- refusal ---------------------------------------------------------------------------

@pytest.mark.parametrize("event", [{"conversation_id": "c1", "turn_id": "", "text": "pay"},
                                   {"conversation_id": "c1", "text": "pay"}])
def test_a_mapping_without_a_turn_id_is_refused_and_appends_nothing(event):
    rt = LocalRuntime()
    rt.deploy(pay_agent(lambda user: "ok"))
    result = rt.submit(event)
    assert (result["status"], result["error"]["class"], result["events"]) == ("failed", "validation", [])
    assert (result["conversation_id"], result["turn_id"]) == ("c1", "")
    validate_result(result)
    assert list(rt.events("c1")) == []
    future: "Future[Dict[str, Any]]" = rt.submit_async(event)
    assert future.result(5)["status"] == "failed"
    assert list(rt.events("c1")) == []


def test_building_a_turn_with_no_id_still_raises():
    with pytest.raises(ValidationError):
        Turn("c1", "")


# -- stores ----------------------------------------------------------------------------

def declared(effects: Optional[Dict[str, Any]], **conversation: Any) -> Dict[str, Any]:
    doc = (Agent(rid("a")).path("main", tools=["pay"], tool_triggers={"pay": "pay"}).tool("pay", "constant", value="ok", effect="read")
           .verifier("none").build().document)
    stores: Dict[str, Any] = dict(doc.get("stores") or {})
    if effects is not None:
        stores["effects"] = effects
    if conversation:
        stores["conversation"] = conversation
    return {**doc, "stores": stores}


def test_the_default_journal_is_in_memory_and_shared_across_restart():
    rt = LocalRuntime()
    rt.deploy(pay_agent(lambda user: "ok"))
    journal = rt.journal
    assert isinstance(journal, InMemoryEffectJournal)
    assert rt.restart().journal is journal


def test_a_memory_block_selects_memory(tmp_path):
    rt = LocalRuntime(store_dir=str(tmp_path))
    rt.deploy(declared({"kind": "memory"}))
    assert isinstance(rt.journal, InMemoryEffectJournal)
    rt.close()


def test_a_file_block_selects_a_file_at_its_url(tmp_path):
    rt = LocalRuntime()
    rt.deploy(declared({"kind": "file", "url": str(tmp_path / "fx")}))
    assert isinstance(rt.journal, FileEffectJournal)
    assert rt.submit(Turn("c1", "t1", "pay"))["status"] == "completed"
    rt.close()
    assert os.path.exists(tmp_path / "fx" / FileEffectJournal.FILE)


def test_a_file_block_with_nowhere_to_go_is_a_validation_error():
    with pytest.raises(ValidationError, match="/stores/effects/url"):
        LocalRuntime().deploy(declared({"kind": "file"}))


def test_store_dir_puts_a_file_journal_beside_the_log_without_inventing_a_conversation(tmp_path):
    rt = LocalRuntime(store_dir=str(tmp_path))
    rt.deploy(pay_agent(lambda user: "ok", effect="idempotent"))
    assert isinstance(rt.journal, FileEffectJournal)
    rt.submit(Turn("c1", "t1", "pay"))
    assert rt.log is not None and list(rt.log.conversation_ids()) == ["c1"]
    rt.close()


def test_a_file_conversation_store_gets_a_file_journal_too(tmp_path):
    rt = LocalRuntime()
    rt.deploy(declared(None, kind="file", url=str(tmp_path)))
    assert isinstance(rt.journal, FileEffectJournal)
    rt.close()


def test_an_unknown_kind_is_a_validation_error():
    with pytest.raises(ValidationError, match="/stores/effects/kind"):
        LocalRuntime().deploy(declared({"kind": "postgres"}))


def test_an_unknown_kind_degrades_only_when_told_to_and_says_so():
    rt = LocalRuntime()
    rt.deploy(declared({"kind": "postgres", "on_unavailable": "degrade"}))
    assert isinstance(rt.journal, InMemoryEffectJournal)
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert any("stores.effects" in d for d in result["runtime_detail"]["degraded"])


def test_a_journal_that_cannot_be_opened_fails_the_build(tmp_path):
    blocked = tmp_path / "file"
    blocked.write_text("in the way")
    with pytest.raises(LogUnavailable):
        LocalRuntime().deploy(declared({"kind": "file", "url": str(blocked)}))
    rt = LocalRuntime()
    rt.deploy(declared({"kind": "file", "url": str(blocked), "on_unavailable": "degrade"}))
    assert isinstance(rt.journal, InMemoryEffectJournal)


def test_a_store_that_fails_mid_turn_stops_the_call_and_reaches_the_caller(tmp_path):
    calls: List[str] = []
    rt = LocalRuntime(store_dir=str(tmp_path))
    rt.deploy(pay_agent(lambda user: calls.append(user), effect="idempotent"))
    assert rt.journal is not None
    rt.journal.close()
    with pytest.raises(LogUnavailable):
        rt.submit(Turn("c1", "t1", "pay"))
    assert calls == []


def test_restart_on_a_file_store_reopens_it(tmp_path):
    calls: List[str] = []
    rt = LocalRuntime(store_dir=str(tmp_path))
    rt.deploy(pay_agent(lambda user: calls.append(user) or "ok"))
    first = rt.submit(Turn("c1", "t1", "pay"))
    rt = rt.restart()
    assert rt.submit(Turn("c1", "t2", "pay"))["status"] == "completed"
    assert rt.submit(Turn("c1", "t1", "pay"))["reply"] == first["reply"]
    assert len(calls) == 2
    rt.close()


def test_a_crash_on_a_file_store_never_repeats_a_non_idempotent_call(tmp_path):
    calls: List[str] = []
    rt = LocalRuntime(store_dir=str(tmp_path))
    rt.deploy(pay_agent(lambda user: calls.append(user) or "ok"))
    rt = crash(rt, Turn("c1", "t1", "pay"), call="pay", when="during")
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert (result["status"], len(calls)) == ("suspended", 1)
    rt.close()


def test_a_journal_that_lost_a_call_the_log_kept_is_not_a_first_call():
    calls: List[str] = []
    log = InMemoryEventLog()
    rt = LocalRuntime(log=log)
    rt.deploy(pay_agent(lambda user: calls.append(user) or "ok"))
    spec = rt.doc
    assert rt.engine is not None
    rt.engine.probe = CrashPoint({"call": "pay", "when": "during"})
    with pytest.raises(SimulatedCrash):
        rt.submit(Turn("c1", "t1", "pay"))
    fresh = LocalRuntime(log=log, journal=InMemoryEffectJournal())  # the log survived, the journal did not
    fresh.tools = rt.tools
    from agentic.runtime import _Deployable
    fresh.deploy(_Deployable(spec, rt.bindings))
    result = fresh.submit(Turn("c1", "t1", "pay"))
    assert (result["status"], len(calls)) == ("suspended", 1)
    resolved = fresh.submit(Turn("c1", "t1", signal={"resolution": "applied", "result": "landed"}))
    assert resolved["status"] == "completed" and resolved["reply"] == "[main] pay returned landed"


# -- unknown outcomes ------------------------------------------------------------------

def test_a_crash_during_an_approved_call_is_unknown_again_and_not_a_third_call():
    calls: List[str] = []
    rt = LocalRuntime()
    rt.deploy(pay_agent(lambda user: calls.append(user) or "ok"))
    rt = crash(rt, Turn("c1", "t1", "pay"), call="pay", when="during")
    assert rt.submit(Turn("c1", "t1", "pay"))["status"] == "suspended"
    rt = crash(rt, Turn("c1", "t1", signal={"resolution": "not_applied"}), call="pay", when="during")
    assert len(calls) == 2
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert (result["status"], len(calls)) == ("suspended", 2)
    assert rt.engine is not None and rt.engine.suspended_turns("c1") == ["t1"]


def test_a_second_unknown_at_the_same_index_reaches_the_log():
    attempts: List[str] = []

    def pay(user: str) -> str:
        attempts.append(user)
        if len(attempts) < 3:
            raise OutcomeUnknown("no answer")
        return "landed"

    rt = LocalRuntime()
    rt.deploy(pay_agent(pay))
    assert rt.submit(Turn("c1", "t1", "pay"))["status"] == "suspended"
    again = rt.submit(Turn("c1", "t1", signal={"resolution": "not_applied"}))
    assert again["status"] == "suspended"
    assert [e["type"] for e in again["events"]] == ["turn_resumed", "unknown_outcome", "turn_suspended"]
    types = [e.type for e in rt.events("c1")]
    assert types.count("turn_suspended") == 2 and types.count("unknown_outcome") == 2
    assert rt.engine is not None and rt.engine.suspended_turns("c1") == ["t1"]
    done = rt.submit(Turn("c1", "t1", signal={"resolution": "not_applied"}))
    assert (done["status"], done["reply"], len(attempts)) == ("completed", "[main] pay returned landed", 3)


def test_a_redelivered_resolution_finds_the_first_answer_standing():
    calls: List[str] = []
    rt = LocalRuntime()
    rt.deploy(pay_agent(lambda user: calls.append(user) or "ok"))
    rt = crash(rt, Turn("c1", "t1", "pay"), call="pay", when="during")
    rt.submit(Turn("c1", "t1", "pay"))
    rt = crash(rt, Turn("c1", "t1", signal={"resolution": "applied", "result": "first"}),
               event="turn_resumed", when="before")
    result = rt.submit(Turn("c1", "t1", signal={"resolution": "not_applied"}))
    assert (result["status"], result["reply"], len(calls)) == ("completed", "[main] pay returned first", 1)


def test_a_bad_resolution_is_refused_even_after_a_good_one_was_journaled():
    rt = LocalRuntime()
    rt.deploy(pay_agent(lambda user: "ok"))
    rt = crash(rt, Turn("c1", "t1", "pay"), call="pay", when="during")
    rt.submit(Turn("c1", "t1", "pay"))
    before = len(rt.events("c1"))
    result = rt.submit(Turn("c1", "t1", signal={"resolution": "maybe"}))
    assert (result["status"], result["error"]["class"]) == ("failed", "validation")
    assert len(rt.events("c1")) == before


# -- runs that end transient -----------------------------------------------------------

def model_pay_agent(fn: Any, effect: str = "non_idempotent") -> Any:
    agent = (Agent(rid("a")).path("main", brain="llm", tools=["pay"]).use_tool("pay", fn, effect=effect)
             .with_llm("stub", script=[{"tool": "pay", "args": {}}, {"text": "paid", "x-fail-attempts": 1}]))
    return agent.verifier("none").build()


def test_a_transient_failure_after_a_non_idempotent_call_does_not_call_it_again():
    calls: List[int] = []
    rt = LocalRuntime()
    rt.deploy(model_pay_agent(lambda: calls.append(1) or "ok"))
    first = rt.submit(Turn("c1", "t1", "pay"))
    assert (first["status"], first["error"]["class"]) == ("failed", "transient")
    done = rt.submit(Turn("c1", "t1", "pay"))
    assert (done["status"], len(calls)) == ("completed", 1)
    assert [c["index"] for c in done["tool_calls"]] == [0]


def test_a_crash_in_a_re_run_stays_on_that_runs_index():
    calls: List[int] = []
    rt = LocalRuntime()
    rt.deploy(model_pay_agent(lambda: calls.append(1) or "ok", effect="idempotent"))
    rt.submit(Turn("c1", "t1", "pay"))
    rt = crash(rt, Turn("c1", "t1", "pay"), call="pay", when="during")
    done = rt.submit(Turn("c1", "t1", "pay"))
    assert done["status"] == "completed"
    assert [c["index"] for c in done["tool_calls"]] == [1]


def test_a_duplicate_after_a_rerun_answers_with_the_run_that_finished():
    calls: List[int] = []
    rt = LocalRuntime()
    rt.deploy(model_pay_agent(lambda: calls.append(1) or "ok"))
    rt.submit(Turn("c1", "t1", "pay"))
    done = rt.submit(Turn("c1", "t1", "pay"))
    again = rt.submit(Turn("c1", "t1", "pay"))
    assert (again["status"], again["error"]) == ("duplicate", None)
    assert again["tool_calls"] == done["tool_calls"]


# -- replay against what the first run saw ---------------------------------------------

def test_a_re_run_sees_the_timers_the_first_run_saw_however_late_it_is():
    clock = ManualClock()
    calls: List[str] = []
    agent = Agent(rid("a")).path("main", tools=["pay"], tool_triggers={"pay": "pay"}).use_tool("pay", lambda user: calls.append(user) or "ok")
    agent.tool("nudge", "constant", value="sent")
    agent.extension("x-note", "a timer that comes due between the crash and the redelivery")
    doc = {**agent.verifier("none").build().document,
           "timers": [{"id": "later", "after_ms": 1000, "tool": "nudge"}]}
    rt = LocalRuntime(clock=clock)
    rt.deploy(type("Spec", (), {"document": doc, "bindings": agent._bindings})())
    rt.submit(Turn("c1", "t0", "hello"))
    rt = crash(rt, Turn("c1", "t1", "pay"), event="tool_called", when="before")
    assert isinstance(rt.clock, ManualClock)
    rt.clock.advance(5000)
    result = rt.submit(Turn("c1", "t1", "pay"))
    assert result["status"] == "completed"
    assert [c["tool"] for c in result["tool_calls"]] == ["pay"]
    assert len(calls) == 1


def test_a_turn_replayed_after_others_arrived_matches_cep_as_it_did():
    agent = Agent(rid("a")).path("main", x_suspend_until="approval").tool("alert", "constant", value="sent")
    doc = {**agent.verifier("none").build().document, "policies": {"unattended_tools": ["alert"]},
           "cep": [{"name": "two", "pattern": [{"where": {"text_contains": "one"}}, {"where": {"text_contains": "two"}}],
                    "on_match": {"kind": "tool", "tool": "alert"}}]}
    rt = LocalRuntime()
    rt.deploy(doc)
    rt.submit(Turn("c1", "t1", "one"))
    held = rt.submit(Turn("c1", "t2", "two"))
    assert [c["tool"] for c in held["tool_calls"]] == ["alert"]
    assert rt.submit(Turn("c1", "t3", "anything"))["status"] == "suspended"
    done = rt.submit(Turn("c1", "t2", signal={"kind": "approval"}))
    assert [(c["tool"], c["index"]) for c in done["tool_calls"]] == [("alert", 0)]


# -- recover_incomplete ----------------------------------------------------------------

def test_recover_incomplete_finishes_partial_turns_and_leaves_the_rest_alone():
    calls: List[str] = []
    agent = Agent(rid("a")).route(rules={"hold": ["hold"]}, default="main")
    agent.path("main", tools=["pay"], tool_triggers={"pay": "pay"}).path("hold", suspend_until="approval")
    agent.use_tool("pay", lambda user: calls.append(user) or "ok")
    rt = LocalRuntime()
    rt.deploy(agent.verifier("none").build())
    done = rt.submit(Turn("c-done", "t1", "pay", user_id="ann"))
    held = rt.submit(Turn("c-held", "t1", "hold"))
    assert (done["status"], held["status"]) == ("completed", "suspended")
    rt = crash(rt, Turn("c-partial", "t1", "pay", "bob", metadata={"source": "web"}), call="pay", when="after")
    rt = crash(rt, Turn("c-resumed", "t1", "hold"), event="turn_suspended", when="after")
    rt = crash(rt, Turn("c-resumed", "t1", signal={"kind": "approval"}), event="turn_resumed", when="after")
    results = {r["conversation_id"]: r for r in rt.recover_incomplete()}
    assert set(results) == {"c-partial", "c-resumed"}
    assert results["c-partial"]["status"] == "completed"
    assert results["c-partial"]["reply"] == "[main] pay returned ok"
    assert results["c-resumed"]["status"] == "completed"
    assert calls == ["ann", "bob"]
    assert next(e for e in rt.events("c-partial") if e.type == "turn_completed").metadata == {"source": "web"}
    assert rt.recover_incomplete() == []


def test_recover_incomplete_leaves_a_turn_that_never_reached_turn_received(tmp_path):
    log = InMemoryEventLog()
    log.append("c1", "t1", "timer_fired", {"timer_id": "x", "due_ms": 0}, 0)
    rt = LocalRuntime(log=log)
    rt.deploy(pay_agent(lambda user: "ok"))
    assert rt.recover_incomplete() == []


def test_recover_incomplete_needs_a_deployed_runtime():
    with pytest.raises(ValidationError):
        LocalRuntime().recover_incomplete()


def test_file_log_passed_in_keeps_its_journal_beside_it(tmp_path):
    log = FileEventLog(str(tmp_path))
    rt = LocalRuntime(log=log)
    rt.deploy(pay_agent(lambda user: "ok"))
    assert isinstance(rt.journal, FileEffectJournal)
    rt.close()
    log.close()
