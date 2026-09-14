"""Runtime ABC, discovery, capability validation, and the local engine's semantics."""

from __future__ import annotations

import random
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List

import pytest

from agentic import (
    Agent,
    CapabilityError,
    Delegation,
    Runtime,
    RuntimeNotAvailableError,
    Turn,
    ValidationError,
    available_runtimes,
    get_runtime,
    register_runtime,
)
from agentic.engine import retry_delay_ms
from agentic.events import EVENT_TYPES, FileEventLog, InMemoryEventLog, reduce_state
from agentic.ir import validate_result
from agentic.runtime import CAPABILITIES, CAPABILITY_VALUES, LocalRuntime, required_capabilities, unregister_runtime


def rid(prefix: str = "id") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def support_agent() -> Agent:
    return (Agent(rid("support"))
            .route(rules={"billing": ["refund", "charge", "balance"], "account": ["password"]}, default="general")
            .path("billing", tools=["lookup_charge"], tool_triggers={"balance": "lookup_charge"})
            .path("account")
            .path("general")
            .tool("lookup_charge", "constant", value=42.5)
            .verifier("prefix"))


# -- discovery -------------------------------------------------------------------

def test_runtime_is_an_abc():
    with pytest.raises(TypeError):
        Runtime()  # type: ignore[abstract]


def test_local_runtime_resolves_by_name_and_entry_point():
    rt = get_runtime("local")
    assert isinstance(rt, LocalRuntime)
    rt.close()
    found = available_runtimes()
    assert "local" in found


def test_unknown_runtime_names_the_extra():
    with pytest.raises(RuntimeNotAvailableError, match="renamed.*'flink-jvm'.*'pyflink'"):
        get_runtime("flink")
    for name, extra in (("flink-jvm", "flink"), ("pyflink", "pyflink"), ("local-jvm", "jvm")):
        if name in available_runtimes():
            continue
        with pytest.raises(RuntimeNotAvailableError, match=rf"pip install 'pyagentic\[{extra}\]'"):
            get_runtime(name)
    with pytest.raises(RuntimeNotAvailableError, match="register_runtime"):
        get_runtime(rid("nope"))


def test_register_runtime_factory_receives_options():
    name = rid("rt")
    seen: Dict[str, Any] = {}

    class Fake(LocalRuntime):
        def __init__(self, **options: Any) -> None:
            seen.update(options)
            super().__init__()

    register_runtime(name, Fake)
    try:
        rt = get_runtime(name, parallelism=8, checkpoint_interval="30s")
        assert isinstance(rt, Fake)
        assert seen == {"parallelism": 8, "checkpoint_interval": "30s"}
        assert available_runtimes()[name] == "register_runtime"
    finally:
        unregister_runtime(name)
    with pytest.raises(RuntimeNotAvailableError):
        get_runtime(name)


def test_factory_returning_non_runtime_is_rejected():
    name = rid("bad")
    register_runtime(name, lambda **_: object())  # type: ignore[arg-type, return-value]
    try:
        with pytest.raises(RuntimeNotAvailableError, match="not a Runtime"):
            get_runtime(name)
    finally:
        unregister_runtime(name)


def test_local_runtime_rejects_unknown_options():
    with pytest.raises(ValidationError, match="parallelism"):
        LocalRuntime(parallelism=8)


# -- capabilities ----------------------------------------------------------------

def test_capabilities_cover_every_v1_id_with_legal_values():
    caps = LocalRuntime().capabilities()
    assert set(caps) == set(CAPABILITIES)
    assert set(caps.values()) <= CAPABILITY_VALUES


def test_deploy_lists_every_unsupported_requirement():
    spec = support_agent().build()
    doc = dict(spec.document)
    doc["timers"] = [{"id": "t", "after_ms": 1000, "tool": "lookup_charge"}]
    doc["cep"] = [{"name": "pair", "pattern": [
        {"stage": "a", "where": {"text_contains": "a"}},
        {"stage": "b", "where": {"text_contains": "b"}, "contiguity": "followedBy"}]}]
    needs = required_capabilities(doc)
    assert {"timers", "cep"} <= set(needs)
    with pytest.raises(CapabilityError) as info:
        LocalRuntime().deploy(doc)
    assert info.value.runtime == "local"
    assert any(r.startswith("timers") for r in info.value.requirements)
    assert not any(r.startswith("cep") for r in info.value.requirements)
    assert "unsupported requirements" in str(info.value)


def test_deploy_rejects_missing_python_bindings():
    doc = support_agent().tool("py", "function", description="python").document()
    with pytest.raises(ValidationError, match="no bound Python function"):
        LocalRuntime().deploy(doc)


def test_submit_before_deploy_is_a_clear_error():
    with pytest.raises(ValidationError, match="deploy"):
        LocalRuntime().submit(Turn("c", "t", "hi"))


# -- event log -------------------------------------------------------------------

def test_event_types_are_closed_and_sequences_dense():
    log = InMemoryEventLog()
    cid = rid("c")
    for i in range(5):
        event = log.append(cid, "t1", "turn_received", {"i": i}, i)
        assert event.sequence == i
    with pytest.raises(ValidationError, match="closed set"):
        log.append(cid, "t1", "made_up_event", {}, 0)
    assert [e.sequence for e in log.read(cid)] == [0, 1, 2, 3, 4]
    assert len(EVENT_TYPES) == 20


def test_state_is_a_fold_over_the_log():
    log = InMemoryEventLog()
    cid = rid("c")
    log.append(cid, "t1", "turn_received", {}, 0)
    messages = [{"role": "user", "text": "a"}, {"role": "assistant", "text": "b"}]
    log.append(cid, "t1", "memory_written", {"messages": messages}, 0)
    log.append(cid, "t2", "turn_received", {}, 0)
    log.append(cid, "t2", "retrieved", {"ids": ["p1"]}, 0)
    assert reduce_state(log.read(cid)) == {"turn_count": 2, "transcript_length": 2, "last_retrieved_ids": ["p1"]}
    assert reduce_state(list(log.read(cid))[:2]) == {"turn_count": 1, "transcript_length": 2}


def test_file_event_log_survives_a_new_instance_and_preserves_unknown_types(tmp_path: Path):
    cid = rid("conv/with slash")
    log = FileEventLog(str(tmp_path))
    log.append(cid, "t1", "turn_received", {"text": "hi"}, 7, {"k": "v"})
    log.append(cid, "t1", "turn_completed", {"reply": "ok"}, 8)
    path = next(tmp_path.glob("*.jsonl"))
    with open(path, "a", encoding="utf-8") as fh:
        fh.write('{"turn_id": "t9", "sequence": 2, "type": "future_event", "payload": {}}\n')
    reopened = FileEventLog(str(tmp_path))
    events = reopened.read(cid)
    assert [e.type for e in events] == ["turn_received", "turn_completed", "future_event"]
    assert events[0].metadata == {"k": "v"} and events[0].timestamp == 7
    assert reopened.append(cid, "t2", "turn_received", {}, 9).sequence == 3
    assert reopened.conversation_ids() == (cid,)


# -- ordering / idempotency ------------------------------------------------------

def test_turns_on_one_conversation_are_serialized_in_arrival_order():
    calls: List[str] = []
    gate = threading.Event()

    def slow(user: str) -> str:
        if not calls:
            gate.wait(2)
        calls.append(user)
        return user

    spec = (Agent(rid("a")).path("main", tool_triggers={"go": "slow"}).use_tool("slow", slow).verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")
    results: Dict[int, Dict[str, Any]] = {}
    started = threading.Barrier(2)

    def worker(i: int) -> None:
        if i == 1:
            started.wait()
        else:
            started.wait()
            threading.Event().wait(0.05)
        results[i] = rt.submit(Turn(cid, f"t{i}", "go", user_id=f"u{i}"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in (1, 2)]
    for t in threads:
        t.start()
    threading.Event().wait(0.2)
    gate.set()
    for t in threads:
        t.join(5)
    assert calls == ["u1", "u2"]
    sequences = [e["sequence"] for r in (results[1], results[2]) for e in r["events"]]
    assert sequences == sorted(sequences) == list(range(len(sequences)))
    assert results[2]["state"]["turn_count"] == 2


def test_different_conversations_do_not_block_each_other():
    release = threading.Event()

    def blocker(user: str) -> str:
        if user == "slow":
            release.wait(2)
        return user

    spec = Agent(rid("a")).path("main", tool_triggers={"go": "b"}).use_tool("b", blocker).verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    done = threading.Event()
    threading.Thread(target=lambda: rt.submit(Turn("slow-conv", "t", "go", user_id="slow")), daemon=True).start()
    threading.Event().wait(0.05)
    threading.Thread(target=lambda: (rt.submit(Turn("fast-conv", "t", "go", user_id="fast")), done.set()),
                     daemon=True).start()
    assert done.wait(1), "an unrelated conversation waited on a blocked one"
    release.set()


def test_duplicate_turn_returns_recorded_result_and_runs_nothing():
    executions: List[str] = []

    def counted(user: str) -> str:
        executions.append(user)
        return "done"

    spec = Agent(rid("a")).path("main", tool_triggers={"go": "c"}).use_tool("c", counted).verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    cid, tid = rid("c"), rid("t")
    first = rt.submit(Turn(cid, tid, "go"))
    again = rt.submit(Turn(cid, tid, "go"))
    assert first["status"] == "completed" and again["status"] == "duplicate"
    assert again["reply"] == first["reply"] and again["tool_calls"] == first["tool_calls"]
    assert again["events"] == []
    assert executions == ["anonymous"]
    assert len(rt.events(cid)) == len(first["events"])
    validate_result(again)


def test_idempotency_none_reprocesses():
    spec = Agent(rid("a")).path("main").policies(idempotency="none").verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")
    rt.submit(Turn(cid, "t", "hi"))
    assert rt.submit(Turn(cid, "t", "hi"))["status"] == "completed"
    assert rt.state(cid)["turn_count"] == 2


# -- replay / durability ---------------------------------------------------------

def test_restart_rebuilds_from_the_log_without_re_executing():
    executions: List[str] = []

    def counted(user: str) -> str:
        executions.append(user)
        return "x"

    spec = Agent(rid("a")).path("main", tool_triggers={"go": "c"}).use_tool("c", counted).verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")
    before = rt.submit(Turn(cid, "t1", "go"))
    rt2 = rt.restart()
    with pytest.raises(ValidationError, match="closed"):
        rt.submit(Turn(cid, "t2", "hi"))
    dup = rt2.submit(Turn(cid, "t1", "go"))
    assert dup["status"] == "duplicate" and dup["reply"] == before["reply"]
    assert dup["tool_calls"] == before["tool_calls"]
    after = rt2.submit(Turn(cid, "t2", "hi"))
    assert after["state"]["turn_count"] == 2
    assert [e["type"] for e in after["events"]][0] == "turn_received"
    assert executions == ["anonymous"]


def test_file_store_survives_a_fresh_runtime_instance(tmp_path: Path):
    spec = support_agent().with_memory("file", url=str(tmp_path)).build()
    assert "durable_store" in spec.requirements
    cid = rid("c")
    rt = LocalRuntime()
    rt.deploy(spec)
    first = rt.submit(Turn(cid, "t1", "what is my balance?"))
    rt.close()
    fresh = LocalRuntime()
    fresh.deploy(spec)
    assert fresh.submit(Turn(cid, "t1", "what is my balance?"))["status"] == "duplicate"
    second = fresh.submit(Turn(cid, "t2", "I lost my password"))
    assert second["state"]["turn_count"] == 2 and second["path"] == "account"
    assert first["tool_calls"][0]["tool"] == "lookup_charge" and second["tool_calls"] == []


def test_unavailable_store_fails_unless_degrade_is_configured():
    failing = support_agent().with_memory("postgres", url="jdbc:nowhere").build()
    with pytest.raises(ValidationError, match="on_unavailable: degrade"):
        LocalRuntime().deploy(failing)
    degraded = support_agent().with_memory("postgres", url="jdbc:nowhere", on_unavailable="degrade").build()
    rt = LocalRuntime()
    rt.deploy(degraded)
    result = rt.submit(Turn(rid("c"), "t", "hello"))
    assert result["status"] == "completed"
    assert result["runtime_detail"]["degraded"]


# -- retry / verification / guardrails ------------------------------------------

def test_retry_attempts_and_delays_follow_the_policy():
    slept: List[float] = []
    spec = (Agent(rid("a")).path("main", tool_triggers={"go": "flaky"})
            .tool("flaky", "failing", value="ok", **{"x-fail-attempts": 2})
            .policies(retry={"kind": "exponential", "max_attempts": 4, "initial_delay_ms": 100, "multiplier": 2.0,
                             "max_delay_ms": 150})
            .verifier("none").build())
    rt = LocalRuntime(sleep=slept.append)
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "go"))
    assert result["status"] == "completed"
    assert [(c["attempt"], "error" in c) for c in result["tool_calls"]] == [(1, True), (2, True), (3, False)]
    tool_events = [e["type"] for e in result["events"] if e["type"].startswith("tool")]
    assert tool_events == ["tool_failed", "tool_failed", "tool_called"]
    assert [e["type"] for e in result["events"]].count("turn_received") == 1
    assert slept == [0.1, 0.15]


def test_retry_delay_math():
    fixed = {"kind": "fixed", "initial_delay_ms": 40}
    assert [retry_delay_ms(fixed, a) for a in (1, 2, 3)] == [40, 40, 40]
    exp = {"kind": "exponential", "initial_delay_ms": 10, "multiplier": 3, "max_delay_ms": 50}
    assert [retry_delay_ms(exp, a) for a in (1, 2, 3)] == [10, 30, 50]
    jittered = retry_delay_ms({**exp, "jitter": True}, 3, random.Random(1))
    assert 0 <= jittered <= 50
    assert retry_delay_ms({"kind": "none"}, 1) == 0


def test_retry_exhaustion_fails_with_tool_error_class():
    spec = (Agent(rid("a")).path("main", tool_triggers={"go": "broken"}).tool("broken", "failing")
            .policies(retry="fixed", max_attempts=2).verifier("none").build())
    rt = LocalRuntime(sleep=lambda _: None)
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "go"))
    assert result["status"] == "failed" and result["error"]["class"] == "tool"
    assert [c["attempt"] for c in result["tool_calls"]] == [1, 2]


def test_on_tool_error_continue_keeps_going():
    spec = (Agent(rid("a")).path("main", tool_triggers={"go": "broken"}).tool("broken", "failing")
            .policies(on_tool_error="continue").verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "go"))
    assert result["status"] == "completed" and "None" in result["reply"]


def test_verification_is_bounded_and_on_exhausted_fail_is_honoured():
    spec = (Agent(rid("a")).path("main").verifier("regex", pattern="^NEVER")
            .policies(verification={"max_attempts": 3, "on_exhausted": "fail"}).build())
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "hi"))
    assert result["status"] == "failed" and result["error"]["class"] == "verification"
    assert [e["type"] for e in result["events"]].count("verification_failed") == 3


def test_path_verifier_overrides_agent_verifier_and_absent_falls_back():
    """primitives.md section 5: paths.<name>.verifier, else agent.verifier, else prefix."""
    names = [rid(p) for p in ("audit", "chat", "billing", "account")]
    rejecting, lenient, fallback_ok, fallback_ko = names
    attempts = random.randint(2, 4)
    agent = Agent(rid("a")).route(rules={n: [n] for n in names}, default=rejecting)
    agent.path(rejecting, verifier={"kind": "regex", "pattern": f"^never-{rid()}"})
    agent.path(lenient, verifier="prefix")
    agent.path(fallback_ok)
    agent.path(fallback_ko)
    spec = (agent.verifier("regex", pattern=rf"^\[({rejecting}|{fallback_ok})\]")
            .policies(verification={"max_attempts": attempts, "on_exhausted": "unverified"}).build())
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")

    overridden = rt.submit(Turn(cid, "t1", rejecting))
    assert overridden["status"] == "unverified" and overridden["path"] == rejecting
    assert overridden["error"]["class"] == "verification"
    assert [e["type"] for e in overridden["events"]].count("verification_failed") == attempts

    accepted = rt.submit(Turn(cid, "t2", lenient))
    assert accepted["status"] == "completed" and accepted["reply"].startswith(f"[{lenient}]")

    assert rt.submit(Turn(cid, "t3", fallback_ok))["status"] == "completed"
    fallen = rt.submit(Turn(cid, "t4", fallback_ko))
    assert fallen["status"] == "unverified" and fallen["path"] == fallback_ko
    assert [e["type"] for e in fallen["events"]].count("verification_failed") == attempts


def test_path_verifier_none_disables_verification_and_absent_defaults_to_prefix():
    open_path, plain = rid("open"), rid("plain")
    strict = (Agent(rid("a")).route(rules={open_path: [open_path], plain: [plain]}, default=plain)
              .path(open_path, verifier="none").path(plain)
              .verifier("regex", pattern=f"^never-{rid()}")
              .policies(verification={"max_attempts": 1, "on_exhausted": "unverified"}).build())
    rt = LocalRuntime()
    rt.deploy(strict)
    assert rt.submit(Turn(rid("c"), "t1", open_path))["status"] == "completed"
    assert rt.submit(Turn(rid("c"), "t2", plain))["status"] == "unverified"

    engine = rt.engine
    assert engine._verifier_for(strict["agent"]["paths"][open_path]) == {"kind": "none"}
    assert engine._verifier_for(strict["agent"]["paths"][plain]) == strict["agent"]["verifier"]

    undeclared = Agent(rid("a")).route(rules={plain: [plain]}, default=plain).path(plain).build()
    assert "verifier" not in undeclared["agent"]
    rt2 = LocalRuntime()
    rt2.deploy(undeclared)
    assert rt2.engine._verifier_for(undeclared["agent"]["paths"][plain]) == {"kind": "prefix"}
    assert rt2.submit(Turn(rid("c"), "t1", plain))["status"] == "completed"


def test_python_verifier_and_brain_and_guardrail_bindings():
    seen: List[str] = []

    def brain(ctx) -> str:
        seen.append(ctx.text)
        total = ctx.invoke("add", {"a": 2, "b": 3})
        return f"[main] {total} after {len(ctx.transcript)} messages"

    spec = (Agent(rid("a"))
            .path("main", brain=brain, tools=["add"], guardrails=["polite"], verifier=lambda reply: "5" in reply)
            .use_tool("add", lambda a, b: a + b, parameters={"type": "object", "required": ["a", "b"],
                                                            "properties": {"a": {"type": "integer"},
                                                                           "b": {"type": "integer"}}})
            .guardrail("polite", fn=lambda text: "rude" if "stupid" in text else None)
            .build())
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")
    ok = rt.submit(Turn(cid, "t1", "compute"))
    assert ok["status"] == "completed" and ok["reply"] == "[main] 5 after 0 messages"
    assert ok["tool_calls"] == [{"tool": "add", "index": 0, "args": {"a": 2, "b": 3}, "result": 5, "attempt": 1}]
    second = rt.submit(Turn(cid, "t2", "again"))
    assert second["reply"] == "[main] 5 after 2 messages"
    rejected = rt.submit(Turn(cid, "t3", "you stupid bot"))
    assert rejected["status"] == "rejected" and rejected["error"]["class"] == "guardrail"
    assert seen == ["compute", "again"]


def test_structured_tool_args_are_typed_never_parsed():
    def brain(ctx) -> str:
        return f"[main] {ctx.invoke('add', {'a': '2', 'b': 3})}"

    spec = (Agent(rid("a")).path("main", brain=brain, tools=["add"])
            .use_tool("add", lambda a, b: a + b, parameters={"type": "object",
                                                            "properties": {"a": {"type": "integer"},
                                                                           "b": {"type": "integer"}}})
            .verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "go"))
    assert result["status"] == "failed" and result["error"]["class"] == "validation"
    assert "'2' is not of type 'integer'" in result["error"]["message"]


def test_output_guardrail_rejects_the_draft():
    spec = (Agent(rid("a")).path("main", guardrails=["no-secrets"])
            .guardrail("no-secrets", stage="output", deny=["You said"], reason="leak").verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "hi"))
    assert result["status"] == "rejected" and result["error"]["message"] == "leak"
    assert "reply_drafted" in [e["type"] for e in result["events"]]


# -- saga / a2a / suspend / retrieval / llm stub -------------------------------------

def test_saga_compensates_completed_steps_in_reverse_order():
    order: List[str] = []

    def step(name: str):
        def fn() -> str:
            order.append(name)
            return name
        return fn

    spec = (Agent(rid("a")).path("main")
            .use_tool("s1", step("s1"), compensation="u1").use_tool("u1", step("u1"))
            .use_tool("s2", step("s2")).use_tool("u2", step("u2"))
            .tool("s3", "failing")
            .saga({"name": "one", "tool": "s1"}, {"name": "two", "tool": "s2", "compensate_with": "u2"},
                  {"name": "three", "tool": "s3"})
            .verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "go"))
    assert result["status"] == "failed" and result["error"]["class"] == "tool"
    assert order == ["s1", "s2", "u2", "u1"]
    assert [c["tool"] for c in result["tool_calls"]] == ["s1", "s2", "s3", "u2", "u1"]
    types = [e["type"] for e in result["events"]]
    phases = ("compensation_started", "compensation_step", "compensation_completed")
    started, stepped, done = (types.index(t) for t in phases)
    assert started < stepped < done
    assert types.count("compensation_step") == 2


def test_a2a_peer_receives_the_idempotency_key():
    seen: List[Delegation] = []

    def peer(args: Dict[str, Any], delegation: Delegation) -> str:
        seen.append(delegation)
        return f"handled {args['user']}"

    spec = (Agent(rid("a")).path("main", tool_triggers={"escalate": "specialist"})
            .use_peer("specialist", peer).verifier("none").build())
    rt = LocalRuntime()
    rt.deploy(spec)
    cid, tid = rid("c"), rid("t")
    result = rt.submit(Turn(cid, tid, "please escalate", user_id="u7"))
    assert result["status"] == "completed"
    assert "delegated" in [e["type"] for e in result["events"]]
    assert seen[0].conversation_id == cid and seen[0].turn_id == tid and seen[0].call_index == 0
    assert result["tool_calls"][0]["result"] == "handled u7"


def test_suspend_resume_across_restart():
    spec = Agent(rid("a")).path("main", suspend_until="approval").verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    cid, tid = rid("c"), rid("t")
    paused = rt.submit(Turn(cid, tid, "refund me"))
    assert paused["status"] == "suspended" and paused["reply"] is None
    rt = rt.restart()
    assert rt.engine is not None and rt.engine.suspended_turns(cid) == [tid]
    resumed = rt.submit(Turn(cid, tid, signal={"kind": "approval", "approved": True}))
    assert resumed["status"] == "completed"
    assert [e["type"] for e in resumed["events"]][:1] == ["turn_resumed"]
    assert "refund me" in resumed["reply"]
    assert resumed["state"]["turn_count"] == 1
    assert rt.submit(Turn(cid, tid, "refund me"))["status"] == "duplicate"
    stray = rt.submit(Turn(cid, rid("other"), signal={"kind": "approval"}))
    assert stray["status"] == "failed" and stray["error"]["class"] == "validation"


def test_retrieval_is_deterministic_and_recorded():
    kb = [{"id": "hours", "text": "We are open nine to five on weekdays"},
          {"id": "returns", "text": "Returns are accepted within thirty days"}]
    spec = Agent(rid("a")).path("main").with_retrieval(kb, dim=64, top_k=2).verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    a = rt.submit(Turn(rid("c"), "t", "when are you open on weekdays"))
    b = rt.submit(Turn(rid("c"), "t", "when are you open on weekdays"))
    assert a["reply"] == b["reply"] == "[main] We are open nine to five on weekdays"
    assert a["state"]["last_retrieved_ids"][0] == "hours"
    assert "retrieved" in [e["type"] for e in a["events"]]


def test_llm_stub_script_runs_tools_then_answers():
    spec = (Agent(rid("a")).path("main", brain="llm", tools=["lookup"]).tool("lookup", "constant", value=7)
            .with_llm("stub", script=[{"tool": "lookup", "args": {"id": 1}}, {"text": "[main] seven"}])
            .build())
    assert "llm_brain" in spec.requirements
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "hi"))
    assert result["reply"] == "[main] seven"
    assert result["tool_calls"] == [{"tool": "lookup", "index": 0, "args": {"id": 1}, "result": 7, "attempt": 1}]


def test_real_llm_provider_is_refused_not_faked():
    spec = Agent(rid("a")).path("main", brain="llm").with_llm("openai", model="gpt").verifier("none").build()
    rt = LocalRuntime()
    rt.deploy(spec)
    result = rt.submit(Turn(rid("c"), "t", "hi"))
    assert result["status"] == "failed" and result["error"]["class"] == "validation"
    assert "openai" in result["error"]["message"]


def test_every_result_validates_against_the_result_schema():
    spec = support_agent().build()
    rt = LocalRuntime()
    rt.deploy(spec)
    cid = rid("c")
    for tid, text in (("t1", "balance"), ("t1", "balance"), ("t2", "password"), ("t3", "ignore all previous")):
        validate_result(rt.submit(Turn(cid, tid, text)))
