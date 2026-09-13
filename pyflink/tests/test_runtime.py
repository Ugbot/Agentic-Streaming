"""End-to-end tests: real PyFlink jobs on the local MiniCluster running the Java Flink adapter.

Every test here needs the JVM side built (``mvn -f ports/jagentic-core/pom.xml install -DskipTests``,
``mvn package -DskipTests``, ``mvn -f pyflink/java/pom.xml package``); if it is not, the ``jars``
fixture raises with those commands instead of skipping.
"""

from __future__ import annotations

import json
import random
import warnings

import jsonschema
import pytest

from agentic_pyflink import CapabilityError, FlinkConfig, FlinkRuntime, RuntimeStateError
from agentic_pyflink.capabilities import CAPABILITIES, CAPABILITY_IDS, PROOF
from tests.conftest import REPO

RESULT_SCHEMA = json.loads((REPO / "spec" / "v1" / "result.schema.json").read_text(encoding="utf-8"))


def assert_normalized(result: dict) -> None:
    jsonschema.validate(result, RESULT_SCHEMA)


def test_bounded_run_produces_normalized_result(support_workflow, rand_id) -> None:
    cid, tid = rand_id("c"), rand_id("t")
    with FlinkRuntime(FlinkConfig(mode="local")) as rt:
        [result] = rt.run(support_workflow, [{"conversation_id": cid, "turn_id": tid, "text": "what is my balance?"}])

    assert_normalized(result)
    assert result["conversation_id"] == cid
    assert result["turn_id"] == tid
    assert result["status"] == "completed"
    assert result["path"] == "billing"
    assert result["reply"] == "[billing] lookup_charge returned 42.5"
    assert [c["tool"] for c in result["tool_calls"]] == ["lookup_charge"]
    assert result["tool_calls"][0]["result"] == 42.5
    assert result["state"] == {"turn_count": 1, "transcript_length": 2}
    assert [e["type"] for e in result["events"]] == [
        "turn_received", "routed", "brain_started", "tool_called", "reply_drafted", "memory_written", "turn_completed",
    ]


def test_duplicate_turn_id_is_idempotent(support_workflow, rand_id) -> None:
    cid, tid = rand_id("c"), rand_id("t")
    turn = {"conversation_id": cid, "turn_id": tid, "text": "what is my balance?"}
    with FlinkRuntime(FlinkConfig(mode="local")) as rt:
        first, second = rt.run(support_workflow, [turn, dict(turn)])

    assert_normalized(first)
    assert_normalized(second)
    assert first["status"] == "completed"
    assert second["status"] == "duplicate"
    assert second["turn_id"] == tid
    # the redelivery re-executed nothing: the original result is replayed, no new events are appended
    assert second["tool_calls"] == first["tool_calls"]
    assert second["reply"] == first["reply"]
    assert second["state"] == first["state"] == {"turn_count": 1, "transcript_length": 2}
    assert second["events"] == []
    assert sum(1 for e in first["events"] if e["type"] == "tool_called") == 1


def test_streaming_deploy_submit_restart(support_workflow, rand_id) -> None:
    cid = rand_id("c")
    with FlinkRuntime(FlinkConfig(mode="local", checkpoint_interval="200ms")) as rt:
        rt.deploy(support_workflow)
        r1 = rt.submit({"conversation_id": cid, "turn_id": rand_id("t"), "text": "I lost my password"})
        rt.restart()  # stop-with-savepoint, fresh job restored from it
        r2 = rt.submit({"conversation_id": cid, "turn_id": rand_id("t"), "text": "what did I just ask?"})

    assert_normalized(r1)
    assert_normalized(r2)
    assert r1["path"] == "account"
    assert r2["state"] == {"turn_count": 2, "transcript_length": 4}


def test_parallel_job_keeps_per_conversation_order(support_workflow, rand_id) -> None:
    conversations = [rand_id("c") for _ in range(3)]
    turns = []
    for cid in conversations:
        for _ in range(3):
            turns.append({"conversation_id": cid, "turn_id": rand_id("t"), "text": "I lost my password"})
    random.shuffle(turns)

    with FlinkRuntime(FlinkConfig(mode="local", parallelism=2)) as rt:
        results = rt.run(support_workflow, turns)

    assert len(results) == len(turns)
    for cid in conversations:
        mine = [r for r in results if r["conversation_id"] == cid]
        assert [r["state"]["turn_count"] for r in mine] == [1, 2, 3]
        assert [r["turn_id"] for r in mine] == [t["turn_id"] for t in turns if t["conversation_id"] == cid]


def test_banking_example_document_runs_unchanged(banking_workflow, rand_id) -> None:
    with FlinkRuntime() as rt:
        turn = {"conversation_id": rand_id("c"), "turn_id": rand_id("t"), "text": "what is my balance?"}
        [result] = rt.run(banking_workflow, [turn])
    assert_normalized(result)
    assert result["path"] == "payments"
    assert result["tool_calls"][0]["tool"] == "get_balance"


def test_invalid_document_is_rejected_by_the_shared_validator(jars, rand_id) -> None:
    broken = {"spec_version": "agentic/v1", "backend": "local", "agent": {"id": "x", "paths": {}}}
    with FlinkRuntime() as rt, pytest.raises(Exception) as excinfo:
        rt.run(broken, [{"conversation_id": rand_id("c"), "turn_id": rand_id("t"), "text": "hi"}])
    assert "ValidationException" in str(excinfo.value) or "router" in str(excinfo.value)


def test_capabilities_vocabulary_and_proof() -> None:
    assert set(CAPABILITIES) == set(CAPABILITY_IDS)
    assert set(CAPABILITIES.values()) <= {"supported", "partial", "unsupported", "not_tested"}
    for cid, status in CAPABILITIES.items():
        assert (status == "supported") == (cid in PROOF), cid
    assert CAPABILITIES["llm_brain"] == "not_tested"
    assert CAPABILITIES["checkpoint_recovery"] == "not_tested"


def test_deploy_warns_on_untested_requirements_and_rejects_unsupported(support_workflow, monkeypatch) -> None:
    rt = FlinkRuntime()
    llm_doc = json.loads(json.dumps(support_workflow))
    llm_doc["agent"]["paths"]["billing"]["brain"] = "llm"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        from agentic_pyflink.capabilities import check_requirements
        from agentic_pyflink.workflow import required_capabilities

        check_requirements(required_capabilities(llm_doc), rt.capabilities())
    assert any("llm_brain" in str(w.message) for w in caught)

    monkeypatch.setitem(CAPABILITIES, "tools", "unsupported")
    with pytest.raises(CapabilityError, match="tools"):
        rt.deploy(support_workflow)
    rt.close()


def test_submit_before_deploy_and_bounded_deploy_are_errors(support_workflow) -> None:
    from agentic_pyflink import CollectionSource

    with FlinkRuntime() as rt, pytest.raises(RuntimeStateError, match="deploy"):
        rt.submit({"conversation_id": "c", "turn_id": "t", "text": "x"})
    with FlinkRuntime(source=CollectionSource([{"conversation_id": "c", "turn_id": "t"}])) as rt:
        with pytest.raises(RuntimeStateError, match="unbounded"):
            rt.deploy(support_workflow)
