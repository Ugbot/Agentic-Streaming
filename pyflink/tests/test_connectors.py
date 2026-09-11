"""Connector wiring. Kafka is checked up to job-graph construction only (no broker in CI); the
runtime does not claim end-to-end Kafka delivery anywhere."""

from __future__ import annotations

import json
import random

import pytest

from agentic_pyflink import FlinkRuntime, KafkaSink, KafkaSource
from agentic_pyflink.connectors import encode_turn, turn_document


def test_turn_document_is_the_fixture_wire_shape() -> None:
    doc = turn_document({"conversation_id": "c1", "turn_id": "t1", "text": "hi", "metadata": {"k": "v"}})
    assert doc == {
        "conversation_id": "c1", "turn_id": "t1", "user_id": "anonymous", "text": "hi", "metadata": {"k": "v"},
    }
    resume = turn_document({"conversation_id": "c1", "turn_id": "t1", "signal": {"approved": True}})
    assert resume["signal"] == {"approved": True} and "text" not in resume
    assert json.loads(encode_turn(doc)) == doc


@pytest.mark.parametrize("missing", ["conversation_id", "turn_id"])
def test_turn_document_requires_ids(missing: str) -> None:
    turn = {"conversation_id": "c1", "turn_id": "t1", "text": "x"}
    turn.pop(missing)
    with pytest.raises(ValueError, match=missing):
        turn_document(turn)


def test_kafka_connectors_build_a_job_graph_without_a_broker(support_workflow) -> None:
    port = random.randint(1024, 65535)
    source = KafkaSource(bootstrap_servers=f"broker:{port}", topic="turns", group_id="g")
    sink = KafkaSink(bootstrap_servers=f"broker:{port}", topic="results")
    rt = FlinkRuntime(source=source, sink=sink)
    try:
        env = rt._environment()  # puts the framework jars (which bundle the Kafka connector) on the classpath
        source.check_available()
        sink.check_available()
        stream = rt._graph(env, support_workflow, source)
        sink.attach(stream)
        plan = json.loads(env.get_execution_plan())
        names = " ".join(node["type"] for node in plan["nodes"])
        assert "json->event" in names and "result->json" in names
    finally:
        rt.close()
