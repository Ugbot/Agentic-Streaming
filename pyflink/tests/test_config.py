"""Unit tests for the Flink configuration surface (no JVM needed)."""

from __future__ import annotations

import random

import pytest

from agentic_pyflink import FlinkConfig, duration_ms
from agentic_pyflink.workflow import WorkflowLoadError, as_workflow, required_capabilities


@pytest.mark.parametrize("text,expected", [("500ms", 500), ("30s", 30_000), ("2m", 120_000), ("1h", 3_600_000)])
def test_duration_units(text: str, expected: int) -> None:
    assert duration_ms(text) == expected


def test_duration_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        duration_ms("soon")


def test_local_mode_configuration() -> None:
    parallelism = random.randint(1, 16)
    interval = random.randint(1, 120)
    cfg = FlinkConfig(mode="local", parallelism=parallelism, checkpoint_interval=f"{interval}s")
    conf = cfg.flink_configuration()
    assert conf["parallelism.default"] == str(parallelism)
    assert conf["execution.checkpointing.interval"] == f"{interval * 1000} ms"
    assert conf["state.backend.type"] == "hashmap"
    assert "execution.target" not in conf
    assert "rest.address" not in conf


def test_cluster_mode_needs_rest_address_and_targets_remote() -> None:
    with pytest.raises(ValueError, match="rest_address"):
        FlinkConfig(mode="cluster")
    port = random.randint(1024, 65535)
    cfg = FlinkConfig(mode="cluster", rest_address="jobmanager.example", rest_port=port, state_backend="rocksdb")
    conf = cfg.flink_configuration()
    assert conf["execution.target"] == "remote"
    assert conf["rest.address"] == "jobmanager.example"
    assert conf["rest.port"] == str(port)
    assert conf["state.backend.type"] == "rocksdb"


def test_incremental_checkpoints_need_rocksdb_or_forst() -> None:
    with pytest.raises(ValueError, match="incremental"):
        FlinkConfig(state_backend="hashmap", incremental_checkpoints=True)
    conf = FlinkConfig(state_backend="forst", incremental_checkpoints=True).flink_configuration()
    assert conf["execution.checkpointing.incremental"] == "true"


@pytest.mark.parametrize("bad", [dict(mode="yarn"), dict(parallelism=0), dict(state_backend="memory")])
def test_invalid_values_are_rejected(bad: dict) -> None:
    with pytest.raises(ValueError):
        FlinkConfig(**bad)


def test_unknown_spec_version_is_rejected() -> None:
    with pytest.raises(WorkflowLoadError, match="spec_version"):
        as_workflow({"spec_version": "agentic/v2", "agent": {}})


def test_required_capabilities_follow_the_document() -> None:
    doc = {
        "spec_version": "agentic/v1",
        "agent": {"router": {"kind": "keyword"}, "paths": {"a": {"brain": "rule", "tools": ["x"]}}},
        "policies": {"retry": {"kind": "exponential"}, "idempotency": "turn-id", "ordering": "per-conversation"},
        "tools": [{"name": "x", "compensation": "undo_x"}],
        "guardrails": [{"name": "g"}],
    }
    needs = required_capabilities(doc)
    assert isinstance(needs, list) and len(needs) == len(set(needs))
    assert set(needs) == {
        "routing", "rule_brain", "memory", "tools", "retry", "idempotency", "ordering", "guardrails", "verifier",
    }
    assert set(required_capabilities({"spec_version": "agentic/v1", "agent": {"paths": {}}})) == {
        "routing", "verifier", "memory", "ordering", "idempotency",
    }
