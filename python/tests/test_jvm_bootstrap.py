"""JVM bootstrap behaviour."""

from __future__ import annotations


def test_jvm_is_running(af):
    assert af.is_started()


def test_start_jvm_is_idempotent(af):
    # Calling start_jvm() again is a no-op; second call must not error.
    af.start_jvm()
    assert af.is_started()


def test_jclass_resolves_framework_class(af):
    Agent = af.jclass("org.agentic.flink.dsl.Agent")
    assert Agent is not None
    # The framework's static builder() factory is the canonical entry point.
    builder = Agent.builder()
    assert builder is not None


def test_jclass_raises_when_class_missing(af):
    import pytest

    with pytest.raises(Exception):
        af.jclass("nope.this.class.does.not.exist")
