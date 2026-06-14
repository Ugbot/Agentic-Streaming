"""Parity tests for the wrappers added for this session's new Java features:
the live hot+cold RAG stack, A2A-on-the-graph (resilient spec + steps), and the
Redis/Fluss/in-memory ConversationStore selection. Exercises the real Java
objects through JPype (the session-scoped JVM from conftest).
"""

from __future__ import annotations

import uuid

import jpype

from agentic_flink import a2a, conversation, retrieve


def _vec(values):
    return jpype.JArray(jpype.JFloat)([float(v) for v in values])


def test_inmemory_hot_index_upsert_and_search():
    idx = retrieve.hot_index_inmemory("py-" + uuid.uuid4().hex, max_entries=100)
    target = _vec([1.0, 0.0, 0.0, 0.0])
    idx.upsert("target", target, "the answer", None)
    idx.upsert("other", _vec([0.0, 1.0, 0.0, 0.0]), "off-axis", None)
    assert idx.size() == 2
    hits = idx.search(target, 2)
    assert len(hits) == 2
    assert str(hits[0].getId()) == "target"
    assert hits[0].getScore() > 0.99


def test_two_tier_hot_only_returns_hot_hits():
    idx = retrieve.hot_index_inmemory("py-" + uuid.uuid4().hex)
    q = _vec([1.0, 0.0, 0.0])
    idx.upsert("h1", q, "hot doc", None)
    # cold=None → hot-only; verifies the wrapper builds a valid TwoTierRetriever.
    retriever = retrieve.two_tier_retriever(idx, None, hot_k=5, cold_k=5)
    merged = retriever.retrieve(q, 5)
    ids = [str(s.getId()) for s in merged]
    assert "h1" in ids


def test_two_tier_with_python_cold_callable():
    idx = retrieve.hot_index_inmemory("py-" + uuid.uuid4().hex)
    q = _vec([1.0, 0.0])
    idx.upsert("h1", q, "hot", None)

    ArrayList = jpype.JClass("java.util.ArrayList")

    def cold(query, k):
        # A Python ColdSearch that returns no cold hits (empty Java list).
        return ArrayList()

    retriever = retrieve.two_tier_retriever(idx, cold, hot_k=5, cold_k=5)
    merged = retriever.retrieve(q, 5)
    assert [str(s.getId()) for s in merged] == ["h1"]


def test_remote_agent_spec_carries_resilience():
    spec = a2a.remote_agent(
        "planner",
        endpoint_url="http://localhost:9/a2a",
        max_retries=4,
        request_timeout_ms=12_345,
        circuit_breaker_threshold=7,
    )
    assert str(spec.name()) == "planner"
    assert spec.maxRetries() == 4
    assert spec.requestTimeoutMs() == 12_345
    assert spec.circuitBreakerThreshold() == 7


def test_a2a_step_builds_from_spec():
    spec = a2a.remote_agent("echo", endpoint_url="https://peer/a2a")
    step = a2a.a2a_step(spec, name="echo-step", output_key="a2a.echo", capacity=16)
    assert str(step.name()) == "echo-step"
    assert str(step.outputKey()) == "a2a.echo"
    assert step.capacity() == 16


def test_client_factories_resolve():
    disc = a2a.discovering_client_factory()
    assert disc is not None
    # resilient() wrapping the discovering factory must also resolve.
    assert a2a.resilient_client_factory(disc) is not None


def test_inmemory_conversation_store_attributes():
    store = conversation.in_memory_shared()
    conv = "py-conv-" + uuid.uuid4().hex
    store.clear(conv)
    store.putAttribute(conv, "a2a.cs.contextId", "ctx-123")
    assert str(store.getAttribute(conv, "a2a.cs.contextId").orElse(None)) == "ctx-123"


def test_discover_conversation_store_returns_instance():
    # Defaults to the in-JVM shared store when no provider is selected.
    store = conversation.discover()
    assert store is not None
