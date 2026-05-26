"""``ChatSetup`` and the LangChain4J chat connection round-trip."""

from __future__ import annotations

import random


def test_chat_setup_propagates_every_field(af):
    from agentic_flink import ChatSetup

    seed = random.randint(0, 1_000_000)
    setup = ChatSetup(
        model="qwen2.5:3b",
        temperature=0.42,
        max_response_tokens=2048,
        stop_sequences=("STOP", "END"),
        seed=seed,
    )
    java = setup._to_java()
    assert str(java.getModelName()) == "qwen2.5:3b"
    assert abs(float(java.getTemperature()) - 0.42) < 1e-9
    assert int(java.getMaxResponseTokens()) == 2048
    stops = list(java.getStopSequences())
    assert [str(s) for s in stops] == ["STOP", "END"]
    assert int(java.getSeed()) == seed


def test_langchain4j_ollama_connection_round_trips(af):
    from agentic_flink import langchain4j_ollama

    conn = langchain4j_ollama("http://example.test:9999")
    assert "ollama" in str(conn.providerName())
    assert str(conn.getBaseUrl()) == "http://example.test:9999"


def test_langchain4j_openai_default_base_url_is_unset(af):
    from agentic_flink import langchain4j_openai

    conn = langchain4j_openai("sk-test-12345")
    assert "openai" in str(conn.providerName())
    assert conn.getBaseUrl() is None
