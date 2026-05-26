"""Vector-memory + corpus spec round-trips."""

from __future__ import annotations

import random


def test_flink_state_hnsw_spec(af):
    from agentic_flink.memory import flink_state_hnsw

    dim = random.randint(64, 1024)
    spec = flink_state_hnsw(dimension=dim, m=24, beam_width=120, search_beam=60)
    assert int(spec.dimension()) == dim
    name = str(spec.providerName())
    assert "Hnsw" in name
    assert "M=24" in name


def test_flink_state_brute_force_spec(af):
    from agentic_flink.memory import flink_state_brute_force

    spec = flink_state_brute_force(dimension=128, similarity="dot")
    assert int(spec.dimension()) == 128
    assert "DOT_PRODUCT" in str(spec.providerName())


def test_unknown_similarity_raises(af):
    import pytest

    from agentic_flink.memory import flink_state_hnsw

    with pytest.raises(ValueError):
        flink_state_hnsw(dimension=128, similarity="manhattan")


def test_short_term_memory_with_ttl(af):
    from datetime import timedelta

    from agentic_flink.memory import flink_state_short_term

    spec = flink_state_short_term(ttl=timedelta(minutes=15))
    name = str(spec.providerName())
    assert "ShortTermMemory" in name
    assert "PT15M" in name


def test_single_operator_corpus_spec(af):
    from agentic_flink.corpus import single_operator
    from agentic_flink.memory import flink_state_hnsw

    spec = single_operator("kb-" + str(random.randint(0, 10_000)), flink_state_hnsw(384))
    assert str(spec.name()).startswith("kb-")
    assert "SingleOperatorCorpus" in str(spec.providerName())


def test_chunker_round_trips(af):
    from agentic_flink.ingest import chunk, recursive_chunker

    chunker = recursive_chunker(max_chars=120, overlap=20)
    text = "Apache Flink is a stream processor. " * 20
    chunks = chunk(chunker, "doc-1", text)
    assert len(chunks) > 1
    for i, c in enumerate(chunks):
        assert c["id"] == f"doc-1::{i}"
        assert c["source_id"] == "doc-1"
        assert c["position"] == i
        assert len(c["text"]) <= 240  # ~2× max budget for slack
