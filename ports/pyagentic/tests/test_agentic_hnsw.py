"""`retrieval.vector_store: {kind: hnsw}`: an in-process HNSW index behind the knowledge base, with no
external service. Recall against brute force on random vectors, and the same answers on a small set."""

from __future__ import annotations

import random
from typing import Any, Dict, List

import pytest

from agentic import Turn
from agentic.errors import ValidationError
from agentic.hnsw import HnswIndex
from agentic.retrieval import KnowledgeBase, cosine
from agentic.runtime import LocalRuntime


def vector(rng: random.Random, dim: int) -> List[float]:
    return [rng.gauss(0.0, 1.0) for _ in range(dim)]


def test_recall_matches_brute_force():
    rng = random.Random(7)
    dim, n, k = 48, 400, 10
    vectors = {f"d{i}": vector(rng, dim) for i in range(n)}
    index = HnswIndex(m=16, ef_construction=200, ef_search=64, seed=42)
    for doc_id, v in vectors.items():
        index.add(doc_id, v)
    assert len(index) == n
    hits = 0
    for _ in range(30):
        query = vector(rng, dim)
        truth = {i for _, i in sorted(((cosine(query, v), i) for i, v in vectors.items()), reverse=True)[:k]}
        hits += len(truth & {doc_id for doc_id, _ in index.search(query, k)})
    assert hits / (30 * k) >= 0.85


def test_a_planted_vector_is_found_first():
    rng = random.Random(11)
    index = HnswIndex(m=8, seed=1)
    planted = vector(rng, 64)
    index.add("target", planted)
    for i in range(200):
        index.add(f"noise{i}", vector(rng, 64))
    (top, score), = index.search([x + rng.gauss(0.0, 0.01) for x in planted], 1)
    assert top == "target" and score > 0.99


KB = [{"id": "kb_cards", "text": "We offer three card types: classic, gold, and platinum."},
      {"id": "kb_limits", "text": "Daily transfer limits are 10,000 by default; raise them in settings."},
      {"id": "kb_dispute", "text": "To dispute a charge, open the transaction and tap Dispute within 60 days."}]


def test_the_index_answers_as_the_exact_ranking_does_on_a_small_set():
    exact = KnowledgeBase(KB, 256, 3)
    indexed = KnowledgeBase(KB, 256, 3, {"kind": "hnsw", "m": 16, "ef_search": 64})
    for query in ("how do I dispute a charge?", "platinum card", "raise my transfer limit"):
        assert indexed.retrieve(query) == exact.retrieve(query)


def document(store: Dict[str, Any]) -> Dict[str, Any]:
    return {"agent": {"id": "a", "router": {"kind": "static", "default": "main"},
                      "paths": {"main": {"brain": "rule"}}},
            "retrieval": {"dim": 256, "vector_store": store, "kb": KB}}


def test_a_turn_retrieves_through_the_index():
    rt = LocalRuntime()
    rt.deploy(document({"kind": "hnsw"}))
    result = rt.submit(Turn("c1", "t1", "how do I dispute a charge?"))
    assert result["status"] == "completed" and "Dispute within 60 days" in result["reply"]
    retrieved = next(e for e in result["events"] if e["type"] == "retrieved")
    assert retrieved["payload"]["ids"][0] == "kb_dispute"


@pytest.mark.parametrize("kind", ["qdrant", "duckdb", "milvus"])
def test_a_store_this_runtime_lacks_is_refused_at_deploy(kind):
    with pytest.raises(ValidationError, match="hnsw"):
        LocalRuntime().deploy(document({"kind": kind}))


@pytest.mark.parametrize("embeddings", [{"provider": "ollama", "model": "nomic-embed-text"},
                                        {"provider": "openai", "model": "text-embedding-3-small"}])
def test_an_embedder_this_runtime_lacks_is_refused_rather_than_replaced(embeddings):
    doc = {**document({"kind": "hnsw"}), "embeddings": embeddings}
    with pytest.raises(ValidationError, match="hashing"):
        LocalRuntime().deploy(doc)


def test_the_hashing_embedder_is_taken():
    LocalRuntime().deploy({**document({"kind": "hnsw"}), "embeddings": {"provider": "hashing", "dim": 256}})
