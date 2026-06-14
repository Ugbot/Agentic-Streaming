"""Embeddable in-process vector stores — the hand-rolled HNSW index (recall vs brute
force, no external service) and the optional DuckDB store (skip if duckdb absent). Uses
randomized vectors, not hardcoded happy paths."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.hnsw import HnswIndex  # noqa: E402
from pyagentic.retrieval import cosine  # noqa: E402
from pyagentic.vectorstores import (  # noqa: E402
    HnswVectorStore,
    InMemoryVectorStore,
    make_vector_store,
)


def _rand_vec(rng: random.Random, dim: int) -> list:
    return [rng.gauss(0.0, 1.0) for _ in range(dim)]


def _brute_topk(vecs, query, k):
    scored = sorted(((cosine(query, v), i) for i, v in vecs.items()), reverse=True)
    return [i for _, i in scored[:k]]


def test_hnsw_recall_matches_brute_force():
    rng = random.Random(7)
    dim, n, k = 48, 400, 10
    vecs = {f"d{i}": _rand_vec(rng, dim) for i in range(n)}

    index = HnswIndex(m=16, ef_construction=200, ef_search=64, seed=42)
    for doc_id, v in vecs.items():
        index.add(doc_id, v, doc_id)
    assert len(index) == n

    queries = [_rand_vec(rng, dim) for _ in range(30)]
    hits = 0
    total = 0
    for q in queries:
        truth = set(_brute_topk(vecs, q, k))
        got = {s.id for s in index.search(q, k)}
        hits += len(truth & got)
        total += k
    recall = hits / total
    # a correct HNSW recovers the vast majority of true neighbours
    assert recall >= 0.85, f"recall@{k} = {recall:.3f}"


def test_hnsw_top1_is_exact_for_planted_query():
    rng = random.Random(11)
    dim = 64
    store = HnswVectorStore(seed=1)
    planted = _rand_vec(rng, dim)
    store.upsert("target", planted, "the answer")
    for i in range(200):
        store.upsert(f"noise{i}", _rand_vec(rng, dim), f"noise {i}")
    # querying with (a perturbation of) the planted vector returns it first
    perturbed = [x + rng.gauss(0.0, 0.01) for x in planted]
    top = store.search(perturbed, 1)
    assert top and top[0].id == "target"
    assert top[0].score > 0.99


def test_hnsw_vs_inmemory_agree_on_small_set():
    rng = random.Random(3)
    dim = 32
    docs = {f"k{i}": _rand_vec(rng, dim) for i in range(20)}
    mem = InMemoryVectorStore()
    hnsw = HnswVectorStore(seed=5)
    for doc_id, v in docs.items():
        mem.upsert(doc_id, v, doc_id)
        hnsw.upsert(doc_id, v, doc_id)
    q = _rand_vec(rng, dim)
    assert mem.search(q, 1)[0].id == hnsw.search(q, 1)[0].id


def test_make_vector_store_hnsw():
    store = make_vector_store({"kind": "hnsw", "m": 8, "ef_search": 32})
    assert isinstance(store, HnswVectorStore)
    store.upsert("a", [1.0, 0.0, 0.0], "a")
    store.upsert("b", [0.0, 1.0, 0.0], "b")
    assert store.search([0.9, 0.1, 0.0], 1)[0].id == "a"


def test_duckdb_vector_store_if_available():
    pytest.importorskip("duckdb")
    from pyagentic.vectorstores import DuckDBVectorStore

    rng = random.Random(13)
    dim = 32
    store = DuckDBVectorStore(dim=dim)
    docs = {f"d{i}": _rand_vec(rng, dim) for i in range(50)}
    for doc_id, v in docs.items():
        store.upsert(doc_id, v, doc_id)
    q = next(iter(docs.values()))
    top = store.search(q, 3)
    assert top[0].id == "d0"  # a doc is most similar to itself
    assert -1.0 <= top[0].score <= 1.0001
