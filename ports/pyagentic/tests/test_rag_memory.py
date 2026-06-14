"""Phase C: vector store (cold tier) + long-term store. In-memory offline; Qdrant +
Postgres live (opt-in, skip when infra absent)."""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.banking import KB, seed_kb  # noqa: E402
from pyagentic.embeddings import HashingEmbedder  # noqa: E402
from pyagentic.longterm import InMemoryLongTermStore, make_long_term_store  # noqa: E402
from pyagentic.retrieval import InMemoryHotVectorIndex, TwoTierRetriever  # noqa: E402
from pyagentic.vectorstores import InMemoryVectorStore, make_vector_store  # noqa: E402

QDRANT_URL = os.environ.get("AGENTIC_TEST_QDRANT_URL", "http://localhost:6333")
PG_URL = os.environ.get("AGENTIC_TEST_PG_URL", "postgresql://agentic:agentic@localhost:5434/agentic")
_EMBED = HashingEmbedder(256)


def _seed(store):
    for doc_id, text in KB.items():
        store.upsert(doc_id, _EMBED.embed(text), text)


def test_inmemory_vector_store_search():
    vs = InMemoryVectorStore()
    _seed(vs)
    hits = vs.search(_EMBED.embed("tell me about crypto cash-back redemption"), 2)
    assert hits[0].id == "kb_cards_crypto"


def test_qdrant_vector_store_as_cold_tier_if_available():
    pytest.importorskip("qdrant_client")
    try:
        vs = make_vector_store({"kind": "qdrant", "url": QDRANT_URL,
                                "collection": "agentic_test_" + uuid.uuid4().hex[:8], "dim": 256}, 256)
        _seed(vs)
    except Exception as exc:
        pytest.skip(f"Qdrant not reachable: {exc}")
    hits = vs.search(_EMBED.embed("how do I dispute a charge"), 2)
    assert hits[0].id == "kb_payments_dispute"
    # wire as the cold tier of a two-tier retriever (hot empty → cold provides the hit)
    retr = TwoTierRetriever(InMemoryHotVectorIndex(), vs.cold_search(), 4, 4)
    merged = retr.retrieve(_EMBED.embed("how do I dispute a charge"), 2)
    assert merged[0].id == "kb_payments_dispute"


def test_inmemory_long_term_store_resume_and_facts():
    s = InMemoryLongTermStore()
    s.save_turn("c1", "alice", "user", "hi")
    s.save_turn("c1", "alice", "assistant", "hello")
    assert s.load_history("c1") == [("user", "hi"), ("assistant", "hello")]
    s.save_fact("alice", "tier", "gold")
    assert s.facts("alice") == {"tier": "gold"}
    assert "c1" in s.conversations_for_user("alice")


def test_postgres_long_term_store_if_available():
    pytest.importorskip("psycopg")
    try:
        store = make_long_term_store({"kind": "postgres", "url": PG_URL})
    except Exception as exc:
        pytest.skip(f"Postgres not reachable: {exc}")
    cid, uid = "c-" + uuid.uuid4().hex[:8], "u-" + uuid.uuid4().hex[:8]
    store.save_turn(cid, uid, "user", "what is my balance?")
    store.save_turn(cid, uid, "assistant", "1234.56")
    assert store.load_history(cid) == [("user", "what is my balance?"), ("assistant", "1234.56")]
    store.save_fact(uid, "tier", "gold")
    assert store.facts(uid)["tier"] == "gold"
    assert cid in store.conversations_for_user(uid)
