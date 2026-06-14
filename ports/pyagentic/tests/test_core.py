"""Runnable tests for the pyagentic essence core — no engine, no model, no network."""

from __future__ import annotations

import uuid

from pyagentic import (
    Event,
    InMemoryConversationStore,
    InMemoryHotVectorIndex,
    LocalRuntime,
    TwoTierRetriever,
    hashing_embedder,
)
from pyagentic.banking import build_banking_graph, default_tools, seed_kb, KB


def test_conversation_store_transcript_and_attrs():
    s = InMemoryConversationStore(max_messages=5)
    cid = "c-" + uuid.uuid4().hex
    from pyagentic import ChatMessage

    for i in range(8):
        s.append(cid, ChatMessage.user(f"m{i}"))
    assert s.message_count(cid) == 5  # bounded
    assert s.history(cid)[-1].content == "m7"
    s.put_attribute(cid, "graph.phase", "router")
    assert s.get_attribute(cid, "graph.phase") == "router"
    s.associate_user(cid, "alice")
    assert cid in s.conversations_for_user("alice")
    s.clear(cid)
    assert s.message_count(cid) == 0
    assert cid not in s.conversations_for_user("alice")


def test_hot_index_knn_and_two_tier_merge():
    embed = hashing_embedder(64)
    hot = InMemoryHotVectorIndex(max_entries=100)
    hot.upsert("h1", embed("the cat sat on the mat"), "cat on mat")
    hot.upsert("h2", embed("quantum physics lecture"), "physics")
    top = hot.search(embed("where is the cat and the mat"), 2)
    assert top[0].id == "h1" and top[0].score > 0.3

    cold_docs = {"c1": "the cat is a feline animal", "shared": "shared cold copy"}
    cold_vecs = {i: embed(t) for i, t in cold_docs.items()}

    def cold(q, k):
        from pyagentic import Scored, cosine
        return [Scored(i, cosine(q, v), cold_docs[i]) for i, v in cold_vecs.items()]

    hot.upsert("shared", embed("the cat sat on the mat"), "shared HOT copy")
    r = TwoTierRetriever(hot, cold, 5, 5)
    merged = r.retrieve(embed("cat mat"), 10)
    ids = [s.id for s in merged]
    assert "h1" in ids and "c1" in ids
    assert ids.count("shared") == 1  # de-duped
    shared = next(s for s in merged if s.id == "shared")
    assert shared.text == "shared HOT copy"  # hot wins the dedup


def _runtime():
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)
    return LocalRuntime(build_banking_graph(), tools=default_tools(), retriever=retriever)


def test_routed_graph_routes_and_persists_phase():
    rt = _runtime()
    cid = "c-" + uuid.uuid4().hex
    res = rt.submit(Event(cid, "what card types do you offer?", user_id="bob"))
    assert res.path == "cards"
    assert res.ok
    assert "card" in res.reply.lower()
    # phase + path persisted across the turn (multi-turn continuity substrate)
    assert rt.store.get_attribute(cid, "graph.phase") == "done"
    assert rt.store.get_attribute(cid, "graph.path") == "cards"
    assert rt.store.get_attribute(cid, "graph.path") in ("cards",)


def test_routed_graph_tool_call():
    rt = _runtime()
    res = rt.submit(Event("c-" + uuid.uuid4().hex, "what is my balance?", user_id="carol"))
    assert res.path == "payments"
    assert "1234.56" in res.reply
    assert "get_balance" in res.tool_calls


def test_routed_graph_retrieval_recall():
    rt = _runtime()
    res = rt.submit(Event("c-" + uuid.uuid4().hex, "tell me about crypto cash-back redemption"))
    assert res.path == "cards"
    assert res.reply.endswith(KB["kb_cards_crypto"]) or "crypto" in res.reply.lower()


def test_per_conversation_isolation_and_user_index():
    rt = _runtime()
    a, b = "conv-a", "conv-b"
    rt.submit(Event(a, "card help", user_id="u1"))
    rt.submit(Event(b, "transfer limit", user_id="u1"))
    assert set(rt.store.conversations_for_user("u1")) == {a, b}
    # each conversation kept its own path
    assert rt.store.get_attribute(a, "graph.path") == "cards"
    assert rt.store.get_attribute(b, "graph.path") == "payments"
