"""agentic-dask: the batch data plane (corpus ingestion, retrieval eval, transcript
replay). Dask itself is optional; when it is installed the real ``dask.bag`` runs, else
the sequential fallback, and ``parallel_backend()`` says which.
"""

from __future__ import annotations

import random

import pytest
from adapter_support import BALANCE_TEXTS, CARD_TEXTS, GENERAL_TEXTS, import_adapter, pick, random_conversation_id

pytestmark = pytest.mark.dask


@pytest.fixture
def dk():
    return import_adapter("agentic_dask")


def test_parallel_backend_reports_what_actually_runs(dk):
    try:
        import dask.bag  # noqa: F401

        assert dk.parallel_backend() == "dask"
    except ImportError:
        assert dk.parallel_backend() == "sequential"


def test_pmap_preserves_order_and_handles_empty_input(dk):
    items = [random.randint(-1000, 1000) for _ in range(random.randint(5, 40))]
    assert dk._pmap(lambda x: x * 2, items) == [x * 2 for x in items]
    assert dk._pmap(lambda x: x, []) == []


def test_ingest_and_recall(dk):
    index = dk.ingest_corpus(dk.KB)
    assert index.size() == len(dk.KB)
    cases = [
        dk.EvalCase("what card types are available", "kb_cards_types"),
        dk.EvalCase("how do I dispute a charge", "kb_payments_dispute"),
    ]
    assert dk.eval_recall(index, cases, k=1) >= 0.5
    assert dk.eval_recall(index, [], k=1) == 0.0


def test_replay_routes_every_turn_in_transcript_order(dk):
    turns = []
    for _ in range(random.randint(3, 8)):
        cid = random_conversation_id("r")
        for texts, path in [(CARD_TEXTS, "cards"), (BALANCE_TEXTS, "payments"), (GENERAL_TEXTS, "general")]:
            turns.append((cid, pick(texts), path))
    random.shuffle(turns)

    results = dk.replay_graph([(cid, text) for cid, text, _ in turns])
    assert [(r["conversation_id"], r["path"]) for r in results] == [(cid, path) for cid, _, path in turns]
    assert all(r["ok"] for r in results)


def test_replay_keeps_same_conversation_turns_ordered_on_one_store(dk, monkeypatch):
    """Turns of one conversation run in submit order on a single ConversationStore
    (single writer per conversation), so multi-turn memory holds across the batch."""
    seen = {}
    original = dk.AgentContext

    class RecordingContext(original):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            seen.setdefault(self.conversation_id, []).append(self.store.message_count(self.conversation_id))

    monkeypatch.setattr(dk, "AgentContext", RecordingContext)
    monkeypatch.setattr(dk, "_pmap", lambda fn, items, npartitions=4: [fn(i) for i in items])
    cid_a, cid_b = random_conversation_id("a"), random_conversation_id("b")
    dk.replay_graph(
        [
            (cid_a, pick(CARD_TEXTS)),
            (cid_b, pick(GENERAL_TEXTS)),
            (cid_a, pick(BALANCE_TEXTS)),
            (cid_a, pick(GENERAL_TEXTS)),
        ]
    )
    assert seen[cid_a] == [0, 2, 4]
    assert seen[cid_b] == [0]


def test_dask_import_resolves_to_the_installed_distribution(dk):
    dask = pytest.importorskip("dask", reason="dask not installed; the adapter then runs sequentially")
    assert "agentic_dask" not in dask.__file__
    assert dk.__file__.endswith("agentic_dask/__init__.py")
