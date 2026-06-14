"""Agentic-Flink on **Dask** — pure Python, the BATCH DATA PLANE.

See ../../docs/portability/dask.md. Dask is the wrong home for the live keyed agent
loop, but the right one for the heavy offline work the project also needs:
parallel RAG ingestion (chunk -> embed -> build the cold index) and offline eval
sweeps (recall@k, or replaying the routed graph over many transcripts). Those reuse
the pure ``pyagentic`` retrieval/graph logic; Dask only parallelizes the map.

Falls back to a sequential map when Dask isn't installed, so the pipeline logic
runs and is verifiable either way.

Run (`pip install "dask[distributed]"`):  python agentic_dask.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pyagentic"))

from pyagentic.banking import KB, build_banking_graph, default_tools, seed_kb  # noqa: E402
from pyagentic.core import AgentContext, Event  # noqa: E402
from pyagentic.memory import InMemoryConversationStore, InMemoryKeyedStateStore  # noqa: E402
from pyagentic.retrieval import (  # noqa: E402
    InMemoryHotVectorIndex,
    Scored,
    TwoTierRetriever,
    hashing_embedder,
)

try:
    import dask.bag as db
except ImportError:
    db = None

_EMBED = hashing_embedder(64)


def _pmap(fn: Callable, items: List, npartitions: int = 4) -> List:
    """Parallel map via Dask bag when available, else sequential — same result.

    Uses the **threads** scheduler so closures may share an in-memory index
    (the demo's stand-in cold store) without pickling. A true distributed run
    points the cold tier at an external store (pgvector/Qdrant/Fluss) — which is
    picklable/remote — and can then use the process/distributed scheduler."""
    if db is not None:
        return db.from_sequence(items, npartitions=npartitions).map(fn).compute(scheduler="threads")
    return [fn(x) for x in items]


# ---- 1. Parallel RAG ingestion: embed a corpus in parallel, build the cold index ----

def ingest_corpus(docs: Dict[str, str]) -> InMemoryHotVectorIndex:
    pairs = _pmap(lambda kv: (kv[0], _EMBED(kv[1]), kv[1]), list(docs.items()))
    index = InMemoryHotVectorIndex(max_entries=10_000)  # stands in for the cold store
    for doc_id, vec, text in pairs:
        index.upsert(doc_id, vec, text)
    return index


# ---- 2. Offline retrieval-quality eval (recall@k), parallelized over queries ----

@dataclass
class EvalCase:
    query: str
    expected_id: str


def eval_recall(index: InMemoryHotVectorIndex, cases: List[EvalCase], k: int = 1) -> float:
    retr = TwoTierRetriever(index, None, k, k)

    def hit(case: EvalCase) -> int:
        ids = [s.id for s in retr.retrieve(_EMBED(case.query), k)]
        return 1 if case.expected_id in ids else 0

    hits = _pmap(hit, cases)
    return sum(hits) / len(cases) if cases else 0.0


# ---- 3. Replay the routed graph over many transcripts in parallel (eval/backtest) ----

def replay_graph(transcripts: List[Tuple[str, str]]) -> List[dict]:
    """transcripts: list of (conversation_id, text). Each conversation is
    independent -> embarrassingly parallel. Returns the per-turn routing result."""
    graph = build_banking_graph()
    tools = default_tools()
    hot = InMemoryHotVectorIndex()
    seed_kb(hot)
    retriever = TwoTierRetriever(hot, None, 4, 4)

    def run(item: Tuple[str, str]) -> dict:
        cid, text = item
        ctx = AgentContext(cid, "batch", InMemoryConversationStore(), InMemoryKeyedStateStore(), tools, retriever)
        res = graph.handle(Event(cid, text, "batch"), ctx)
        return {"conversation_id": cid, "path": res.path, "ok": res.ok}

    return _pmap(run, transcripts)


def _demo():
    backend = "Dask" if db is not None else "sequential (dask not installed)"
    print(f"RAG batch pipeline backend: {backend}")

    index = ingest_corpus(KB)
    print(f"ingested {index.size()} docs into the cold index")

    cases = [
        EvalCase("what card types are available", "kb_cards_types"),
        EvalCase("how do I redeem crypto cash back", "kb_cards_crypto"),
        EvalCase("how do I dispute a charge", "kb_payments_dispute"),
        EvalCase("what is my daily transfer limit", "kb_payments_limits"),
    ]
    recall = eval_recall(index, cases, k=1)
    print(f"retrieval recall@1 over {len(cases)} queries: {recall:.2f}")

    transcripts = [("c%d" % i, t) for i, t in enumerate(
        ["card types?", "dispute a charge", "what is my balance", "hello there"])]
    results = replay_graph(transcripts)
    paths = {r["conversation_id"]: r["path"] for r in results}
    print(f"routed-graph replay paths: {paths}")
    return recall


if __name__ == "__main__":  # pragma: no cover
    _demo()
