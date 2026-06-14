"""Live hot+cold retrieval — the portable analogue of ``HotVectorIndex`` +
``TwoTierRetriever`` + the deterministic ``HashingEmbedder`` from the Quarkus
``/rag`` proxy.

Dependency-free so the port runs and is tested without a model: the hashing
embedder is deterministic (bag-of-words → L2-normalized), the hot tier is an
exact brute-force cosine window, and the two-tier retriever merges hot + cold,
de-duping by id (hot wins). Swap the embedder for a real model (DJL/OpenAI) and
the cold tier for pgvector/Qdrant without touching the merge logic.
"""

from __future__ import annotations

import math
import re
from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from typing import Callable, Dict, List, Optional, Protocol, Tuple

_TOKEN = re.compile(r"[a-z0-9]+")


def hashing_embedder(dim: int = 64) -> Callable[[str], List[float]]:
    """Deterministic bag-of-words hashing embedder. Documents sharing vocabulary
    land near each other in cosine space — enough to exercise retrieval end-to-end
    without an external model."""

    def embed(text: str) -> List[float]:
        v = [0.0] * dim
        if text:
            for tok in _TOKEN.findall(text.lower()):
                h = hash(tok)
                v[h % dim] += 1.0 if (h >> 63) == 0 else -1.0
        norm = math.sqrt(sum(x * x for x in v))
        return [x / norm for x in v] if norm > 0 else v

    return embed


def cosine(a: List[float], b: List[float]) -> float:
    if len(a) != len(b):
        return -1.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


@dataclass(frozen=True)
class Scored:
    id: str
    score: float
    text: str


class HotVectorIndex(Protocol):
    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None: ...

    def search(self, query: List[float], k: int) -> List[Scored]: ...

    def size(self) -> int: ...


class InMemoryHotVectorIndex:
    """Capacity-bounded (LRU) brute-force cosine window — the recent/hot tier."""

    def __init__(self, max_entries: int = 2000) -> None:
        self._max = max(1, max_entries)
        self._entries: "OrderedDict[str, Tuple[List[float], str]]" = OrderedDict()
        self._lock = RLock()

    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None:
        with self._lock:
            self._entries.pop(doc_id, None)
            self._entries[doc_id] = (embedding, text)
            while len(self._entries) > self._max:
                self._entries.popitem(last=False)

    def search(self, query: List[float], k: int) -> List[Scored]:
        with self._lock:
            scored = [Scored(i, cosine(query, e), t) for i, (e, t) in self._entries.items()]
        scored.sort(key=lambda s: s.score, reverse=True)
        return scored[: max(1, k)]

    def size(self) -> int:
        with self._lock:
            return len(self._entries)


# A cold tier is just a search function: (query_vec, k) -> List[Scored].
ColdSearch = Callable[[List[float], int], List[Scored]]


class TwoTierRetriever:
    """Merge hot + cold, de-dup by id keeping the higher score (hot supersedes a
    stale cold copy), return global top-k. Tolerates either tier failing."""

    def __init__(self, hot: Optional[HotVectorIndex], cold: Optional[ColdSearch], hot_k: int = 8, cold_k: int = 8):
        self.hot = hot
        self.cold = cold
        self.hot_k = max(1, hot_k)
        self.cold_k = max(1, cold_k)

    def retrieve(self, query: List[float], k: int) -> List[Scored]:
        best: Dict[str, Scored] = {}
        if self.hot is not None:
            try:
                for s in self.hot.search(query, self.hot_k):
                    self._merge(best, s)
            except Exception:
                pass
        if self.cold is not None:
            try:
                for s in self.cold(query, self.cold_k):
                    self._merge(best, s)
            except Exception:
                pass
        out = sorted(best.values(), key=lambda s: s.score, reverse=True)
        return out[: max(1, k)]

    @staticmethod
    def _merge(best: Dict[str, Scored], s: Scored) -> None:
        prior = best.get(s.id)
        if prior is None or s.score > prior.score:
            best[s.id] = s
