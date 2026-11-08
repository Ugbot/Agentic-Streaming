"""Deterministic retrieval: the FNV-1a hashing embedder and brute-force cosine ranking.

Every runtime uses this embedder for the conformance fixtures, so the same query returns
the same passages in the same order everywhere.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Sequence, Tuple

from .errors import ValidationError

if TYPE_CHECKING:
    from .hnsw import HnswIndex

_TOKEN = re.compile(r"[a-z0-9]+")
_FNV_OFFSET_32 = 0x811C9DC5
_FNV_PRIME_32 = 0x01000193
_MASK_32 = 0xFFFFFFFF


def fnv1a_32(token: str) -> int:
    h = _FNV_OFFSET_32
    for b in token.encode("utf-8"):
        h ^= b
        h = (h * _FNV_PRIME_32) & _MASK_32
    return h


def hashing_embed(text: str, dim: int) -> List[float]:
    vector = [0.0] * dim
    for token in _TOKEN.findall((text or "").lower()):
        vector[fnv1a_32(token) % dim] += 1.0
    norm = math.sqrt(sum(x * x for x in vector))
    return [x / norm for x in vector] if norm else vector


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


@dataclass(frozen=True)
class Passage:
    id: str
    text: str
    score: float


VECTOR_STORES = ("memory", "hnsw")


class KnowledgeBase:
    """The declared `retrieval.kb` indexed with the hashing embedder. Ranked exactly, or through an
    in-process HNSW index under `vector_store: {kind: hnsw, m, ef_search}`."""

    def __init__(self, documents: Sequence[Mapping[str, Any]], dim: int, top_k: int,
                 vector_store: Optional[Mapping[str, Any]] = None) -> None:
        self.dim = dim
        self.top_k = top_k
        self._docs: List[Tuple[str, str, List[float]]] = [
            (str(doc["id"]), str(doc["text"]), hashing_embed(str(doc["text"]), dim)) for doc in documents
        ]
        kind = (vector_store or {}).get("kind", "memory")
        if kind not in VECTOR_STORES:
            raise ValidationError(f"retrieval.vector_store kind {kind!r} is not available in this runtime; "
                                  f"supported: {', '.join(VECTOR_STORES)}", "/retrieval/vector_store/kind")
        self._index: Optional["HnswIndex"] = None
        if kind == "hnsw":
            from .hnsw import HnswIndex  # it ranks with `cosine`, from here

            store = vector_store or {}
            self._index = HnswIndex(m=int(store.get("m", 16)), ef_search=int(store.get("ef_search", 50)))
            for doc_id, _, embedding in self._docs:
                self._index.add(doc_id, embedding)

    def __len__(self) -> int:
        return len(self._docs)

    def retrieve(self, query: str, k: int = 0) -> List[Passage]:
        """Top-k passages by cosine, ties broken by id so the order is total."""
        vector = hashing_embed(query, self.dim)
        if self._index is not None:
            texts = {doc_id: text for doc_id, text, _ in self._docs}
            found = self._index.search(vector, k or self.top_k)
            return sorted((Passage(doc_id, texts[doc_id], score) for doc_id, score in found),
                          key=lambda p: (-p.score, p.id))
        scored = sorted(
            (Passage(doc_id, text, cosine(vector, embedding)) for doc_id, text, embedding in self._docs),
            key=lambda p: (-p.score, p.id),
        )
        return scored[: (k or self.top_k)]
