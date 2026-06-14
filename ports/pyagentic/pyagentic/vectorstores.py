"""Vector store SPI — the cold tier of the two-tier retriever, behind an interface.
``InMemoryVectorStore`` is the default; ``QdrantVectorStore`` is the real reference impl
(works against a Qdrant server). The store exposes ``cold_search`` so it plugs straight
into ``TwoTierRetriever(hot, cold)``.
"""

from __future__ import annotations

import uuid
from typing import Callable, List, Optional, Protocol

from .retrieval import Scored, cosine


class VectorStore(Protocol):
    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None: ...

    def search(self, query: List[float], k: int) -> List[Scored]: ...

    def cold_search(self) -> Callable[[List[float], int], List[Scored]]:
        """Adapt this store to the TwoTierRetriever cold-tier signature."""
        ...


class InMemoryVectorStore:
    """Brute-force in-memory store — the default cold tier for tests/dev."""

    def __init__(self) -> None:
        self._docs: dict = {}

    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None:
        self._docs[doc_id] = (embedding, text)

    def search(self, query: List[float], k: int) -> List[Scored]:
        scored = [Scored(i, cosine(query, e), t) for i, (e, t) in self._docs.items()]
        scored.sort(key=lambda s: s.score, reverse=True)
        return scored[: max(1, k)]

    def cold_search(self) -> Callable[[List[float], int], List[Scored]]:
        return self.search


class QdrantVectorStore:
    """Real vector store backed by a Qdrant server (the reference cold-tier impl)."""

    def __init__(self, url: str = "http://localhost:6333", collection: str = "agentic", dim: int = 256):
        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, VectorParams

        self._models = __import__("qdrant_client.models", fromlist=["PointStruct"])
        self._client = QdrantClient(url=url)
        self.collection = collection
        if not self._client.collection_exists(collection):
            self._client.create_collection(
                collection_name=collection,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE))

    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None:
        point = self._models.PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)),
            vector=list(embedding),
            payload={"doc_id": doc_id, "text": text})
        self._client.upsert(collection_name=self.collection, points=[point])

    def search(self, query: List[float], k: int) -> List[Scored]:
        hits = self._client.query_points(
            collection_name=self.collection, query=list(query), limit=max(1, k), with_payload=True).points
        return [Scored(h.payload.get("doc_id", str(h.id)), float(h.score), h.payload.get("text", "")) for h in hits]

    def cold_search(self) -> Callable[[List[float], int], List[Scored]]:
        return self.search


def make_vector_store(spec: Optional[dict], dim: int = 256) -> Optional[VectorStore]:
    """Build a VectorStore from a ``{kind, url, collection, dim}`` spec (the YAML
    ``retrieval.vector_store`` section). kind = memory | qdrant. None spec => None."""
    if not spec:
        return None
    kind = (spec.get("kind") or "memory").lower()
    if kind == "memory":
        return InMemoryVectorStore()
    if kind == "qdrant":
        return QdrantVectorStore(
            url=spec.get("url", "http://localhost:6333"),
            collection=spec.get("collection", "agentic"),
            dim=int(spec.get("dim", dim)))
    raise ValueError(f"unknown vector store kind {kind!r}; choose memory|qdrant")
