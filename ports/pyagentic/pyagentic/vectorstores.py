"""Vector store SPI — the cold tier of the two-tier retriever, behind an interface.
``InMemoryVectorStore`` (brute force) is the default; ``HnswVectorStore`` is a real,
**in-process** approximate-nearest-neighbour index (no external service);
``DuckDBVectorStore`` uses the embeddable DuckDB engine (optional, file-or-memory);
``QdrantVectorStore`` is the external-server reference impl. Every store exposes
``cold_search`` so it plugs straight into ``TwoTierRetriever(hot, cold)``.
"""

from __future__ import annotations

import uuid
from typing import Callable, List, Optional, Protocol

from .hnsw import HnswIndex
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


class HnswVectorStore:
    """In-process HNSW vector store — a real approximate-nearest-neighbour index with no
    external dependency. Drop-in cold tier; recall approaches brute force without scanning
    every vector. Tune via ``m`` / ``ef_construction`` / ``ef_search``."""

    def __init__(self, m: int = 16, ef_construction: int = 200, ef_search: int = 50, seed: int = 42) -> None:
        self._index = HnswIndex(m=m, ef_construction=ef_construction, ef_search=ef_search, seed=seed)

    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None:
        self._index.add(doc_id, embedding, text)

    def search(self, query: List[float], k: int) -> List[Scored]:
        return self._index.search(query, k)

    def cold_search(self) -> Callable[[List[float], int], List[Scored]]:
        return self.search


class DuckDBVectorStore:
    """Embeddable vector store backed by DuckDB (optional dep ``duckdb``). Stores vectors
    in a table and ranks with ``list_cosine_similarity`` — an in-process analytical DB, no
    server. Persists to a file when ``path`` is given, else runs in memory."""

    def __init__(self, dim: int = 256, path: str = ":memory:", table: str = "agentic_vectors") -> None:
        import duckdb  # optional, embeddable

        self._con = duckdb.connect(path)
        self._dim = dim
        self._table = table
        self._con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            f"(doc_id VARCHAR PRIMARY KEY, embedding FLOAT[{dim}], text VARCHAR)")

    def upsert(self, doc_id: str, embedding: List[float], text: str) -> None:
        vec = [float(x) for x in embedding]
        self._con.execute(f"DELETE FROM {self._table} WHERE doc_id = ?", [doc_id])
        self._con.execute(
            f"INSERT INTO {self._table} VALUES (?, ?, ?)", [doc_id, vec, text])

    def search(self, query: List[float], k: int) -> List[Scored]:
        vec = [float(x) for x in query]
        rows = self._con.execute(
            f"SELECT doc_id, text, list_cosine_similarity(embedding, ?) AS score "
            f"FROM {self._table} ORDER BY score DESC LIMIT ?",
            [vec, max(1, k)]).fetchall()
        return [Scored(r[0], float(r[2]), r[1]) for r in rows]

    def cold_search(self) -> Callable[[List[float], int], List[Scored]]:
        return self.search


class QdrantVectorStore:
    """Real vector store backed by a Qdrant server (the external-server reference impl)."""

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
    """Build a VectorStore from a ``{kind, url, collection, dim, path}`` spec (the YAML
    ``retrieval.vector_store`` section). kind = memory | hnsw | duckdb | qdrant.
    None spec => None."""
    if not spec:
        return None
    kind = (spec.get("kind") or "memory").lower()
    if kind == "memory":
        return InMemoryVectorStore()
    if kind == "hnsw":
        return HnswVectorStore(
            m=int(spec.get("m", 16)),
            ef_construction=int(spec.get("ef_construction", 200)),
            ef_search=int(spec.get("ef_search", 50)),
            seed=int(spec.get("seed", 42)))
    if kind == "duckdb":
        return DuckDBVectorStore(
            dim=int(spec.get("dim", dim)),
            path=spec.get("path", ":memory:"),
            table=spec.get("table", "agentic_vectors"))
    if kind == "qdrant":
        return QdrantVectorStore(
            url=spec.get("url", "http://localhost:6333"),
            collection=spec.get("collection", "agentic"),
            dim=int(spec.get("dim", dim)))
    raise ValueError(f"unknown vector store kind {kind!r}; choose memory|hnsw|duckdb|qdrant")
