"""Retrieve pipeline wrapper, including the live hot+cold (two-tier) retrieval stack."""

from __future__ import annotations

from typing import Any

from ._jvm import jclass


def pipeline_from(queries_stream):
    """Begin building a :class:`RetrievalPipeline` against a Java
    ``DataStream<String>`` of queries. Returns the Java ``StageEmbed`` so
    callers chain the fluent stages."""
    Pipeline = jclass("org.agentic.flink.retrieve.RetrievalPipeline")
    return Pipeline.from_(queries_stream)


# ---------------------------------------------------------------------------
# Live RAG hot tier (parity with the Java HotVectorIndex / TwoTierRetriever).
# ---------------------------------------------------------------------------


def hot_index_inmemory(name: str, max_entries: int = 2000):
    """Process-wide in-JVM hot vector index (the embedded default).

    A capacity-bounded, brute-force cosine window over the most-recent documents;
    shared per JVM by ``name`` so an ingest operator and a query operator see the
    same data. Returns the Java :class:`InMemoryHotVectorIndex`.
    """
    Idx = jclass("org.agentic.flink.retrieve.InMemoryHotVectorIndex")
    return Idx(name, int(max_entries))


def hot_index_redis(
    name: str,
    host: str = "localhost",
    port: int = 6379,
    max_entries: int = 2000,
    ttl_seconds: int = 86_400,
):
    """Redis-backed hot vector index (the preferred distributed hot tier).

    A capped, TTL'd recent window in Redis shared across processes. Returns the
    Java :class:`RedisHotVectorIndex`.
    """
    Idx = jclass("org.agentic.flink.retrieve.RedisHotVectorIndex")
    return Idx(name, host, int(port), int(max_entries), int(ttl_seconds))


def two_tier_retriever(hot, cold, hot_k: int = 8, cold_k: int = 8):
    """Build a :class:`TwoTierRetriever` merging a hot index with a cold search.

    ``hot`` is a Java ``HotVectorIndex`` (may be ``None``); ``cold`` is a
    ``TwoTierRetriever.ColdSearch`` — either a Java object implementing it, or a
    Python callable ``(query, k) -> list[ScoredItem]`` (JPype proxies it). Returns
    the Java :class:`TwoTierRetriever`.
    """
    Retriever = jclass("org.agentic.flink.retrieve.TwoTierRetriever")
    return Retriever(hot, _as_cold_search(cold), int(hot_k), int(cold_k))


def _as_cold_search(cold: Any):
    """Accept a Java ColdSearch or a Python callable and return a Java ColdSearch."""
    if cold is None or _is_java_cold_search(cold):
        return cold
    if callable(cold):
        import jpype

        ColdSearch = jclass("org.agentic.flink.retrieve.TwoTierRetriever$ColdSearch")

        @jpype.JImplements(ColdSearch)
        class _PyColdSearch:
            @jpype.JOverride
            def search(self, query, k):  # noqa: N802 (matches the Java method name)
                return cold(query, k)

        return _PyColdSearch()
    raise TypeError("cold must be a TwoTierRetriever.ColdSearch or a callable(query, k)")


def _is_java_cold_search(obj: Any) -> bool:
    try:
        ColdSearch = jclass("org.agentic.flink.retrieve.TwoTierRetriever$ColdSearch")
        return isinstance(obj, ColdSearch)
    except Exception:
        return False


def search_hot_cold(stage_search, corpus_spec, hot, k: int):
    """Wire a live two-tier search stage into a Java ``RetrievalPipeline.StageSearch``.

    ``stage_search`` is the Java ``StageSearch`` (from ``embed(...)``); returns the
    Java ``StageRerank`` so callers continue the fluent chain.
    """
    return stage_search.searchHotCold(corpus_spec, hot, int(k))


__all__ = [
    "pipeline_from",
    "hot_index_inmemory",
    "hot_index_redis",
    "two_tier_retriever",
    "search_hot_cold",
]
