"""Memory specs — short-term and vector.

Thin factory functions that return live Java
:class:`ShortTermMemorySpec` / :class:`VectorMemorySpec` instances. Pass
them straight to :meth:`AgentBuilder.with_short_term_memory` /
:meth:`AgentBuilder.with_vector_memory`.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from ._jvm import jclass


def flink_state_short_term(ttl: Optional[timedelta] = None):
    """Default short-term memory: Flink keyed state with optional TTL."""
    SM = jclass("org.agentic.flink.memory.FlinkStateShortTermMemory")
    if ttl is None:
        return SM.spec()
    Duration = jclass("java.time.Duration")
    return SM.spec(Duration.ofMillis(int(ttl.total_seconds() * 1000)))


def flink_state_brute_force(
    dimension: int,
    similarity: str = "cosine",
    max_items: int = 10_000,
):
    """Brute-force KNN over Flink ``MapState``. Sub-millisecond for a few
    thousand vectors at ``d=384``."""
    SM = jclass("org.agentic.flink.memory.vector.FlinkStateVectorMemory")
    Similarity = jclass("org.agentic.flink.memory.vector.VectorMemorySpec$Similarity")
    sim = Similarity.valueOf(_normalize_similarity(similarity))
    return SM.spec(dimension, sim, max_items)


def flink_state_hnsw(
    dimension: int,
    *,
    m: int = 16,
    beam_width: int = 100,
    search_beam: int = 50,
    alpha: float = 1.2,
    similarity: str = "cosine",
):
    """HNSW over Flink state — the navigable-small-world graph variant.

    Vectors live in Flink ``MapState``; the graph is rebuilt on operator
    ``open()`` by replaying state. Suitable for 10⁴–10⁵ vectors per key.
    """
    HnswMem = jclass(
        "org.agentic.flink.memory.vector.FlinkStateHnswVectorMemory"
    )
    HnswCfg = jclass(
        "org.agentic.flink.memory.vector.HnswBuildConfig"
    )
    Similarity = jclass(
        "org.agentic.flink.memory.vector.VectorMemorySpec$Similarity"
    )
    sim = Similarity.valueOf(_normalize_similarity(similarity))
    return HnswMem.spec(dimension, HnswCfg(m, beam_width, search_beam, alpha, sim))


def _normalize_similarity(s: str) -> str:
    """Accept Pythonic ``"cosine"`` / ``"dot"`` / ``"l2"`` and map to the
    Java enum's constant names."""
    aliases = {
        "cosine": "COSINE",
        "dot": "DOT_PRODUCT",
        "dot_product": "DOT_PRODUCT",
        "l2": "NEGATIVE_L2",
        "euclidean": "NEGATIVE_L2",
        "negative_l2": "NEGATIVE_L2",
    }
    upper = (s or "cosine").lower()
    if upper not in aliases:
        raise ValueError(
            f"unknown similarity {s!r}; expected one of {sorted(aliases)}"
        )
    return aliases[upper]


__all__ = ["flink_state_short_term", "flink_state_brute_force", "flink_state_hnsw"]
