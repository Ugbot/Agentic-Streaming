"""Corpus specs — three flavours, all returning live Java ``CorpusSpec`` instances."""

from __future__ import annotations

from typing import Mapping

from ._jvm import jclass


def single_operator(name: str, vector_spec):
    """In-operator corpus. Both reads and writes happen on the same Flink
    operator (typically a ``KeyedCoProcessFunction``)."""
    Single = jclass("org.agentic.flink.corpus.SingleOperatorCorpus")
    java_vec = vector_spec._to_java() if hasattr(vector_spec, "_to_java") else vector_spec
    return Single.spec(name, java_vec)


def broadcast(name: str, vector_spec):
    """Ingest in one operator; per-subtask replicas in any number of read
    operators via Flink broadcast state. User wires the broadcast plumbing;
    each replica is a :func:`single_operator` view underneath."""
    Broadcast = jclass("org.agentic.flink.corpus.BroadcastCorpus")
    java_vec = vector_spec._to_java() if hasattr(vector_spec, "_to_java") else vector_spec
    return Broadcast.spec(name, java_vec)


def external(
    name: str,
    backend: str,
    backend_config: Mapping[str, str],
    dimension: int,
):
    """External vector store (``pgvector``, ``qdrant``, …). Operators are
    stateless; the corpus is shared across jobs."""
    External = jclass("org.agentic.flink.corpus.ExternalCorpus")
    HashMap = jclass("java.util.HashMap")
    cfg = HashMap()
    for k, v in backend_config.items():
        cfg.put(str(k), str(v))
    return External.spec(name, backend, cfg, int(dimension))


__all__ = ["single_operator", "broadcast", "external"]
