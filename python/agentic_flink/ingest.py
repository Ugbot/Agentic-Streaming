"""Ingest pipeline + chunker wrappers."""

from __future__ import annotations

from ._jvm import jclass


def recursive_chunker(max_chars: int, overlap: int | None = None):
    """LangChain-style recursive text splitter."""
    Chunker = jclass("org.agentic.flink.ingest.RecursiveTextChunker")
    if overlap is None:
        return Chunker(int(max_chars))
    return Chunker(int(max_chars), int(overlap))


def chunk(chunker, source_id: str, text: str) -> list[dict]:
    """Run a :class:`RecursiveTextChunker` (or any ``Chunker``) over a single
    document and return the resulting chunks as a list of Python dicts.
    Useful for tests and stand-alone use without a Flink job."""
    chunks = chunker.chunk(source_id, text)
    out = []
    for c in chunks:
        out.append(
            {
                "id": str(c.getId()),
                "text": str(c.getText()),
                "source_id": str(c.getSourceId()),
                "position": int(c.getPosition()),
                "token_count": int(c.getTokenCount()),
            }
        )
    return out


def pipeline_from(pages_stream):
    """Begin building an :class:`IngestionPipeline` against a Java
    ``DataStream<CrawledPage>``. Returns the Java ``StageChunk`` so callers
    chain the fluent stages directly."""
    Pipeline = jclass("org.agentic.flink.ingest.IngestionPipeline")
    return Pipeline.from_(pages_stream)


__all__ = ["recursive_chunker", "chunk", "pipeline_from"]
