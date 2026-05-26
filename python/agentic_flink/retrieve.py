"""Retrieve pipeline wrapper."""

from __future__ import annotations

from ._jvm import jclass


def pipeline_from(queries_stream):
    """Begin building a :class:`RetrievalPipeline` against a Java
    ``DataStream<String>`` of queries. Returns the Java ``StageEmbed`` so
    callers chain the fluent stages."""
    Pipeline = jclass("org.agentic.flink.retrieve.RetrievalPipeline")
    return Pipeline.from_(queries_stream)


__all__ = ["pipeline_from"]
