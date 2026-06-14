"""Embedding wrappers: connection + setup factories."""

from __future__ import annotations

from dataclasses import dataclass

from ._jvm import jclass


@dataclass(frozen=True)
class EmbeddingSetup:
    """Maps onto ``org.agentic.flink.embedding.EmbeddingSetup``."""

    model: str
    dimension: int
    normalize: bool = True

    def _to_java(self):
        ES = jclass("org.agentic.flink.embedding.EmbeddingSetup")
        return ES.of(self.model, self.dimension, self.normalize)


def ollama_embedding(base_url: str = "http://localhost:11434"):
    """Default Ollama embedder."""
    Conn = jclass("org.agentic.flink.embedding.OllamaEmbeddingConnection")
    return Conn(base_url)


def djl_embedding(model_uri: str):
    """DJL-backed sentence-transformer embedder (e.g.
    ``"djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"``)."""
    Conn = jclass("org.agentic.flink.embedding.djl.DjlEmbeddingConnection")
    return Conn.of(model_uri)


def gemini_embedding(api_key: str | None = None):
    """Google Gemini embedder (``gemini-embedding-001``; the hackathon-mandated marked-run model).
    Falls back to the ``GOOGLE_API_KEY`` environment variable when ``api_key`` is omitted."""
    import os

    key = api_key or os.environ.get("GOOGLE_API_KEY")
    if not key:
        raise ValueError("gemini_embedding requires an API key (arg or GOOGLE_API_KEY env var)")
    Conn = jclass("org.agentic.flink.embedding.GeminiEmbeddingConnection")
    return Conn(key)


__all__ = ["EmbeddingSetup", "ollama_embedding", "djl_embedding", "gemini_embedding"]
