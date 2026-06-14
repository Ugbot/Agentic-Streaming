"""Embedder SPI tests — hashing default offline; litellm/Ollama live (opt-in, skips)."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.embeddings import HashingEmbedder, make_embedder  # noqa: E402
from pyagentic.retrieval import cosine  # noqa: E402


def test_hashing_embedder_default_deterministic():
    e = make_embedder({"kind": "hashing", "dim": 256})
    assert isinstance(e, HashingEmbedder) and e.dim == 256
    v1, v2 = e.embed("crypto cash-back"), e.embed("crypto cash-back")
    assert v1 == v2 and len(v1) == 256
    # callable for drop-in use where a plain embed(text) fn was expected
    assert e("crypto cash-back") == v1
    # related text is nearer than unrelated
    near = cosine(e.embed("crypto cash-back redemption"), v1)
    far = cosine(e.embed("the weather is sunny today"), v1)
    assert near > far


def test_make_embedder_unknown_kind():
    with pytest.raises(ValueError):
        make_embedder({"kind": "nope"})


def test_litellm_embedder_against_ollama_if_available():
    """Live check against a local Ollama embedding model (nomic-embed-text). Skips if
    litellm or Ollama isn't available — the hashing default covers offline."""
    pytest.importorskip("litellm")
    import urllib.request

    try:
        urllib.request.urlopen("http://localhost:11434/api/tags", timeout=2).read()
    except Exception as exc:
        pytest.skip(f"Ollama not reachable: {exc}")

    e = make_embedder({"kind": "ollama", "model": "nomic-embed-text"})
    try:
        v = e.embed("hello world")
    except Exception as exc:
        pytest.skip(f"Ollama embed failed (model not pulled?): {exc}")
    assert len(v) > 0 and e.dim == len(v)
    # a real embedder: same text → (near-)identical vectors, cosine ~1
    assert cosine(v, e.embed("hello world")) > 0.99
    assert all(math.isfinite(x) for x in v)
