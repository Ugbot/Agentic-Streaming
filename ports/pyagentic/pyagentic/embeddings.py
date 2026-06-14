"""Embedder SPI — turn text into a vector. The model-free ``HashingEmbedder`` is the
default (deterministic, offline); ``LiteLLMEmbedder`` calls a real provider (Ollama,
OpenAI, …) via litellm. The retriever and brains take any ``Embedder``, so swapping a
hashing embedder for a real one is a one-line config change.
"""

from __future__ import annotations

from typing import List, Optional, Protocol, runtime_checkable

from .retrieval import hashing_embedder


@runtime_checkable
class Embedder(Protocol):
    dim: int

    def embed(self, text: str) -> List[float]: ...

    def embed_batch(self, texts: List[str]) -> List[List[float]]: ...


class HashingEmbedder:
    """Deterministic FNV bag-of-words embedder (the default). Callable for drop-in use
    where a plain ``embed(text)`` function was expected."""

    def __init__(self, dim: int = 256) -> None:
        self.dim = dim
        self._fn = hashing_embedder(dim)

    def embed(self, text: str) -> List[float]:
        return self._fn(text)

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return [self._fn(t) for t in texts]

    def __call__(self, text: str) -> List[float]:
        return self._fn(text)


class LiteLLMEmbedder:
    """Real embeddings via `litellm` — one API across providers. ``model`` is a litellm
    model string, e.g. ``ollama/nomic-embed-text`` or ``text-embedding-3-small``. The
    dimension is learned from the first response unless given."""

    def __init__(self, model: str = "ollama/nomic-embed-text", dim: Optional[int] = None,
                 api_base: Optional[str] = None, **kwargs) -> None:
        import litellm  # opt-in heavy dep

        self._litellm = litellm
        self.model = model
        self._dim = dim
        self._kwargs = dict(kwargs)
        if api_base:
            self._kwargs["api_base"] = api_base

    def _call(self, inputs: List[str]) -> List[List[float]]:
        resp = self._litellm.embedding(model=self.model, input=inputs, **self._kwargs)
        # litellm returns an object with .data = [{"embedding": [...]}, ...]
        data = resp["data"] if isinstance(resp, dict) else resp.data
        vecs = [list(item["embedding"] if isinstance(item, dict) else item.embedding) for item in data]
        if self._dim is None and vecs:
            self._dim = len(vecs[0])
        return vecs

    @property
    def dim(self) -> int:
        if self._dim is None:
            self._dim = len(self.embed("dimension probe"))
        return self._dim

    def embed(self, text: str) -> List[float]:
        return self._call([text])[0]

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return self._call(list(texts))


def make_embedder(spec: Optional[dict]) -> Embedder:
    """Build an Embedder from a ``{kind, dim, model, base_url}`` spec (the YAML
    ``embeddings`` section). ``kind`` is ``hashing`` (default) | ``litellm`` | ``ollama``
    | ``openai``."""
    spec = spec or {}
    kind = (spec.get("kind") or spec.get("provider") or "hashing").lower()
    if kind in ("hashing", "memory"):
        return HashingEmbedder(int(spec.get("dim", 256)))
    if kind in ("litellm", "ollama", "openai"):
        if "model" in spec:
            model = spec["model"]
        elif kind == "openai":
            model = "text-embedding-3-small"
        else:
            model = "ollama/nomic-embed-text"
        if kind == "ollama" and not model.startswith("ollama/"):
            model = "ollama/" + model
        return LiteLLMEmbedder(model=model, dim=spec.get("dim"), api_base=spec.get("base_url"))
    raise ValueError(f"unknown embedder kind {kind!r}; choose hashing|litellm|ollama|openai")
