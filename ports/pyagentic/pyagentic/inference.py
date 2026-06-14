"""Traditional ML / DL inference SPI (Tier 4) — ``Classifier`` and ``Scorer`` interfaces
plus two working, deterministic-by-default implementations and a ``ClassifierGuardrail``
that screens turns using a classifier instead of a regex.

- ``LexiconClassifier``: keyword-weighted bag-of-words classifier — no model, fully offline.
- ``EmbeddingClassifier``: nearest-centroid classifier over an ``Embedder`` — works offline
  with the hashing embedder and becomes a *real* semantic classifier when given a real
  embedder (Ollama/OpenAI via ``LiteLLMEmbedder``). This is the "real model opt-in" path:
  swap the embedder, keep the SPI.

Mirrors the Flink ``inference/`` SPI (Classifier/Scorer/guardrails) at a portable subset.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Sequence, Tuple, runtime_checkable

from .embeddings import Embedder, HashingEmbedder
from .retrieval import cosine

_WORD = re.compile(r"[a-z0-9']+")


def _tokens(text: str) -> List[str]:
    return _WORD.findall((text or "").lower())


@dataclass(frozen=True)
class Classification:
    """A label + its confidence in [0,1], with the full per-label score map."""

    label: str
    score: float
    scores: Dict[str, float] = field(default_factory=dict)


@runtime_checkable
class Classifier(Protocol):
    def classify(self, text: str) -> Classification: ...


@runtime_checkable
class Scorer(Protocol):
    def score(self, text: str) -> float: ...


class LexiconClassifier:
    """Keyword-weighted classifier: each label has a list of keywords; the score for a
    label is the fraction of the text's tokens that hit that label's lexicon, softened so
    the per-label scores sum to 1. Deterministic, offline, no model."""

    def __init__(self, lexicon: Dict[str, Sequence[str]], default_label: str = "other") -> None:
        if not lexicon:
            raise ValueError("lexicon must have at least one label")
        self._lex = {label: {w.lower() for w in words} for label, words in lexicon.items()}
        self._default = default_label

    def _raw_scores(self, text: str) -> Dict[str, float]:
        toks = _tokens(text)
        if not toks:
            return {label: 0.0 for label in self._lex}
        n = len(toks)
        counts: Dict[str, float] = {}
        for label, words in self._lex.items():
            hits = sum(1 for t in toks if t in words)
            counts[label] = hits / n
        return counts

    def classify(self, text: str) -> Classification:
        raw = self._raw_scores(text)
        total = sum(raw.values())
        if total <= 0.0:
            scores = {label: 0.0 for label in raw}
            return Classification(self._default, 0.0, scores)
        scores = {label: v / total for label, v in raw.items()}
        label = max(scores, key=scores.get)
        return Classification(label, scores[label], scores)


class EmbeddingClassifier:
    """Nearest-centroid classifier over an ``Embedder``. ``fit`` averages the embeddings
    of each label's example texts into a centroid; ``classify`` returns the label whose
    centroid is most cosine-similar, with the similarity mapped to [0,1] as the score and
    a softmax over similarities as the per-label distribution. Real ML: with a real
    embedder this is a genuine semantic classifier; with the hashing embedder it's a
    deterministic offline default."""

    def __init__(self, embedder: Optional[Embedder] = None, temperature: float = 10.0) -> None:
        self._embedder = embedder or HashingEmbedder(256)
        self._temperature = temperature
        self._centroids: Dict[str, List[float]] = {}

    def fit(self, examples: Dict[str, Sequence[str]]) -> "EmbeddingClassifier":
        if not examples:
            raise ValueError("need at least one labeled example set")
        for label, texts in examples.items():
            texts = list(texts)
            if not texts:
                continue
            vecs = self._embedder.embed_batch(texts)
            dim = len(vecs[0])
            centroid = [0.0] * dim
            for v in vecs:
                for i, x in enumerate(v):
                    centroid[i] += x
            norm = float(len(vecs))
            self._centroids[label] = [x / norm for x in centroid]
        if not self._centroids:
            raise ValueError("no non-empty example sets")
        return self

    def classify(self, text: str) -> Classification:
        if not self._centroids:
            raise RuntimeError("classifier not fitted; call fit(...) first")
        vec = self._embedder.embed(text)
        sims: Dict[str, float] = {label: cosine(vec, c) for label, c in self._centroids.items()}
        # softmax over similarities for a proper distribution
        exps = {label: math.exp(self._temperature * s) for label, s in sims.items()}
        denom = sum(exps.values()) or 1.0
        scores = {label: e / denom for label, e in exps.items()}
        label = max(scores, key=scores.get)
        return Classification(label, scores[label], scores)


class ClassifierScorer:
    """Adapts a ``Classifier`` into a ``Scorer`` for one target label (its probability)."""

    def __init__(self, classifier: Classifier, label: str) -> None:
        self._classifier = classifier
        self._label = label

    def score(self, text: str) -> float:
        c = self._classifier.classify(text)
        return c.scores.get(self._label, c.score if c.label == self._label else 0.0)


class ClassifierGuardrail:
    """Guardrail that blocks when a classifier assigns a *blocked* label with confidence
    at or above ``threshold``. A learned/lexicon alternative to ``RegexGuardrail``."""

    def __init__(
        self,
        classifier: Classifier,
        blocked_labels: Sequence[str],
        threshold: float = 0.5,
        reason: str = "blocked by classifier policy",
        check_outputs: bool = False,
    ) -> None:
        self._classifier = classifier
        self._blocked = {label.lower() for label in blocked_labels}
        self._threshold = threshold
        self._reason = reason
        self._check_outputs = check_outputs

    def _hit(self, text: str) -> Optional[str]:
        if not text:
            return None
        c = self._classifier.classify(text)
        if c.label.lower() in self._blocked and c.score >= self._threshold:
            return f"{self._reason} ({c.label}={c.score:.2f})"
        return None

    def check_input(self, text: str) -> Optional[str]:
        return self._hit(text)

    def check_output(self, reply: str) -> Optional[str]:
        return self._hit(reply) if self._check_outputs else None
