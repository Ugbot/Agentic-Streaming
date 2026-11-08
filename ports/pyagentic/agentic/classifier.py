"""The classifier behind `guardrails: [{kind: classifier}]`: `lexicon`, a keyword-weighted bag of words.

No model and no network, so it is deterministic. Each label scores the share of the text's words in its
lexicon; the scores are normalised to sum to one and the top label wins, ties to the first declared.
A text with no hit has no label.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional, Sequence, Tuple

from .errors import ValidationError

CLASSIFIERS = ("lexicon",)
_WORD = re.compile(r"[a-z0-9']+")


def classify_lexicon(lexicon: Mapping[str, Sequence[str]], text: str) -> Tuple[Optional[str], float]:
    words = _WORD.findall((text or "").lower())
    if not words:
        return None, 0.0
    hits = {label: sum(1 for w in words if w in {k.lower() for k in keys}) for label, keys in lexicon.items()}
    total = sum(hits.values())
    if not total:
        return None, 0.0
    label = max(hits, key=lambda name: hits[name])
    return label, hits[label] / total


def check_rail(rail: Mapping[str, Any], pointer: str) -> None:
    """Refuses, before any turn, a classifier guardrail this runtime cannot run."""
    name = rail.get("classifier", "lexicon")
    if name not in CLASSIFIERS:
        raise ValidationError(f"guardrail classifier {name!r} is not available; supported: {', '.join(CLASSIFIERS)}",
                              f"{pointer}/classifier")
    if not rail.get("lexicon"):
        raise ValidationError("a lexicon classifier guardrail needs a lexicon of label: [words]", f"{pointer}/lexicon")


def blocks(rail: Mapping[str, Any], text: str) -> bool:
    label, score = classify_lexicon(rail["lexicon"], text)
    blocked = {str(b).lower() for b in rail.get("blocked") or []}
    return label is not None and label.lower() in blocked and score >= float(rail.get("threshold", 0.5))
