"""Context-window management — MoSCoW prioritization + compaction so a long transcript
fits a token budget before it's sent to the model. Portable analogue of the Flink
``ContextWindowManager``: keep MUST first, then SHOULD, then COULD; drop WON'T; stop at
the budget.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import List


class Priority(IntEnum):
    WONT = 0
    COULD = 1
    SHOULD = 2
    MUST = 3


@dataclass
class ContextItem:
    text: str
    priority: Priority = Priority.SHOULD

    @property
    def tokens(self) -> int:
        # cheap, deterministic token estimate (~4 chars/token)
        return max(1, (len(self.text) + 3) // 4)


class ContextWindowManager:
    """Compacts a list of context items to a token budget, MoSCoW-first."""

    def __init__(self, max_tokens: int) -> None:
        self.max_tokens = max(1, max_tokens)

    def compact(self, items: List[ContextItem]) -> List[ContextItem]:
        """Drop WON'T, then greedily keep highest-priority items within the budget,
        preserving the original order among the kept items."""
        candidates = [it for it in items if it.priority != Priority.WONT]
        # choose which to keep by descending priority (stable on input order)
        order = sorted(range(len(candidates)), key=lambda i: (-int(candidates[i].priority), i))
        budget = self.max_tokens
        keep = set()
        for i in order:
            t = candidates[i].tokens
            if t <= budget:
                keep.add(i)
                budget -= t
        return [candidates[i] for i in range(len(candidates)) if i in keep]

    def total_tokens(self, items: List[ContextItem]) -> int:
        return sum(it.tokens for it in items)
