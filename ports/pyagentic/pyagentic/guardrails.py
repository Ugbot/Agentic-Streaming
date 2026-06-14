"""Guardrails SPI — the portable analogue of Flink's ``Guardrail`` / the banking
``BankingScreening``. A guardrail screens the inbound user text and/or the outbound
reply; returning a non-empty reason blocks the turn (the RoutedGraph short-circuits with
an ``ok=False`` ``[blocked]`` reply). Default off; opt in by passing guardrails to the
graph.
"""

from __future__ import annotations

import re
from typing import List, Optional, Protocol


class Guardrail(Protocol):
    def check_input(self, text: str) -> Optional[str]:
        """Return a block reason for the inbound text, or None to allow."""
        ...

    def check_output(self, reply: str) -> Optional[str]:
        """Return a block reason for the outbound reply, or None to allow."""
        ...


class RegexGuardrail:
    """Blocks when any deny-pattern matches (case-insensitive). Mirrors the injection /
    prohibited-content screen in the banking example."""

    def __init__(self, deny: List[str], reason: str = "blocked by policy", check_outputs: bool = False) -> None:
        self._patterns = [re.compile(p, re.IGNORECASE) for p in deny]
        self._reason = reason
        self._check_outputs = check_outputs

    def _hit(self, text: str) -> Optional[str]:
        if text:
            for p in self._patterns:
                if p.search(text):
                    return self._reason
        return None

    def check_input(self, text: str) -> Optional[str]:
        return self._hit(text)

    def check_output(self, reply: str) -> Optional[str]:
        return self._hit(reply) if self._check_outputs else None
