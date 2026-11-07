"""`policies.limits`: per-principal rates, budgets and quotas, and the kill switch.

The Python peer of `org.jagentic.core.Limits`. The counters live in this object, so they are per
process and gone after a restart. A limit here slows a principal down; it is not a billing record.
Tokens are an estimate, four characters to the token, because the clients here read no provider usage.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional, Set, Tuple

MINUTE = 60_000
DAY = 86_400_000

Violation = Tuple[str, Dict[str, Any]]


def who(turn: Any) -> str:
    """Who the counters are kept for."""
    tenant = (turn.principal or {}).get("tenant")
    return f"{tenant}/{turn.user_id}" if tenant else turn.user_id


class Limits:
    def __init__(self, doc: Mapping[str, Any], env: Optional[Mapping[str, str]] = None) -> None:
        self.settings: Mapping[str, Any] = (doc.get("policies") or {}).get("limits") or {}
        self.env = os.environ if env is None else env
        self.counts: Dict[Tuple[Any, ...], float] = {}
        self.turns: Dict[Tuple[str, int], Set[str]] = {}

    def stopped(self) -> bool:
        """The document's switch, `AGENTIC_KILL_SWITCH=true`, or the `AGENTIC_KILL_SWITCH_FILE` existing."""
        if self.settings.get("kill_switch") is True or self.env.get("AGENTIC_KILL_SWITCH", "").lower() == "true":
            return True
        file = self.env.get("AGENTIC_KILL_SWITCH_FILE")
        return bool(file) and os.path.exists(file)

    def _take(self, slot: Tuple[Any, ...], limit: float) -> bool:
        used = self.counts.get(slot, 0.0)
        if used + 1 > limit:
            return False
        self.counts[slot] = used + 1
        return True

    def admit_turn(self, principal: str, turn_id: str, now_ms: int) -> Optional[Violation]:
        """A turn counts against the rate once, however many times a crash redelivers it."""
        if self.stopped():
            return "kill_switch", {}
        rate = self.settings.get("turns_per_minute")
        if rate is None:
            return None
        minute = now_ms // MINUTE
        for slot in [s for s in self.turns if s[1] < minute]:
            del self.turns[slot]
        seen = self.turns.setdefault((principal, minute), set())
        if turn_id in seen:
            return None
        if len(seen) >= rate:
            return "turns_per_minute", {"max": rate}
        seen.add(turn_id)
        return None

    def admit_model(self, principal: str, now_ms: int) -> Optional[Violation]:
        """A budget is spent by the call that crosses it, not refused by it."""
        if self.stopped():
            return "kill_switch", {}
        for limit, counter in (("tokens_per_day", "tokens"), ("cost_per_day", "cost")):
            budget = self.settings.get(limit)
            if budget is not None and self.counts.get((counter, principal, now_ms // DAY), 0.0) >= budget:
                return limit, {"max": budget}
        return None

    def charge_model(self, principal: str, now_ms: int, characters: int) -> None:
        tokens = max(1, characters // 4)
        slot = ("tokens", principal, now_ms // DAY)
        self.counts[slot] = self.counts.get(slot, 0.0) + tokens
        rate = self.settings.get("cost_per_1k_tokens")
        if rate is not None:
            slot = ("cost", principal, now_ms // DAY)
            self.counts[slot] = self.counts.get(slot, 0.0) + tokens / 1000.0 * rate

    def has_quota(self, tool: str) -> bool:
        return tool in (self.settings.get("tool_calls_per_day") or {})

    def admit_tool(self, principal: str, now_ms: int, tool: str, begun: bool) -> Optional[Violation]:
        if self.stopped():
            return "kill_switch", {"tool": tool}
        quota = (self.settings.get("tool_calls_per_day") or {}).get(tool)
        if not begun and quota is not None and not self._take(("tool", principal, tool, now_ms // DAY), quota):
            return "tool_calls_per_day", {"tool": tool, "max": quota}
        return None
