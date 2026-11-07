"""The day counters were kept for ever, one per principal, tool and day, so a runtime's memory grew
with every principal that ever called. A day that is over is dropped when the next one starts."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentic.limits import DAY, Limits  # noqa: E402


def test_a_day_that_is_over_is_dropped() -> None:
    limits = Limits({"policies": {"limits": {"tokens_per_day": 1_000_000, "cost_per_day": 1000,
                                             "cost_per_1k_tokens": 0.01, "tool_calls_per_day": {"pay": 5}}}}, env={})
    for i in range(1000):
        limits.charge_model(f"acme/user{i}", 0, 400)
        assert limits.admit_tool(f"acme/user{i}", 0, "pay", False) is None
    assert len(limits.counts) == 3000
    limits.charge_model("acme/late", 2 * DAY, 400)
    assert limits.admit_tool("acme/late", 2 * DAY, "pay", False) is None
    assert len(limits.counts) == 3, "only today's counters are kept"
