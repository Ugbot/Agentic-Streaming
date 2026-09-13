"""Capability declaration of the PyFlink runtime (``spec/v1/primitives.md`` §6).

Every ``supported`` entry is proven by a test in ``pyflink/tests``: the conformance fixtures of
``spec/conformance/v1`` run on a local MiniCluster (``test_conformance.py``) or a dedicated
runtime test named in :data:`PROOF`. Anything the Java adapter implements but this package has
no test for is ``not_tested``; nothing here is inferred from another runtime's results.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Mapping

CAPABILITY_IDS = (
    "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails", "verifier",
    "ordering", "idempotency", "retry", "memory", "retrieval", "context_window", "replay",
    "suspend_resume", "timers", "saga", "a2a", "cep", "event_time", "checkpoint_recovery",
    "parallelism", "durable_store",
)

# Capability -> the test that proves it. Fixture ids refer to spec/conformance/v1/fixtures.
PROOF: dict[str, str] = {
    "routing": "conformance: routing-keyword, routing-default",
    "rule_brain": "conformance: routing-keyword, routing-default",
    "tools": "conformance: tool-invocation, tool-failure",
    "structured_tool_args": "conformance: tool-invocation (args: {user: anonymous})",
    "guardrails": "conformance: guardrail-rejection",
    "verifier": "conformance: verification-failure",
    "ordering": "conformance: ordered-concurrent-turns",
    "idempotency": "conformance: duplicate-turn; test_runtime.py::test_duplicate_turn_id_is_idempotent",
    "retry": "conformance: retry-tool",
    "memory": "conformance: memory-read-write",
    "retrieval": "conformance: retrieval",
    "replay": "conformance: replay-after-restart (stop-with-savepoint + restore)",
    "suspend_resume": "conformance: suspend-resume",
    "saga": "conformance: saga-compensation",
    "a2a": "conformance: a2a-delegation",
    "durable_store": "conformance: replay-after-restart, suspend-resume (state survives restart)",
    "parallelism": "test_runtime.py::test_parallel_job_keeps_per_conversation_order (parallelism=2)",
}

CAPABILITIES: dict[str, str] = {cid: ("supported" if cid in PROOF else "not_tested") for cid in CAPABILITY_IDS}

# What the conformance binding exercises: fixtures needing anything outside this set are skipped.
CONFORMANCE_CAPABILITIES: set[str] = {cid for cid, status in CAPABILITIES.items() if status == "supported"}


class CapabilityError(ValueError):
    """The workflow needs capabilities this runtime declares unsupported."""


def check_requirements(required: Iterable[str], declared: Mapping[str, str]) -> None:
    """Raise :class:`CapabilityError` for ``unsupported`` requirements; warn about ``not_tested``."""
    required = sorted(set(required))
    unsupported = [cid for cid in required if declared.get(cid, "unsupported") == "unsupported"]
    if unsupported:
        raise CapabilityError(
            "the workflow needs capabilities the flink runtime does not support: " + ", ".join(unsupported)
        )
    untested = [cid for cid in required if declared.get(cid) == "not_tested"]
    if untested:
        warnings.warn(
            "the workflow uses capabilities the flink runtime implements but has not proven by test: "
            + ", ".join(untested),
            stacklevel=3,
        )
