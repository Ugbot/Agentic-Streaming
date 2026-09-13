"""A Python binding used by test_conformance_matrix.py to exercise the `--python-binding` hook.

It delegates to the reference runtime, declares `saga` unsupported, and deliberately mangles
the `tool-failure` fixture so the test can see the runner's comparator catch a binding that
returns wrong results. A real binding follows the same contract: take the fixture mapping,
return the normalized results in turn order, or `{"skip": reason}`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

from reference_runtime import ReferenceRuntime, Turn

CAPABILITIES = {"routing", "rule_brain", "tools", "structured_tool_args", "guardrails", "verifier",
                "ordering", "idempotency", "retry", "memory", "retrieval", "replay", "suspend_resume",
                "a2a", "durable_store"}


def run(fixture: Dict[str, Any]) -> Union[Dict[str, str], List[Dict[str, Any]]]:
    missing = sorted(set(fixture["requires"]) - CAPABILITIES)
    if missing:
        return {"skip": f"requires {missing}"}
    runtime = ReferenceRuntime(fixture["workflow"])
    results: List[Dict[str, Any]] = []
    for turn in fixture["turns"]:
        if turn.get("restart_runtime"):
            runtime.restart()
        results.append(runtime.submit(Turn(turn["conversation_id"], turn["turn_id"], turn.get("text", ""), turn.get("signal"))))
    if fixture["id"] == "tool-failure":
        results[0]["error"] = None
    return results
