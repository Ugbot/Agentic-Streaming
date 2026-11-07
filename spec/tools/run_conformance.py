#!/usr/bin/env python3
"""Run the v1 conformance fixtures against the reference runtime.

    python spec/tools/run_conformance.py            # all fixtures
    python spec/tools/run_conformance.py retry-tool # one fixture by id

This is also the comparator every other runtime binding should reuse: a binding produces
normalized results (spec/v1/result.schema.json) and calls `check_expectation` on each one.
"""

from __future__ import annotations

import copy
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from http_stub import HttpStub  # noqa: E402
from reference_runtime import ReferenceRuntime, SpecError, Turn  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[1] / "conformance" / "v1" / "fixtures"

# What the reference runtime is for. Fixtures needing anything else are reported as
# skipped, never as passed. Three ids are claimed in the narrow sense the reference can
# honour, which is exactly what the fixtures observe:
# - `durable_store`, `checkpoint_recovery`: the log, pending timers and the logical clock
#   outlive a `restart()` within one process. That proves replay, resume and timer recovery
#   semantics but is not durability across a crash.
# - `parallelism`: conversations are isolated under concurrent delivery. The reference is
#   single-threaded, so it proves the isolation contract, not concurrent execution.
# - `llm_brain`: the deterministic `stub` provider driven by `llm.script`; no model.
REFERENCE_CAPABILITIES = {
    "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails", "verifier",
    "ordering", "idempotency", "retry", "memory", "retrieval", "context_window", "replay",
    "suspend_resume", "timers", "saga", "a2a", "cep", "event_time", "checkpoint_recovery",
    "parallelism", "durable_store", "crash_recovery", "effect_journal", "tool_policy", "principal", "tool_approval", "limits", "redaction",
}


def load(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def check_expectation(expected: Dict[str, Any], actual: Dict[str, Any]) -> List[str]:
    """Compare one expectation against one normalized result. Returns failure messages."""
    problems: List[str] = []

    def mismatch(field: str, want: Any, got: Any) -> None:
        problems.append(f"{field}: expected {want!r}, got {got!r}")

    for field in ("conversation_id", "status", "path", "reply"):
        if field in expected and expected[field] != actual.get(field):
            mismatch(field, expected[field], actual.get(field))

    if "status_any" in expected and actual.get("status") not in expected["status_any"]:
        mismatch("status_any", expected["status_any"], actual.get("status"))

    if "reply_matches" in expected:
        reply = actual.get("reply") or ""
        if not re.search(expected["reply_matches"], reply):
            mismatch("reply_matches", expected["reply_matches"], reply)

    if "error_class" in expected:
        got = (actual.get("error") or {}).get("class")
        if got != expected["error_class"]:
            mismatch("error_class", expected["error_class"], got)

    if "tool_calls" in expected:
        want = expected["tool_calls"]
        got = actual.get("tool_calls", [])
        if len(want) != len(got):
            mismatch("tool_calls length", len(want), len(got))
        else:
            for i, (w, g) in enumerate(zip(want, got)):
                if w["tool"] != g["tool"]:
                    mismatch(f"tool_calls[{i}].tool", w["tool"], g["tool"])
                for key in ("index", "attempt", "args", "result"):
                    if key in w and w[key] != g.get(key):
                        mismatch(f"tool_calls[{i}].{key}", w[key], g.get(key))
                if w.get("failed", False) != (g.get("error") is not None):
                    mismatch(f"tool_calls[{i}].failed", w.get("failed", False), g.get("error"))

    types = [e["type"] for e in actual.get("events", [])]
    if "events_include" in expected:
        remaining = list(types)
        for wanted in expected["events_include"]:
            if wanted in remaining:
                remaining = remaining[remaining.index(wanted) + 1:]
            else:
                problems.append(f"events_include: {wanted} missing or out of order in {types}")
    for unwanted in expected.get("events_exclude", []):
        if unwanted in types:
            problems.append(f"events_exclude: {unwanted} present in {types}")
    if "events" in expected and expected["events"] != types:
        mismatch("events", expected["events"], types)
    for wanted in expected.get("event_payloads", []):
        found = [e for e in actual.get("events", []) if e["type"] == wanted["type"]]
        occurrence = wanted.get("occurrence", 1)
        if len(found) < occurrence:
            problems.append(f"event_payloads: no {wanted['type']} occurrence {occurrence} in {types}")
            continue
        payload = found[occurrence - 1].get("payload") or {}
        for key, want in wanted["payload"].items():
            if payload.get(key) != want:
                mismatch(f"event_payloads.{wanted['type']}.{key}", want, payload.get(key))
    for name in expected.get("event_fields", []):
        for event in actual.get("events", []):
            if event.get(name) is None:
                problems.append(f"event_fields: {event['type']} has no {name}")

    for key, want in (expected.get("state_includes") or {}).items():
        got = actual.get("state", {}).get(key)
        if want != got:
            mismatch(f"state.{key}", want, got)

    return problems


def resolve_http(workflow: Dict[str, Any], base: str) -> Dict[str, Any]:
    """Every binding does this: a tool url of `{{http}}/...` points at its own local stub."""
    doc = copy.deepcopy(workflow)
    for tool in doc.get("tools") or []:
        if isinstance(tool.get("url"), str):
            tool["url"] = tool["url"].replace("{{http}}", base)
    return doc


def execute(fixture: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Deliver a fixture's turns to the reference runtime. `fixture` has `workflow` resolved."""
    with HttpStub() as stub:
        runtime = ReferenceRuntime(resolve_http(fixture["workflow"], stub.base))
        results: List[Dict[str, Any]] = []
        for spec in fixture["turns"]:
            if spec.get("restart_runtime"):
                runtime.restart()
            if "advance_time_ms" in spec:
                runtime.advance(spec["advance_time_ms"])
            results.append(runtime.submit(Turn(
                conversation_id=spec["conversation_id"],
                turn_id=spec["turn_id"],
                text=spec.get("text", ""),
                user_id=spec.get("user_id", "anonymous"),
                signal=spec.get("signal"),
                metadata=dict(spec.get("metadata") or {}),
                principal=spec.get("principal"),
            ), spec.get("crash_at")))
        return results


def run_fixture(path: Path) -> List[str]:
    fixture = load(path)
    missing = set(fixture["requires"]) - REFERENCE_CAPABILITIES
    if missing:
        return [f"SKIP requires {sorted(missing)}"]

    if fixture.get("workflow") is None:
        fixture["workflow"] = load((path.parent / fixture["workflow_ref"]).resolve())

    results = execute(fixture)
    problems: List[str] = []
    for i, expected in enumerate(fixture["expect"]):
        if i >= len(results):
            problems.append(f"expect[{i}]: no result produced")
            continue
        problems += [f"expect[{i}] ({expected['turn_id']}) {p}" for p in check_expectation(expected, results[i])]
    return problems


def main(argv: List[str]) -> int:
    paths = sorted(FIXTURES.glob("*.yaml"))
    if argv:
        paths = [p for p in paths if load(p)["id"] in argv]
        if not paths:
            print(f"no fixture matches {argv}")
            return 1

    failed = skipped = passed = 0
    for path in paths:
        fixture_id = load(path)["id"]
        try:
            problems = run_fixture(path)
        except (SpecError, KeyError) as exc:
            problems = [f"raised {type(exc).__name__}: {exc}"]
        if problems and problems[0].startswith("SKIP"):
            skipped += 1
            print(f"skip {fixture_id}: {problems[0][5:]}")
        elif problems:
            failed += 1
            print(f"FAIL {fixture_id}")
            for problem in problems:
                print(f"     {problem}")
        else:
            passed += 1
            print(f"pass {fixture_id}")

    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
