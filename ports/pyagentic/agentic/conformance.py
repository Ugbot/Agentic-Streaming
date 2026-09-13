"""Run the shared v1 conformance fixtures, read in place, against a `Runtime`.

    python -m agentic.conformance                 # every fixture, local runtime
    python -m agentic.conformance retry-tool      # one fixture by id
    python -m agentic.conformance --runtime NAME  # any registered runtime

Fixtures live in `spec/conformance/v1/fixtures/` of the Agentic-Streaming checkout. They
are located by walking up from this package or from `AGENTIC_SPEC_DIR`; not finding them
is an error, never a silent skip. A fixture whose `requires` includes a capability the
runtime does not declare `supported` or `partial` is recorded as skipped, never passed.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

import yaml

from .errors import AgenticError
from .events import Turn
from .runtime import LocalRuntime, Runtime, get_runtime

FIXTURE_SUBDIR = Path("spec") / "conformance" / "v1" / "fixtures"
RESULT_DETAIL_KEY = "runtime_detail"  # excluded from every comparison


class FixturesNotFound(AgenticError):
    error_class = "validation"


def fixtures_dir(start: Optional[Path] = None) -> Path:
    override = os.environ.get("AGENTIC_SPEC_DIR")
    if override:
        candidate = Path(override) / "conformance" / "v1" / "fixtures"
        if candidate.is_dir():
            return candidate
        raise FixturesNotFound(f"AGENTIC_SPEC_DIR={override!r} has no conformance/v1/fixtures directory")
    here = (start or Path(__file__)).resolve()
    for parent in [here] + list(here.parents):
        candidate = parent / FIXTURE_SUBDIR
        if candidate.is_dir():
            return candidate
    raise FixturesNotFound(
        "could not find spec/conformance/v1/fixtures above this package; run from an "
        "Agentic-Streaming checkout or set AGENTIC_SPEC_DIR=<checkout>/spec")


def fixture_paths(directory: Optional[Path] = None) -> List[Path]:
    paths = sorted((directory or fixtures_dir()).glob("*.yaml"))
    if not paths:
        raise FixturesNotFound(f"no fixtures in {directory or fixtures_dir()}")
    return paths


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        loaded: Dict[str, Any] = yaml.safe_load(fh)
    return loaded


def check_expectation(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> List[str]:
    """The comparison rules of `spec/conformance/v1/README.md`. Returns mismatch messages."""
    problems: List[str] = []

    def mismatch(name: str, want: Any, got: Any) -> None:
        problems.append(f"{name}: expected {want!r}, got {got!r}")

    for name in ("conversation_id", "status", "path", "reply"):
        if name in expected and expected[name] != actual.get(name):
            mismatch(name, expected[name], actual.get(name))

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
                for key in ("index", "attempt", "args"):
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

    for key, want in (expected.get("state_includes") or {}).items():
        got = (actual.get("state") or {}).get(key)
        if want != got:
            mismatch(f"state.{key}", want, got)

    return problems


@dataclass
class Outcome:
    fixture_id: str
    path: Path
    status: str  # pass | fail | skip
    problems: List[str] = field(default_factory=list)
    results: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def reason(self) -> str:
        return "; ".join(self.problems)


RuntimeFactory = Callable[[], Runtime]


def run_fixture(path: Path, make_runtime: RuntimeFactory = LocalRuntime) -> Outcome:
    fixture = load_yaml(path)
    if fixture.get("workflow") is None:
        fixture["workflow"] = load_yaml((path.parent / fixture["workflow_ref"]).resolve())
    return run_fixture_document(fixture, make_runtime, path)


def run_fixture_document(fixture: Mapping[str, Any], make_runtime: RuntimeFactory = LocalRuntime,
                         path: Optional[Path] = None) -> Outcome:
    """Run one fixture whose `workflow` is already resolved and compare it with `expect`."""
    fixture_id = fixture["id"]
    path = path or Path(fixture_id)
    runtime = make_runtime()
    declared = runtime.capabilities()
    missing = sorted(cap for cap in fixture["requires"] if declared.get(cap) not in ("supported", "partial"))
    if missing:
        runtime.close()
        declared_as = ", ".join(f"{m}={declared.get(m, 'absent')}" for m in missing)
        return Outcome(fixture_id, path, "skip", [f"requires {missing}, declared {declared_as}"])

    workflow = fixture["workflow"]
    results: List[Dict[str, Any]] = []
    try:
        runtime.deploy(workflow)
        for spec in fixture["turns"]:
            if spec.get("restart_runtime"):
                runtime = _restart(runtime, workflow)
            results.append(runtime.submit(Turn(
                conversation_id=spec["conversation_id"],
                turn_id=spec["turn_id"],
                text=spec.get("text", ""),
                signal=spec.get("signal"),
            )))
    except AgenticError as exc:
        return Outcome(fixture_id, path, "fail", [f"raised {type(exc).__name__}: {exc}"], results)
    finally:
        runtime.close()

    problems: List[str] = []
    for i, expected in enumerate(fixture["expect"]):
        if i >= len(results):
            problems.append(f"expect[{i}]: no result produced")
            continue
        problems += [f"expect[{i}] ({expected['turn_id']}) {p}" for p in check_expectation(expected, results[i])]
    return Outcome(fixture_id, path, "fail" if problems else "pass", problems, results)


def matrix_binding(fixture: Mapping[str, Any]) -> Union[List[Dict[str, Any]], Dict[str, str]]:
    """The `agentic.conformance` entry point for `spec/tools/conformance_matrix.py`.

    Receives a fixture with `workflow` resolved; returns the normalized results in turn order,
    or `{"skip": reason}` naming the capabilities the local runtime does not claim. The matrix
    runner does the comparison itself.
    """
    outcome = run_fixture_document(fixture)
    if outcome.status == "skip":
        return {"skip": outcome.reason}
    if len(outcome.results) < len(fixture["turns"]):
        raise AgenticError(outcome.reason)
    return outcome.results


def _restart(runtime: Runtime, workflow: Mapping[str, Any]) -> Runtime:
    restart = getattr(runtime, "restart", None)
    if not callable(restart):
        raise AgenticError(f"runtime {runtime.name!r} has no restart(); fixtures with restart_runtime "
                           f"cannot run against it")
    fresh = restart()
    if not isinstance(fresh, Runtime):
        raise AgenticError(f"{runtime.name}.restart() must return a Runtime")
    return fresh


def run_all(make_runtime: RuntimeFactory = LocalRuntime, only: Sequence[str] = (),
            directory: Optional[Path] = None) -> List[Outcome]:
    paths = fixture_paths(directory)
    if only:
        paths = [p for p in paths if load_yaml(p)["id"] in only]
        if not paths:
            raise FixturesNotFound(f"no fixture matches {list(only)}")
    return [run_fixture(p, make_runtime) for p in paths]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ids", nargs="*", help="fixture ids to run (default: all)")
    parser.add_argument("--runtime", default="local")
    args = parser.parse_args(argv)
    outcomes = run_all(lambda: get_runtime(args.runtime), args.ids)
    counts = {"pass": 0, "fail": 0, "skip": 0}
    for outcome in outcomes:
        counts[outcome.status] += 1
        if outcome.status == "pass":
            print(f"pass {outcome.fixture_id}")
        elif outcome.status == "skip":
            print(f"skip {outcome.fixture_id}: {outcome.reason}")
        else:
            print(f"FAIL {outcome.fixture_id}")
            for problem in outcome.problems:
                print(f"     {problem}")
    print(f"\n{counts['pass']} passed, {counts['fail']} failed, {counts['skip']} skipped")
    return 1 if counts["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
