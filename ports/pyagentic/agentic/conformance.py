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
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Union, runtime_checkable

import yaml

from .errors import AgenticError
from .events import Turn
from .runtime import LocalRuntime, Runtime, get_runtime


@runtime_checkable
class AsyncSubmitting(Protocol):
    """Runtimes that accept overlapping submissions (the fixtures' `concurrent_with`)."""

    def submit_async(self, event: Union[Turn, Mapping[str, Any]]) -> "Future[Dict[str, Any]]": ...


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


def turn_metadata(spec: Mapping[str, Any]) -> Dict[str, str]:
    """A fixture turn's `metadata` block as the string map a `Turn` carries."""
    return {str(k): str(v) for k, v in dict(spec.get("metadata") or {}).items()}


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
        for batch in concurrent_batches(fixture["turns"]):
            if batch[0].get("restart_runtime"):
                runtime = _restart(runtime, workflow)
            results.extend(_deliver_batch(runtime, batch))
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


def concurrent_batches(turns: Sequence[Mapping[str, Any]]) -> List[List[Mapping[str, Any]]]:
    """Consecutive turns joined by `concurrent_with` form one batch; every other turn is its own."""
    batches: List[List[Mapping[str, Any]]] = []
    i = 0
    while i < len(turns):
        turn = turns[i]
        group = set(turn.get("concurrent_with") or [])
        if not group:
            batches.append([turn])
            i += 1
            continue
        group.add(turn["turn_id"])
        j = i
        while j < len(turns) and turns[j]["turn_id"] in group:
            j += 1
        batches.append(list(turns[i:j]))
        i = j
    return batches


def _deliver_batch(runtime: Runtime, batch: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Deliver one batch of mutually `concurrent_with` turns: on a runtime with `submit_async`
    every turn is handed over in declared order without waiting for the previous one, so the
    turns are in flight together and the runtime decides how they overlap (only within their
    per-conversation order). The results come back in the declared order, whatever order the
    turns finished in. A runtime without `submit_async` receives the turns one after another."""
    turns = [Turn(conversation_id=spec["conversation_id"], turn_id=spec["turn_id"],
                  text=spec.get("text", ""), signal=spec.get("signal"),
                  metadata=turn_metadata(spec)) for spec in batch]
    if len(turns) == 1 or not isinstance(runtime, AsyncSubmitting):
        return [runtime.submit(turn) for turn in turns]
    futures = [runtime.submit_async(turn) for turn in turns]
    return [future.result() for future in futures]


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
