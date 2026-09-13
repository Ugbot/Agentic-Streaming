"""Run the shared ``spec/conformance/v1`` fixtures against a registered runtime.

::

    python -m agentic_flink.conformance --runtime local-jvm [--fixtures spec/conformance/v1/fixtures]

Semantics mirror ``spec/tools/run_conformance.py`` (whose ``check_expectation`` comparator is
reused verbatim when the spec tree is reachable): a fixture whose ``requires`` lists a capability
the runtime does not report as ``supported`` or ``partial`` is a **skip**, never a pass; a
fixture that needs a runtime restart is a skip unless the runtime exposes ``restart()``; a
fixture with ``concurrent_with`` turns uses ``submit_async`` when available and one bounded
``submit_all`` batch otherwise.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import importlib.util
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, runtime_checkable

import yaml

from ._contract import Runtime, get_runtime
from .workflow import AgentSpec, Event


@runtime_checkable
class Restartable(Protocol):
    """Runtimes that can model a fixture's ``restart_runtime`` (state dropped, log kept)."""

    def restart(self) -> None: ...


@runtime_checkable
class AsyncSubmitting(Protocol):
    """Runtimes that accept overlapping submissions (fixtures' ``concurrent_with``)."""

    def submit_async(self, event: Event) -> "concurrent.futures.Future[Dict[str, Any]]": ...


@runtime_checkable
class BatchSubmitting(Protocol):
    """Runtimes that run a whole batch of events as one unit (bounded jobs)."""

    def submit_all(self, events: Sequence[Event]) -> List[Dict[str, Any]]: ...


PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_SPEC_ROOT = PACKAGE_DIR.parents[1] / "spec"


def spec_root() -> Path:
    return Path(os.environ.get("AGENTIC_SPEC_ROOT", DEFAULT_SPEC_ROOT))


def default_fixtures_dir() -> Path:
    return spec_root() / "conformance" / "v1" / "fixtures"


def load_comparator() -> Callable[[Dict[str, Any], Dict[str, Any]], List[str]]:
    """``check_expectation`` from ``spec/tools/run_conformance.py`` (the shared comparator)."""
    path = spec_root() / "tools" / "run_conformance.py"
    if not path.exists():
        raise FileNotFoundError(
            f"shared comparator {path} not found; set AGENTIC_SPEC_ROOT to a checkout's spec/ directory"
        )
    module_spec = importlib.util.spec_from_file_location("agentic_spec_run_conformance", path)
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module.check_expectation


@dataclass
class Outcome:
    fixture_id: str
    status: str  # "pass" | "fail" | "skip"
    reason: Optional[str] = None
    problems: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.status == "pass":
            return f"PASS {self.fixture_id}"
        if self.status == "skip":
            return f"SKIP {self.fixture_id}: {self.reason}"
        return f"FAIL {self.fixture_id}\n  " + "\n  ".join(self.problems or [self.reason or ""])


def _load(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def fixture_workflow(path: Path, fixture: Dict[str, Any]) -> AgentSpec:
    workflow = fixture.get("workflow")
    if workflow is None:
        workflow = _load((path.parent / fixture["workflow_ref"]).resolve())
    return AgentSpec(workflow)


def _event(turn: Dict[str, Any]) -> Event:
    cid, tid = str(turn["conversation_id"]), str(turn["turn_id"])
    if turn.get("signal") is not None:
        return Event.resume(cid, tid, turn["signal"])
    return Event.turn(cid, tid, str(turn.get("text", "")))


def run_fixture(path: Path, runtime: Runtime, comparator=None) -> Outcome:
    """Run one fixture on an (undeployed) runtime instance. The runtime is closed afterwards."""
    fixture = _load(path)
    fixture_id = fixture["id"]
    caps = runtime.capabilities()
    unsupported = sorted(c for c in fixture["requires"] if caps.get(c, "unsupported") not in ("supported", "partial"))
    if unsupported:
        return Outcome(fixture_id, "skip", f"runtime {runtime.name!r} does not support {unsupported}")
    turns: List[Dict[str, Any]] = fixture["turns"]
    needs_restart = any(t.get("restart_runtime") for t in turns)
    if needs_restart and not isinstance(runtime, Restartable):
        return Outcome(fixture_id, "skip", f"runtime {runtime.name!r} cannot model restart_runtime")
    comparator = comparator or load_comparator()

    try:
        runtime.deploy(fixture_workflow(path, fixture))
        results = _drive(runtime, turns)
    finally:
        runtime.close()

    problems: List[str] = []
    for i, expected in enumerate(fixture["expect"]):
        if i >= len(results):
            problems.append(f"expect[{i}]: no result produced")
            continue
        problems += [f"expect[{i}] ({expected['turn_id']}) {p}" for p in comparator(expected, results[i])]
    return Outcome(fixture_id, "fail" if problems else "pass", problems=problems)


def _drive(runtime: Runtime, turns: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(runtime, AsyncSubmitting) and isinstance(runtime, BatchSubmitting) \
            and not any(t.get("restart_runtime") for t in turns):
        return list(runtime.submit_all([_event(t) for t in turns]))

    results: List[Dict[str, Any]] = []
    pending: "List[concurrent.futures.Future[Dict[str, Any]]]" = []

    def drain() -> None:
        while pending:
            results.append(pending.pop(0).result())

    for turn in turns:
        if turn.get("restart_runtime") and isinstance(runtime, Restartable):
            drain()
            runtime.restart()
        event = _event(turn)
        if turn.get("concurrent_with") is not None and isinstance(runtime, AsyncSubmitting):
            pending.append(runtime.submit_async(event))
        else:
            drain()
            results.append(runtime.submit(event))
    drain()
    return results


def run_all(runtime_name: str, fixtures_dir: Optional[Path] = None, only: Sequence[str] = (),
            **runtime_options: Any) -> List[Outcome]:
    """Run every fixture (or those whose id is in ``only``) on a fresh ``runtime_name`` instance each."""
    fixtures_dir = fixtures_dir or default_fixtures_dir()
    paths = sorted(fixtures_dir.glob("*.yaml"))
    if not paths:
        raise FileNotFoundError(f"no fixtures under {fixtures_dir}")
    comparator = load_comparator()
    ids = {_load(p)["id"]: p for p in paths}
    unknown = sorted(set(only) - set(ids))
    if unknown:
        raise KeyError(f"unknown fixture id(s) {unknown}; known: {sorted(ids)}")
    outcomes: List[Outcome] = []
    for fixture_id, path in ids.items():
        if only and fixture_id not in only:
            continue
        outcomes.append(run_fixture(path, get_runtime(runtime_name, **runtime_options), comparator))
    return outcomes


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--runtime", default="local-jvm")
    parser.add_argument("--fixtures", type=Path, default=None)
    parser.add_argument("--parallelism", type=int, default=None, help="flink only")
    parser.add_argument("ids", nargs="*")
    args = parser.parse_args(argv)
    options: Dict[str, Any] = {}
    if args.parallelism is not None:
        options["parallelism"] = args.parallelism
    outcomes = run_all(args.runtime, args.fixtures, args.ids, **options)
    for o in outcomes:
        print(o)
    passed = sum(o.status == "pass" for o in outcomes)
    failed = sum(o.status == "fail" for o in outcomes)
    skipped = sum(o.status == "skip" for o in outcomes)
    print(f"{passed} passed, {failed} failed, {skipped} skipped ({args.runtime})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["Outcome", "default_fixtures_dir", "fixture_workflow", "load_comparator", "run_all", "run_fixture"]
