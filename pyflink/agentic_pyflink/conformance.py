"""PyFlink binding of ``spec/conformance/v1``.

Loads each fixture straight from the repository, runs its workflow as a Flink job on the local
MiniCluster through :class:`FlinkRuntime`, and applies the comparator of
``spec/tools/run_conformance.py`` (imported, not re-implemented) to the normalized results.

Fixture verbs map onto Flink as in the JVM binding (``FlinkConformanceHarness``): a turn is an
element keyed by conversation id; ``concurrent_with`` turns are delivered back to back in one
file so Flink's per-key ordering is what gets tested; ``restart_runtime`` is stop-with-savepoint
followed by a fresh job restored from that savepoint, so only checkpointed state survives.

Run it directly for a report::

    python -m agentic_pyflink.conformance            # all fixtures
    python -m agentic_pyflink.conformance duplicate-turn retrieval
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .capabilities import CONFORMANCE_CAPABILITIES
from .config import FlinkConfig
from .jars import repo_root
from .runtime import FlinkRuntime


@dataclass
class Outcome:
    """Exactly one of: passed, skipped (with reason), failed (with problems)."""

    id: str
    skip_reason: str | None = None
    problems: list[str] = field(default_factory=list)

    @property
    def skipped(self) -> bool:
        return self.skip_reason is not None

    @property
    def passed(self) -> bool:
        return self.skip_reason is None and not self.problems

    @property
    def status(self) -> str:
        return "skipped" if self.skipped else ("passed" if self.passed else "failed")


def spec_dir() -> Path:
    root = repo_root()
    if root is None or not (root / "spec" / "conformance" / "v1" / "fixtures").is_dir():
        raise FileNotFoundError("spec/conformance/v1/fixtures not found; run from a repository checkout")
    return root / "spec"


def fixture_files() -> list[Path]:
    return sorted((spec_dir() / "conformance" / "v1" / "fixtures").glob("*.yaml"))


def load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _comparator():
    """``check_expectation`` from ``spec/tools/run_conformance.py``: the shared comparison rules."""
    tools = spec_dir() / "tools"
    if str(tools) not in sys.path:
        sys.path.insert(0, str(tools))
    location = tools / "run_conformance.py"
    spec = importlib.util.spec_from_file_location("spec_run_conformance", location)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {location}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.check_expectation


def run_fixture(path: Path, config: FlinkConfig | None = None, result_timeout: float = 90.0) -> Outcome:
    fixture = load(path)
    fixture_id = str(fixture["id"])
    missing = sorted(set(fixture["requires"]) - CONFORMANCE_CAPABILITIES)
    if missing:
        return Outcome(fixture_id, skip_reason=f"requires {missing}")

    workflow = fixture.get("workflow")
    if workflow is None:
        workflow = load((path.parent / fixture["workflow_ref"]).resolve())

    check_expectation = _comparator()
    results: list[Mapping[str, Any]] = []
    with FlinkRuntime(config or FlinkConfig(checkpoint_interval="200ms"), result_timeout=result_timeout) as rt:
        rt.deploy(workflow)
        batch: list[dict[str, Any]] = []

        def flush() -> None:
            if batch:
                results.extend(rt.submit_all(list(batch)))
                batch.clear()

        for turn in fixture["turns"]:
            if turn.get("restart_runtime"):
                flush()
                rt.restart()
            event = {"conversation_id": turn["conversation_id"], "turn_id": turn["turn_id"]}
            if turn.get("signal") is not None:
                event["signal"] = turn["signal"]
            else:
                event["text"] = turn.get("text", "")
            if turn.get("concurrent_with"):
                batch.append(event)
            else:
                flush()
                batch.append(event)
        flush()

    problems: list[str] = []
    for i, expected in enumerate(fixture["expect"]):
        if i >= len(results):
            problems.append(f"expect[{i}]: no result produced")
            continue
        problems += [f"expect[{i}] ({expected['turn_id']}) {p}" for p in check_expectation(expected, results[i])]
    return Outcome(fixture_id, problems=problems)


def run_all(ids: Sequence[str] = (), config: FlinkConfig | None = None) -> list[Outcome]:
    paths = fixture_files()
    if ids:
        paths = [p for p in paths if load(p)["id"] in ids]
    return [run_fixture(p, config) for p in paths]


def main(argv: Sequence[str]) -> int:
    outcomes = run_all(argv)
    failed = 0
    for o in outcomes:
        if o.skipped:
            print(f"SKIP {o.id}: {o.skip_reason}")
        elif o.passed:
            print(f"PASS {o.id}")
        else:
            failed += 1
            print(f"FAIL {o.id}")
            for p in o.problems:
                print(f"     {p}")
    passed = sum(1 for o in outcomes if o.passed)
    skipped = sum(1 for o in outcomes if o.skipped)
    print(f"{passed} passed, {failed} failed, {skipped} skipped")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
