"""Run the shared v1 conformance fixtures, read in place, against a `Runtime`.

    python -m agentic.conformance                 # every fixture, local runtime
    python -m agentic.conformance retry-tool      # one fixture by id
    python -m agentic.conformance --runtime NAME  # any registered runtime

Fixtures live in `spec/conformance/v1/fixtures/` of the Agentic-Streaming checkout. They
are located by walking up from this package or from `AGENTIC_SPEC_DIR`; not finding them
is an error, never a silent skip. A fixture whose `requires` includes a capability the
runtime does not declare `supported` or `partial` is recorded as skipped, never passed.

The binding honours the fixture verbs of `spec/conformance/v1/README.md`, `crash_at` among them. A
crash raises `SimulatedCrash` from `Engine.probe`: a BaseException, so nothing the turn does can
catch it. The binding then restarts the runtime from what it keeps, and the delivery's result has
status `crashed`.
"""

from __future__ import annotations

import argparse
import copy
import importlib.util
import os
import re
import sys
from concurrent.futures import Future
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple, Union, runtime_checkable

import yaml

from .errors import AgenticError
from .events import Turn
from .runtime import LocalRuntime, Runtime, get_runtime
from .workflow_timers import ManualClock


@runtime_checkable
class AsyncSubmitting(Protocol):
    """Runtimes that accept overlapping submissions (the fixtures' `concurrent_with`)."""

    def submit_async(self, event: Union[Turn, Mapping[str, Any]]) -> "Future[Dict[str, Any]]": ...


FIXTURE_SUBDIR = Path("spec") / "conformance" / "v1" / "fixtures"
RESULT_DETAIL_KEY = "runtime_detail"  # excluded from every comparison


class FixturesNotFound(AgenticError):
    error_class = "validation"


class SimulatedCrash(BaseException):
    """The `crash_at` verb. A BaseException, so that nothing a turn does can catch it."""


class CrashPoint:
    """An `Engine.probe` that dies at a fixture's `crash_at`: `event` or `call`, `when`, `occurrence`."""

    def __init__(self, plan: Mapping[str, Any]) -> None:
        self.plan = plan
        self.seen = 0

    def __call__(self, axis: str, name: str, when: str) -> None:
        if self.plan.get(axis) != name or self.plan["when"] != when:
            return
        self.seen += 1
        if self.seen == self.plan.get("occurrence", 1):
            raise SimulatedCrash()


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


def fixture_runtime() -> LocalRuntime:
    """The default runtime under a fixture: local, on a manual processing clock starting at
    zero so `advance_time_ms` is the only thing that moves time."""
    return LocalRuntime(clock=ManualClock())


def run_fixture(path: Path, make_runtime: RuntimeFactory = fixture_runtime) -> Outcome:
    fixture = load_yaml(path)
    if fixture.get("workflow") is None:
        fixture["workflow"] = load_yaml((path.parent / fixture["workflow_ref"]).resolve())
    return run_fixture_document(fixture, make_runtime, path)


def run_fixture_document(fixture: Mapping[str, Any], make_runtime: RuntimeFactory = fixture_runtime,
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

    results: List[Dict[str, Any]] = []
    with ExitStack() as stack:
        workflow = _with_http_stub(fixture["workflow"], stack)
        try:
            runtime.deploy(workflow)
            for batch in concurrent_batches(fixture["turns"]):
                if batch[0].get("restart_runtime"):
                    runtime = _restart(runtime, workflow)
                if batch[0].get("advance_time_ms"):
                    _advance(runtime, int(batch[0]["advance_time_ms"]))
                if batch[0].get("crash_at"):
                    runtime, result = _deliver_to_crash(runtime, workflow, batch[0])
                    results.append(result)
                else:
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


def _with_http_stub(workflow: Any, stack: ExitStack) -> Any:
    """A tool url of `{{http}}/...` points at a stub the binding starts, the one the spec keeps in
    `spec/tools/http_stub.py`. A workflow with no such tool needs no stub, and an `AgentSpec` is
    handed to `deploy` as it is, since only a fixture document carries the placeholder."""
    if not isinstance(workflow, Mapping) or not any("{{http}}" in str(tool.get("url", "")) for tool in workflow.get("tools") or []):
        return workflow
    source = fixtures_dir().parents[2] / "tools" / "http_stub.py"
    loader = importlib.util.spec_from_file_location("agentic_http_stub", source)
    if loader is None or loader.loader is None or not source.exists():
        raise FixturesNotFound(f"no HTTP stub at {source}")
    module = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(module)
    base = stack.enter_context(module.HttpStub()).base
    resolved: Dict[str, Any] = copy.deepcopy(dict(workflow))
    for tool in resolved["tools"]:
        if isinstance(tool.get("url"), str):
            tool["url"] = tool["url"].replace("{{http}}", base)
    return resolved


def turn_of(spec: Mapping[str, Any]) -> Dict[str, Any]:
    """A fixture turn as the mapping `Runtime.submit` takes. A mapping rather than a `Turn`, since a
    fixture may deliver the empty `turn_id` a `Turn` refuses to be built with."""
    return {"conversation_id": spec["conversation_id"], "turn_id": spec["turn_id"], "text": spec.get("text", ""),
            "user_id": spec.get("user_id", "anonymous"), "signal": spec.get("signal"),
            "metadata": turn_metadata(spec), "principal": spec.get("principal")}


def _deliver_to_crash(runtime: Runtime, workflow: Mapping[str, Any],
                      spec: Mapping[str, Any]) -> Tuple[Runtime, Dict[str, Any]]:
    """One delivery that dies at `crash_at`. Returns the runtime to carry on with, recovered from
    what it keeps across a crash, and the delivery's result."""
    engine = getattr(runtime, "engine", None)
    if engine is None:
        raise AgenticError(f"runtime {runtime.name!r} has no engine probe; crash_at cannot run against it")
    engine.probe = CrashPoint(spec["crash_at"])
    try:
        result = runtime.submit(turn_of(spec))
    except SimulatedCrash:
        return _restart(runtime, workflow), {"conversation_id": spec["conversation_id"], "turn_id": spec["turn_id"],
                                             "status": "crashed", "events": []}
    engine.probe = None
    return runtime, result


def _deliver_batch(runtime: Runtime, batch: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Deliver one batch of mutually `concurrent_with` turns: on a runtime with `submit_async`
    every turn is handed over in declared order without waiting for the previous one, so the
    turns are in flight together and the runtime decides how they overlap (only within their
    per-conversation order). The results come back in the declared order, whatever order the
    turns finished in. A runtime without `submit_async` receives the turns one after another."""
    turns = [turn_of(spec) for spec in batch]
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


def _advance(runtime: Runtime, ms: int) -> None:
    clock = runtime.clock if isinstance(runtime, LocalRuntime) else None
    if not isinstance(clock, ManualClock):
        raise AgenticError(f"runtime {runtime.name!r} is not on a ManualClock; fixtures with advance_time_ms "
                           f"cannot run against it")
    clock.advance(ms)


def run_all(make_runtime: RuntimeFactory = fixture_runtime, only: Sequence[str] = (),
            directory: Optional[Path] = None) -> List[Outcome]:
    paths = fixture_paths(directory)
    if only:
        paths = [p for p in paths if load_yaml(p)["id"] in only]
        if not paths:
            raise FixturesNotFound(f"no fixture matches {list(only)}")
    return [run_fixture(p, make_runtime) for p in paths]


def main(argv: Optional[Sequence[str]] = None) -> int:
    # The fixtures talk to a local stub. Nothing else is allowed, so they can prove the refusals.
    os.environ.setdefault("AGENTIC_EGRESS_ALLOW_HOSTS", "127.0.0.1,localhost")
    os.environ.setdefault("AGENTIC_EGRESS_ALLOW_PRIVATE", "true")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ids", nargs="*", help="fixture ids to run (default: all)")
    parser.add_argument("--runtime", default="local")
    args = parser.parse_args(argv)
    # Fixtures advance logical time, so the local runtime runs on the same clock as under pytest.
    make_runtime = fixture_runtime if args.runtime == "local" else (lambda: get_runtime(args.runtime))
    outcomes = run_all(make_runtime, args.ids)
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
