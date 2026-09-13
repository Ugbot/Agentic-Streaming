#!/usr/bin/env python3
"""Run the v1 conformance fixtures against every runtime binding and build the capability matrix.

    python spec/tools/conformance_matrix.py                       # every binding it can find
    python spec/tools/conformance_matrix.py --runtimes reference clojure
    python spec/tools/conformance_matrix.py --require jvm-core --require flink   # missing toolchain is a failure
    python spec/tools/conformance_matrix.py --output build/conformance.json --write-docs docs/capabilities.md
    python spec/tools/conformance_matrix.py --render build/conformance.json --write-docs docs/capabilities.md

One runner, one comparator, one artifact. Each runtime is driven through its own binding,
never a reimplementation of it:

| runtime   | binding                                                          | comparison            |
|-----------|------------------------------------------------------------------|-----------------------|
| reference | `spec/tools/reference_runtime.py`, in process                    | here, on its results  |
| jvm-core  | `ports/jagentic-core` JUnit `ConformanceTest`                    | inside the binding    |
| flink     | `src/test/.../FlinkConformanceTest` on a MiniCluster             | inside the binding    |
| pekko     | `agentic-pekko` JUnit `PekkoConformanceTest`                     | inside the binding    |
| clojure   | `agentic-clj` `agentic.conformance/run-all`                      | here, on its results  |
| python    | an installed `agentic.conformance` entry point (see below)       | here, on its results  |

"Here" means the binding returns one normalized result document per turn (schema
`spec/v1/result.schema.json`); the runner validates every document against that schema,
drops `runtime_detail`, and applies `run_conformance.check_expectation`, the comparator the
conformance README defines. "Inside the binding" means the binding runs the same comparator
as a port and reports one JUnit test case per fixture; the runner reads the surefire XML and
records passed / skipped (an aborted assumption) / failed per fixture.

A Python binding is discovered through the `agentic.conformance` entry-point group (or
`--python-binding module:callable`). The callable receives the fixture as a mapping (with
`workflow` already resolved) and returns the list of normalized results, in turn order.

Capability cells are derived only from outcomes, never declared:

- `supported`: every fixture that requires the capability passed on that runtime;
- `partial`: some passed and at least one failed or was skipped, the note names them;
- `unsupported`: the runtime skipped or failed every fixture that requires it;
- `not_tested`: no fixture requires it, or the runtime did not run (binding absent,
  toolchain missing, or the binding itself errored). The note says which.

Exit status is 1 when any fixture failed, any binding errored, or a runtime named with
`--require` could not run. Missing toolchains for runtimes not marked `--require` are
reported as `not_tested`, loudly, in the output and in the artifact.
"""

from __future__ import annotations

import argparse
import copy
import datetime as _dt
import importlib
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reference_runtime import ReferenceRuntime, SpecError, Turn  # noqa: E402
from run_conformance import FIXTURES, REFERENCE_CAPABILITIES, check_expectation, load  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RESULT_SCHEMA = ROOT / "spec" / "v1" / "result.schema.json"
PRIMITIVES = ROOT / "spec" / "v1" / "primitives.md"

# The v1 capability ids, in the order primitives.md section 6 lists them.
CAPABILITIES: List[str] = [
    "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails",
    "verifier", "ordering", "idempotency", "retry", "memory", "retrieval", "context_window",
    "replay", "suspend_resume", "timers", "saga", "a2a", "cep", "event_time",
    "checkpoint_recovery", "parallelism", "durable_store",
]

CELL_VALUES = ("supported", "partial", "unsupported", "not_tested")
PASSED, FAILED, SKIPPED = "passed", "failed", "skipped"
RAN, NOT_TESTED, ERROR = "ran", "not_tested", "error"

MVN = shutil.which("mvn")
CLOJURE = shutil.which("clojure")


def maven_command(root: Path) -> Optional[str]:
    """The repo's committed Maven wrapper when present, else `mvn` from PATH."""
    wrapper = root / "mvnw"
    if wrapper.is_file() and os.access(wrapper, os.X_OK):
        return str(wrapper)
    return MVN


class BindingError(RuntimeError):
    """The binding is present and its toolchain is available, but it did not produce outcomes."""


@dataclass
class FixtureOutcome:
    fixture: str
    status: str  # passed | failed | skipped
    requires: List[str]
    problems: List[str] = field(default_factory=list)
    skip_reason: Optional[str] = None
    missing: List[str] = field(default_factory=list)  # capabilities the skip reason names

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"fixture": self.fixture, "status": self.status, "requires": self.requires}
        if self.problems:
            d["problems"] = self.problems
        if self.skip_reason is not None:
            d["skip_reason"] = self.skip_reason
            d["missing"] = self.missing
        return d


@dataclass
class RuntimeReport:
    name: str
    binding: str
    comparison: str  # runner | binding
    status: str  # ran | not_tested | error
    reason: Optional[str] = None
    fixtures: List[FixtureOutcome] = field(default_factory=list)
    command: Optional[str] = None
    duration_s: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "name": self.name, "binding": self.binding, "comparison": self.comparison, "status": self.status,
        }
        if self.reason:
            d["reason"] = self.reason
        if self.command:
            d["command"] = self.command
        if self.duration_s is not None:
            d["duration_s"] = round(self.duration_s, 1)
        d["fixtures"] = [f.to_dict() for f in self.fixtures]
        d["capabilities"] = derive_capabilities(self)
        return d


# --------------------------------------------------------------------------- fixtures

def fixture_index(fixtures_dir: Path = FIXTURES) -> Dict[str, Dict[str, Any]]:
    """Fixture id -> fixture document with `workflow` resolved, in file order."""
    out: Dict[str, Dict[str, Any]] = {}
    for path in sorted(fixtures_dir.glob("*.yaml")):
        doc = load(path)
        if doc.get("workflow") is None:
            doc["workflow"] = load((path.parent / doc["workflow_ref"]).resolve())
        doc["_file"] = path.name
        out[doc["id"]] = doc
    return out


def capability_ids_from_spec(primitives: Path = PRIMITIVES) -> List[str]:
    """The `Capability ids in v1:` list from primitives.md, so CAPABILITIES cannot drift from it."""
    text = primitives.read_text(encoding="utf-8")
    start = text.index("Capability ids in v1:")
    end = text.index("\n\n", start)
    return [tok.strip("`") for tok in text[start:end].split("`")[1::2]]


def result_validator() -> Draft202012Validator:
    return Draft202012Validator(json.loads(RESULT_SCHEMA.read_text(encoding="utf-8")))


def strip_runtime_detail(result: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(result)
    out.pop("runtime_detail", None)
    return out


def compare_results(fixture: Dict[str, Any], results: Sequence[Dict[str, Any]],
                    validator: Optional[Draft202012Validator] = None) -> List[str]:
    """Schema-validate and compare a binding's normalized results with the fixture's expectations."""
    validator = validator or result_validator()
    problems: List[str] = []
    for i, result in enumerate(results):
        for err in sorted(validator.iter_errors(result), key=lambda e: list(e.absolute_path)):
            pointer = "/".join(str(p) for p in err.absolute_path)
            problems.append(f"result[{i}] invalid against result.schema.json at /{pointer}: {err.message}")
    for i, expected in enumerate(fixture["expect"]):
        if i >= len(results):
            problems.append(f"expect[{i}]: no result produced")
            continue
        actual = strip_runtime_detail(results[i])
        problems += [f"expect[{i}] ({expected['turn_id']}) {p}" for p in check_expectation(expected, actual)]
    return problems


def skip_outcome(fixture: Dict[str, Any], missing: Iterable[str], reason: Optional[str] = None) -> FixtureOutcome:
    missing = sorted(missing)
    return FixtureOutcome(fixture["id"], SKIPPED, list(fixture["requires"]),
                          skip_reason=reason or f"requires {missing}", missing=missing)


def outcome_from_results(fixture: Dict[str, Any], results: Sequence[Dict[str, Any]],
                         validator: Optional[Draft202012Validator] = None) -> FixtureOutcome:
    problems = compare_results(fixture, results, validator)
    return FixtureOutcome(fixture["id"], FAILED if problems else PASSED, list(fixture["requires"]), problems)


def missing_from_skip_reason(reason: str, requires: Sequence[str]) -> List[str]:
    """The capability ids a binding's skip message names, restricted to the fixture's `requires`."""
    return sorted(c for c in requires if c in reason)


# --------------------------------------------------------------------------- bindings

def run_reference(fixtures: Dict[str, Dict[str, Any]]) -> RuntimeReport:
    report = RuntimeReport("reference", "spec/tools/reference_runtime.py", "runner", RAN)
    validator = result_validator()
    for fixture in fixtures.values():
        missing = set(fixture["requires"]) - REFERENCE_CAPABILITIES
        if missing:
            report.fixtures.append(skip_outcome(fixture, missing))
            continue
        runtime = ReferenceRuntime(fixture["workflow"])
        results: List[Dict[str, Any]] = []
        try:
            for spec in fixture["turns"]:
                if spec.get("restart_runtime"):
                    runtime.restart()
                results.append(runtime.submit(Turn(
                    conversation_id=spec["conversation_id"], turn_id=spec["turn_id"],
                    text=spec.get("text", ""), signal=spec.get("signal"),
                )))
        except (SpecError, KeyError) as exc:
            report.fixtures.append(FixtureOutcome(fixture["id"], FAILED, list(fixture["requires"]),
                                                  [f"raised {type(exc).__name__}: {exc}"]))
            continue
        report.fixtures.append(outcome_from_results(fixture, results, validator))
    return report


def run_python_binding(fixtures: Dict[str, Dict[str, Any]], spec: Optional[str]) -> RuntimeReport:
    """`spec` is `module:callable`; when None, the `agentic.conformance` entry-point group is consulted."""
    report = RuntimeReport("python", "agentic.conformance entry point", "runner", RAN)
    if spec is None:
        eps = list(importlib.metadata.entry_points(group="agentic.conformance"))
        if not eps:
            report.status, report.reason = NOT_TESTED, (
                "binding absent: no Python binding installed, the `agentic.conformance` entry-point group is empty "
                "and --python-binding was not given")
            return report
        if len(eps) > 1:
            names = ", ".join(sorted(f"{e.name}={e.value}" for e in eps))
            report.status, report.reason = ERROR, f"several Python bindings installed, pass --python-binding: {names}"
            return report
        report.binding = f"entry point {eps[0].name} = {eps[0].value}"
        spec = eps[0].value
    module_name, _, attr = spec.partition(":")
    if not attr:
        report.status, report.reason = ERROR, f"--python-binding must be module:callable, got {spec!r}"
        return report
    try:
        fn: Callable[[Dict[str, Any]], Sequence[Dict[str, Any]]] = getattr(importlib.import_module(module_name), attr)
    except (ImportError, AttributeError) as exc:
        report.status, report.reason = ERROR, f"cannot load Python binding {spec}: {exc}"
        return report
    if report.binding.startswith("agentic.conformance"):
        report.binding = f"--python-binding {spec}"
    validator = result_validator()
    for fixture in fixtures.values():
        doc = {k: v for k, v in fixture.items() if not k.startswith("_")}
        try:
            results = fn(copy.deepcopy(doc))
        except Exception as exc:  # the binding decides what a skip looks like: a dict with `skip`
            report.fixtures.append(FixtureOutcome(fixture["id"], FAILED, list(fixture["requires"]),
                                                  [f"binding raised {type(exc).__name__}: {exc}"]))
            continue
        if isinstance(results, dict) and "skip" in results:
            reason = str(results["skip"])
            report.fixtures.append(skip_outcome(fixture, missing_from_skip_reason(reason, fixture["requires"]), reason))
            continue
        report.fixtures.append(outcome_from_results(fixture, list(results), validator))
    return report


def _run(cmd: List[str], cwd: Path, log: Path, env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w", encoding="utf-8") as fh:
        return subprocess.run(cmd, cwd=str(cwd), stdout=fh, stderr=subprocess.STDOUT, text=True,
                              env={**os.environ, **(env or {})})


def parse_surefire(reports_dir: Path, fixtures: Dict[str, Dict[str, Any]], suite: str,
                   factory: str = "fixtures") -> List[FixtureOutcome]:
    """One outcome per fixture from the surefire XML of `suite`.

    The bindings are JUnit `@TestFactory` methods producing one dynamic test per fixture file, in
    sorted file order. Surefire writes a dynamic test under its display name when the reporter is
    configured for phrased names, and under the factory method name otherwise; in the latter case
    the cases are matched to fixtures by position, and any fixture id embedded in a skip or failure
    message must agree with that position.
    """
    files = list(reports_dir.glob(f"TEST-*{suite}.xml"))
    if not files:
        raise BindingError(f"no surefire report for {suite} under {reports_dir}")
    by_file = {doc["_file"]: doc for doc in fixtures.values()}
    in_order = list(fixtures.values())
    cases: List[ET.Element] = [c for xml in files for c in ET.parse(xml).getroot().iter("testcase")]
    positional = [c for c in cases if c.get("name", "") == factory]
    if positional and len(positional) != len(in_order):
        raise BindingError(f"{suite}.{factory} produced {len(positional)} dynamic tests for {len(in_order)} fixtures")
    seen: Dict[str, FixtureOutcome] = {}
    for case in cases:
        name = case.get("name", "")
        if name == factory:
            fixture = in_order[positional.index(case)]
        else:
            fixture = by_file.get(name) or by_file.get(name.split("(")[0])
        if fixture is None:
            continue
        message = " ".join(filter(None, (child.get("message") for child in case)))
        named = [doc["id"] for doc in in_order if f" {doc['id']}:" in message or f" {doc['id']}\n" in message]
        if named and fixture["id"] not in named:
            raise BindingError(f"{suite}: test case for {fixture['id']} reports {named}: {message}")
        requires = list(fixture["requires"])
        skipped = case.find("skipped")
        failure = case.find("failure") if case.find("failure") is not None else case.find("error")
        if skipped is not None:
            reason = skipped.get("message") or "skipped by the binding"
            seen[fixture["id"]] = FixtureOutcome(fixture["id"], SKIPPED, requires, skip_reason=reason,
                                                 missing=missing_from_skip_reason(reason, requires))
        elif failure is not None:
            first_line = (failure.text or "").strip().splitlines()[:1]
            text = failure.get("message") or (first_line[0] if first_line else "failed")
            seen[fixture["id"]] = FixtureOutcome(fixture["id"], FAILED, requires, [text])
        else:
            seen[fixture["id"]] = FixtureOutcome(fixture["id"], PASSED, requires)
    absent = [f for f in fixtures if f not in seen]
    if absent:
        raise BindingError(f"{suite} reported no test case for fixtures {absent}")
    return [seen[f] for f in fixtures]


@dataclass
class MavenSuite:
    name: str
    pom: str  # relative to ROOT
    suite: str
    needs_core_install: bool = True

    def binding(self) -> str:
        return f"{self.pom.rsplit('/pom.xml', 1)[0] or '.'}: JUnit {self.suite}"


MAVEN_SUITES = {
    "jvm-core": MavenSuite("jvm-core", "ports/jagentic-core/pom.xml", "ConformanceTest", needs_core_install=False),
    "flink": MavenSuite("flink", "pom.xml", "FlinkConformanceTest"),
    "pekko": MavenSuite("pekko", "agentic-pekko/pom.xml", "PekkoConformanceTest"),
}


def _suite_present(root: Path, suite: MavenSuite) -> bool:
    module = root / suite.pom
    if not module.exists():
        return False
    return any(module.parent.glob(f"src/test/java/**/{suite.suite}.java"))


_core_installed = False


def ensure_core_installed(root: Path, logs: Path) -> None:
    global _core_installed
    if _core_installed:
        return
    proc = _run([maven_command(root), "-B", "-ntp", "-f", "ports/jagentic-core/pom.xml", "install", "-DskipTests"], root,
                logs / "jagentic-core-install.log")
    if proc.returncode != 0:
        raise BindingError(f"mvn install of ports/jagentic-core failed (exit {proc.returncode}); see {logs / 'jagentic-core-install.log'}")
    _core_installed = True


def run_maven_suite(suite: MavenSuite, fixtures: Dict[str, Dict[str, Any]], root: Path, logs: Path) -> RuntimeReport:
    report = RuntimeReport(suite.name, suite.binding(), "binding", RAN)
    if not _suite_present(root, suite):
        report.status, report.reason = NOT_TESTED, f"binding absent: no {suite.suite}.java under {suite.pom.rsplit('/', 1)[0] or '.'}"
        return report
    mvn = maven_command(root)
    if mvn is None:
        report.status, report.reason = NOT_TESTED, "toolchain unavailable: no ./mvnw in the checkout and mvn not on PATH"
        return report
    cmd = [mvn, "-B", "-ntp", "-f", suite.pom, "test", f"-Dtest={suite.suite}", "-Dsurefire.failIfNoSpecifiedTests=false"]
    report.command = " ".join(cmd[1:])
    started = _dt.datetime.now()
    try:
        if suite.needs_core_install:
            ensure_core_installed(root, logs)
        proc = _run(cmd, root, logs / f"{suite.name}.log")
        reports_dir = root / Path(suite.pom).parent / "target" / "surefire-reports"
        report.fixtures = parse_surefire(reports_dir, fixtures, suite.suite)
        if proc.returncode != 0 and not any(f.status == FAILED for f in report.fixtures):
            raise BindingError(f"mvn exited {proc.returncode} without a failing fixture; see {logs / (suite.name + '.log')}")
    except BindingError as exc:
        report.status, report.reason, report.fixtures = ERROR, str(exc), []
    report.duration_s = (_dt.datetime.now() - started).total_seconds()
    return report


CLOJURE_EXPR = """
(require '[agentic.conformance :as conf] '[clojure.data.json :as json])
(let [outcomes (conf/run-all)]
  (println "@@AGENTIC_CONFORMANCE@@")
  (println (json/write-str (mapv (fn [o] (update o :status name)) outcomes)))
  (println "@@END@@"))
(shutdown-agents)
"""


def parse_clojure_output(text: str) -> List[Dict[str, Any]]:
    if "@@AGENTIC_CONFORMANCE@@" not in text or "@@END@@" not in text:
        raise BindingError("agentic.conformance/run-all did not print its outcomes")
    payload = text.split("@@AGENTIC_CONFORMANCE@@", 1)[1].split("@@END@@", 1)[0].strip()
    return json.loads(payload)


def outcomes_from_clojure(outcomes: List[Dict[str, Any]], fixtures: Dict[str, Dict[str, Any]]) -> List[FixtureOutcome]:
    by_id = {o["id"]: o for o in outcomes}
    validator = result_validator()
    out: List[FixtureOutcome] = []
    for fixture in fixtures.values():
        o = by_id.get(fixture["id"])
        if o is None:
            raise BindingError(f"agentic.conformance reported nothing for fixture {fixture['id']}")
        if o["status"] == "skipped":
            reason = "; ".join(o.get("problems") or []) or "skipped by the binding"
            out.append(skip_outcome(fixture, missing_from_skip_reason(reason, fixture["requires"]), reason))
        else:
            out.append(outcome_from_results(fixture, o.get("results") or [], validator))
    return out


def run_clojure(fixtures: Dict[str, Dict[str, Any]], root: Path, logs: Path) -> RuntimeReport:
    report = RuntimeReport("clojure", "agentic-clj: agentic.conformance/run-all", "runner", RAN)
    module = root / "agentic-clj"
    if not (module / "src" / "agentic" / "conformance.clj").exists():
        report.status, report.reason = NOT_TESTED, "binding absent: agentic-clj/src/agentic/conformance.clj not found"
        return report
    if CLOJURE is None:
        report.status, report.reason = NOT_TESTED, "toolchain unavailable: clojure CLI not on PATH"
        return report
    cmd = [CLOJURE, "-M", "-e", CLOJURE_EXPR]
    report.command = "clojure -M -e <agentic.conformance/run-all as JSON>"
    started = _dt.datetime.now()
    log = logs / "clojure.log"
    try:
        proc = _run(cmd, module, log)
        text = log.read_text(encoding="utf-8")
        if proc.returncode != 0:
            raise BindingError(f"clojure exited {proc.returncode}; see {log}")
        report.fixtures = outcomes_from_clojure(parse_clojure_output(text), fixtures)
    except (BindingError, json.JSONDecodeError) as exc:
        report.status, report.reason, report.fixtures = ERROR, str(exc), []
    report.duration_s = (_dt.datetime.now() - started).total_seconds()
    return report


RUNTIMES = ["reference", "jvm-core", "flink", "pekko", "clojure", "python"]


def run_runtime(name: str, fixtures: Dict[str, Dict[str, Any]], root: Path, logs: Path,
                python_binding: Optional[str]) -> RuntimeReport:
    if name == "reference":
        return run_reference(fixtures)
    if name == "clojure":
        return run_clojure(fixtures, root, logs)
    if name == "python":
        return run_python_binding(fixtures, python_binding)
    return run_maven_suite(MAVEN_SUITES[name], fixtures, root, logs)


# --------------------------------------------------------------------------- matrix

def derive_capabilities(report: RuntimeReport) -> Dict[str, Dict[str, str]]:
    """Capability -> {value, note}, from outcomes alone."""
    cells: Dict[str, Dict[str, str]] = {}
    if report.status != RAN:
        note = report.reason or report.status
        return {c: {"value": "not_tested", "note": note} for c in CAPABILITIES}
    for cap in CAPABILITIES:
        relevant = [f for f in report.fixtures if cap in f.requires]
        if not relevant:
            cells[cap] = {"value": "not_tested", "note": "no v1 fixture requires it"}
            continue
        passed = [f.fixture for f in relevant if f.status == PASSED]
        failed = [f.fixture for f in relevant if f.status == FAILED]
        # A skip only counts against the capabilities the skip reason names; for the rest of the
        # fixture's `requires` it is inconclusive.
        skipped = [f.fixture for f in relevant if f.status == SKIPPED and (cap in f.missing or not f.missing)]
        inconclusive = [f.fixture for f in relevant if f.status == SKIPPED and f.fixture not in skipped]
        conclusive = len(relevant) - len(inconclusive)
        if conclusive == 0:
            cells[cap] = {"value": "not_tested", "note": f"only skipped fixtures require it: {inconclusive}"}
        elif len(passed) == conclusive:
            cells[cap] = {"value": "supported", "note": "passed " + ", ".join(passed)}
        elif passed:
            parts = []
            if failed:
                parts.append("failed " + ", ".join(failed))
            if skipped:
                parts.append("skipped " + ", ".join(skipped))
            cells[cap] = {"value": "partial", "note": "passed " + ", ".join(passed) + "; " + "; ".join(parts)}
        else:
            parts = []
            if failed:
                parts.append("failed " + ", ".join(failed))
            if skipped:
                parts.append("skipped " + ", ".join(skipped))
            cells[cap] = {"value": "unsupported", "note": "; ".join(parts)}
    return cells


def build_artifact(reports: List[RuntimeReport], fixtures: Dict[str, Dict[str, Any]], root: Path) -> Dict[str, Any]:
    git = shutil.which("git")
    commit = None
    if git:
        proc = subprocess.run([git, "rev-parse", "HEAD"], cwd=str(root), capture_output=True, text=True)
        commit = proc.stdout.strip() or None
    return {
        "spec_version": "agentic/v1",
        "generated_at": _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat(),
        "commit": commit,
        "host": {"platform": platform.platform(), "python": platform.python_version(),
                 "mvn": maven_command(root) is not None, "clojure": CLOJURE is not None},
        "capabilities": CAPABILITIES,
        "fixtures": [{"id": f["id"], "file": f["_file"], "requires": list(f["requires"])} for f in fixtures.values()],
        "runtimes": [r.to_dict() for r in reports],
    }


def render_docs(artifact: Dict[str, Any]) -> str:
    runtimes = artifact["runtimes"]
    names = [r["name"] for r in runtimes]
    lines: List[str] = []
    lines.append("# Capability matrix, agentic/v1")
    lines.append("")
    lines.append("Generated by `spec/tools/conformance_matrix.py` from a run of the shared conformance fixtures")
    lines.append("(`spec/conformance/v1/fixtures`). Do not edit by hand; regenerate with")
    lines.append("`python spec/tools/conformance_matrix.py --write-docs docs/capabilities.md`.")
    lines.append("")
    commit = artifact.get("commit") or "unknown"
    lines.append(f"Run: {artifact['generated_at']}, commit `{commit[:12]}`, {artifact['host']['platform']}.")
    lines.append("")
    lines.append("Every cell is derived from fixture outcomes, never declared (`spec/v1/primitives.md`, section 6):")
    lines.append("`supported` means every fixture requiring the capability passed; `partial` means some passed;")
    lines.append("`unsupported` means the runtime skipped or failed all of them; `not_tested` means no fixture")
    lines.append("requires it or the runtime did not run in this generation, for the reason given below.")
    lines.append("")
    lines.append("## Runtimes")
    lines.append("")
    lines.append("| Runtime | Binding | Compared | Status | Passed | Failed | Skipped |")
    lines.append("|---|---|---|---|---:|---:|---:|")
    for r in runtimes:
        counts = {s: sum(1 for f in r["fixtures"] if f["status"] == s) for s in (PASSED, FAILED, SKIPPED)}
        where = "by the runner on the binding's normalized results" if r["comparison"] == "runner" else "inside the binding (ported comparator), read from JUnit XML"
        status = r["status"] if r["status"] == RAN else f"{r['status']}: {r.get('reason', '')}"
        lines.append(f"| {r['name']} | `{r['binding']}` | {where} | {status} | {counts[PASSED]} | {counts[FAILED]} | {counts[SKIPPED]} |")
    lines.append("")
    lines.append("## Capabilities")
    lines.append("")
    lines.append("| Capability | " + " | ".join(names) + " |")
    lines.append("|---|" + "|".join("---" for _ in names) + "|")
    for cap in artifact["capabilities"]:
        cells = [r["capabilities"][cap]["value"] for r in runtimes]
        lines.append(f"| `{cap}` | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("## Fixtures")
    lines.append("")
    lines.append("| Fixture | Requires | " + " | ".join(names) + " |")
    lines.append("|---|---|" + "|".join("---" for _ in names) + "|")
    by_runtime = {r["name"]: {f["fixture"]: f for f in r["fixtures"]} for r in runtimes}
    for fx in artifact["fixtures"]:
        cells = []
        for r in runtimes:
            if r["status"] != RAN:
                cells.append("not run")
                continue
            f = by_runtime[r["name"]].get(fx["id"])
            cells.append(f["status"] if f else "not run")
        lines.append(f"| `{fx['id']}` | {', '.join(fx['requires'])} | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    required = {c for fx in artifact["fixtures"] for c in fx["requires"]}
    uncovered = [c for c in artifact["capabilities"] if c not in required]
    if uncovered:
        lines.append("No v1 fixture requires " + ", ".join(f"`{c}`" for c in uncovered)
                     + ": these are `not_tested` for every runtime until a fixture covers them.")
        lines.append("")
    for r in runtimes:
        lines.append(f"### {r['name']}")
        lines.append("")
        if r["status"] != RAN:
            lines.append(f"- {r['status']}: {r.get('reason', '')}")
        else:
            notes = 0
            for cap in artifact["capabilities"]:
                cell = r["capabilities"][cap]
                if cell["value"] != "supported" and cap in required:
                    lines.append(f"- `{cap}` {cell['value']}: {cell['note']}")
                    notes += 1
            for f in r["fixtures"]:
                if f["status"] == FAILED:
                    lines.append(f"- `{f['fixture']}` failed: " + "; ".join(f.get("problems", [])))
                    notes += 1
                elif f["status"] == SKIPPED:
                    lines.append(f"- `{f['fixture']}` skipped: {f.get('skip_reason', '')}")
                    notes += 1
            if not notes:
                lines.append("- every fixture passed")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def print_summary(reports: List[RuntimeReport]) -> None:
    for r in reports:
        if r.status != RAN:
            print(f"{r.name}: {r.status}: {r.reason}")
            continue
        for f in r.fixtures:
            tag = {PASSED: "pass", FAILED: "FAIL", SKIPPED: "skip"}[f.status]
            extra = f": {f.skip_reason}" if f.status == SKIPPED else ""
            print(f"{r.name}: {tag} {f.fixture}{extra}")
            for p in f.problems:
                print(f"    {p}")
        counts = {s: sum(1 for f in r.fixtures if f.status == s) for s in (PASSED, FAILED, SKIPPED)}
        print(f"{r.name}: {counts[PASSED]} passed, {counts[FAILED]} failed, {counts[SKIPPED]} skipped")


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--runtimes", nargs="+", choices=RUNTIMES, default=RUNTIMES)
    ap.add_argument("--require", action="append", default=[], choices=RUNTIMES,
                    help="fail if this runtime could not run (absent binding or toolchain)")
    ap.add_argument("--require-present", action="store_true",
                    help="fail if any runtime whose binding exists in the checkout could not run (missing toolchain)")
    ap.add_argument("--root", type=Path, default=ROOT, help="repository root holding the bindings")
    ap.add_argument("--output", type=Path, help="write the JSON artifact here")
    ap.add_argument("--write-docs", type=Path, help="render docs/capabilities.md-style markdown here")
    ap.add_argument("--render", type=Path, help="render docs from an existing artifact instead of running")
    ap.add_argument("--logs", type=Path, help="directory for binding logs (default: <root>/build/conformance-logs)")
    ap.add_argument("--python-binding", help="module:callable of a Python binding")
    args = ap.parse_args(argv)

    if args.render:
        artifact = json.loads(args.render.read_text(encoding="utf-8"))
        if not args.write_docs:
            ap.error("--render needs --write-docs")
        args.write_docs.write_text(render_docs(artifact), encoding="utf-8")
        print(f"wrote {args.write_docs}")
        return 0

    declared = capability_ids_from_spec()
    if declared != CAPABILITIES:
        print(f"capability ids drifted from spec/v1/primitives.md: {declared} vs {CAPABILITIES}")
        return 1

    root = args.root.resolve()
    logs = (args.logs or root / "build" / "conformance-logs").resolve()
    fixtures = fixture_index()
    reports = [run_runtime(name, fixtures, root, logs, args.python_binding) for name in args.runtimes]
    print_summary(reports)

    artifact = build_artifact(reports, fixtures, root)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {args.output}")
    if args.write_docs:
        args.write_docs.parent.mkdir(parents=True, exist_ok=True)
        args.write_docs.write_text(render_docs(artifact), encoding="utf-8")
        print(f"wrote {args.write_docs}")

    rc = 0
    for r in reports:
        if r.status == ERROR or any(f.status == FAILED for f in r.fixtures):
            rc = 1
        if r.status == NOT_TESTED and (r.name in args.require or (
                args.require_present and not (r.reason or "").startswith("binding absent"))):
            print(f"required runtime {r.name} did not run: {r.reason}")
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
