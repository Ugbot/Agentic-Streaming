#!/usr/bin/env python3
"""Fail a CI job when a test was skipped for a reason that is not on the allowlist.

Service-backed tests in this repository (Postgres, Redis/Valkey, Kafka, Qdrant, a Python
interpreter with the MCP SDK, Testcontainers) skip themselves with JUnit ``Assumptions`` or
``pytest.skip`` when the backing service is not reachable. On a developer box that is the
right behaviour; in CI it hides a missing service behind a green build. Instead of editing
every test, the CI job runs the suites with the services provisioned and then runs this
script over the JUnit XML reports (surefire ``TEST-*.xml`` and pytest ``--junitxml``). Every
``<skipped>`` element whose message does not match one of the allowlisted patterns fails the
job, so a skip in CI is only possible for the reasons written down in the allowlist.

The allowlist is a text file of one regular expression per line; blank lines and ``#``
comments are ignored, and an inline ``  # reason`` documents why the skip is legitimate.
Conformance fixtures that a runtime declares as unsupported are the canonical allowed skip.

Usage:
    skip_audit.py --allowlist tools/ci/skip-allowlist.txt 'reports/**/TEST-*.xml' ...

Exit status: 0 when every skip is allowlisted, 1 when at least one is not, 2 when the report
globs match no file (a wrong glob would otherwise pass silently).
"""

from __future__ import annotations

import argparse
import glob
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


# Surefire 3 writes an aborted assumption as element text: "org.opentest4j.TestAbortedException:
# <reason>\n\tat ...", and JUnit prefixes assumeTrue(...) reasons with "Assumption failed: ".
# Only the bare reason is matched against the allowlist.
_REASON_PREFIX = re.compile(r"^(?:[\w.$]+(?:Exception|Error): )?(?:Assumption failed: )?")


@dataclass(frozen=True)
class Skip:
    report: Path
    test: str
    message: str

    def __str__(self) -> str:
        return f"{self.test} [{self.report}]: {self.message or '(no message)'}"


def load_allowlist(path: Path) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.split("  #", 1)[0].strip() if not raw.lstrip().startswith("#") else ""
        if not line:
            continue
        try:
            patterns.append(re.compile(line))
        except re.error as exc:
            raise SystemExit(f"{path}:{lineno}: invalid regular expression {line!r}: {exc}") from exc
    return patterns


def skipped_tests(report: Path) -> list[Skip]:
    try:
        root = ET.parse(report).getroot()
    except ET.ParseError as exc:
        raise SystemExit(f"{report}: not a JUnit XML report: {exc}") from exc
    skips: list[Skip] = []
    for case in root.iter("testcase"):
        for skipped in case.findall("skipped"):
            name = ".".join(p for p in (case.get("classname"), case.get("name")) if p)
            skips.append(Skip(report, name, skip_message(skipped)))
    return skips


def skip_message(skipped: ET.Element) -> str:
    message = skipped.get("message")
    if message is None:
        lines = [line.strip() for line in (skipped.text or "").splitlines() if line.strip()]
        message = lines[0] if lines else ""
    return _REASON_PREFIX.sub("", message.strip(), count=1)


def collect_reports(globs: Sequence[str]) -> list[Path]:
    found: set[Path] = set()
    for pattern in globs:
        found.update(Path(p) for p in glob.glob(pattern, recursive=True) if Path(p).is_file())
    return sorted(found)


def audit(reports: Iterable[Path], allowlist: Sequence[re.Pattern[str]]) -> tuple[list[Skip], list[Skip]]:
    allowed: list[Skip] = []
    unexpected: list[Skip] = []
    for report in reports:
        for skip in skipped_tests(report):
            (allowed if any(p.search(skip.message) for p in allowlist) else unexpected).append(skip)
    return allowed, unexpected


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--allowlist", type=Path, required=True, help="file of allowed skip-message regexes")
    parser.add_argument("reports", nargs="+", help="glob(s) of JUnit XML reports; ** is supported")
    args = parser.parse_args(argv)

    allowlist = load_allowlist(args.allowlist)
    reports = collect_reports(args.reports)
    if not reports:
        print(f"skip-audit: no JUnit XML reports matched {args.reports}", file=sys.stderr)
        return 2

    allowed, unexpected = audit(reports, allowlist)
    cases = sum(1 for r in reports for _ in ET.parse(r).getroot().iter("testcase"))
    print(f"skip-audit: {len(reports)} report(s), {cases} test case(s), "
          f"{len(allowed)} allowlisted skip(s), {len(unexpected)} unexpected skip(s)")
    for skip in allowed:
        print(f"  allowed    {skip}")
    for skip in unexpected:
        print(f"  UNEXPECTED {skip}")
    if unexpected:
        print("skip-audit: a test skipped for a reason that is not allowlisted; in CI the backing "
              "service or dependency must be provided (or the reason documented in the allowlist).",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
