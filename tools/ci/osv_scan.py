#!/usr/bin/env python3
"""Scan every resolved dependency against OSV, and fail on an advisory nobody has looked at.

    python tools/ci/osv_scan.py --deps-from-maven --out target/osv.json
    python tools/ci/osv_scan.py --deps maven-deps.txt --out target/osv.json   # a list made earlier

The Maven side is `dependency:list` over the reactor at test scope, so it sees what the tests run; the
Python side is both `uv.lock` files. An advisory listed in `tools/ci/osv-allowlist.txt` with a reason
is reported and passes; any other fails the run. docs/security.md explains the ones listed. OSV
matches coordinates, so it cannot see into a shaded jar.
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import subprocess
import sys
import tomllib
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Iterable, List, Mapping, Set, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULES = ("ports/jagentic-core", "agentic-gateway", ".", "agentic-pekko")
LOCKS = ("ports/pyagentic/uv.lock", "ports/agentic-pipeline/uv.lock")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
Package = Tuple[str, str, str]  # ecosystem, name, version


def maven_lines(root: pathlib.Path) -> List[str]:
    run = subprocess.run(["./mvnw", "-B", "-q", "-Dstyle.color=never", "-f", "reactor/pom.xml", "dependency:list",
                          "-DoutputFile=target/deps.txt", "-DincludeScope=test"],
                         cwd=root, capture_output=True, text=True, check=False)
    if run.returncode:
        sys.exit(f"dependency:list failed:\n{run.stdout[-3000:]}{run.stderr[-1000:]}")
    lines: List[str] = []
    for module in MODULES:
        lines += (root / module / "target" / "deps.txt").read_text(encoding="utf-8").splitlines()
    return lines


def parse_maven(lines: Iterable[str]) -> Set[Package]:
    """`group:artifact:type[:classifier]:version:scope` lines, the reactor's own modules left out."""
    found: Set[Package] = set()
    for line in lines:
        parts = ANSI.sub("", line).strip().split(" -- ")[0].strip().split(":")
        if len(parts) < 5 or parts[0] == "org.jagentic":
            continue
        found.add(("Maven", f"{parts[0]}:{parts[1]}", parts[-2]))
    return found


def parse_lock(text: str) -> Set[Package]:
    return {("PyPI", p["name"], p["version"]) for p in tomllib.loads(text).get("package", []) if "version" in p}


def allowlist(text: str) -> Dict[str, str]:
    """`ID  reason` per line; a line with no reason is a mistake, not an exemption."""
    entries: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        ident, _, reason = line.partition(" ")
        if not reason.strip():
            sys.exit(f"osv allowlist entry {ident} has no reason")
        entries[ident] = reason.strip()
    return entries


def post(url: str, body: Mapping[str, object]) -> Dict[str, object]:
    request = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def advisories(packages: List[Package], query: Callable[[str, Mapping[str, object]], Dict[str, object]] = post
               ) -> Dict[Package, Set[str]]:
    found: Dict[Package, Set[str]] = collections.defaultdict(set)
    for i in range(0, len(packages), 500):
        chunk = packages[i:i + 500]
        answer = query("https://api.osv.dev/v1/querybatch", {"queries": [
            {"package": {"ecosystem": e, "name": n}, "version": v} for e, n, v in chunk]})
        for package, result in zip(chunk, answer["results"]):  # type: ignore[index]
            if result.get("next_page_token"):
                sys.exit(f"OSV paged the answer for {package}; this scan does not follow pages")
            for vuln in result.get("vulns") or []:
                found[package].add(vuln["id"])
    return found


def verdict(found: Mapping[Package, Set[str]], aliases: Mapping[str, Set[str]],
            allowed: Mapping[str, str]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    """Every advisory once per package, split into those allowed with a reason and those not."""
    passed: List[Dict[str, object]] = []
    failed: List[Dict[str, object]] = []
    for package, ids in sorted(found.items()):
        seen: Set[str] = set()
        for ident in sorted(ids):
            names = aliases.get(ident, set()) | {ident}
            if names & seen:
                continue
            seen |= names
            reason = next((allowed[n] for n in sorted(names) if n in allowed), None)
            row = {"ecosystem": package[0], "package": package[1], "version": package[2], "id": ident,
                   "aliases": sorted(names - {ident}), "reason": reason}
            (passed if reason else failed).append(row)
    return passed, failed


def aliases_of(ids: Iterable[str]) -> Dict[str, Set[str]]:
    def one(ident: str) -> Tuple[str, Set[str]]:
        with urllib.request.urlopen(f"https://api.osv.dev/v1/vulns/{ident}", timeout=60) as response:
            return ident, set(json.load(response).get("aliases") or [])
    with ThreadPoolExecutor(8) as pool:
        return dict(pool.map(one, sorted(set(ids))))


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--deps-from-maven", action="store_true")
    source.add_argument("--deps", type=pathlib.Path, help="dependency:list output, every module concatenated")
    parser.add_argument("--allowlist", type=pathlib.Path, default=ROOT / "tools/ci/osv-allowlist.txt")
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)

    lines = maven_lines(ROOT) if args.deps_from_maven else args.deps.read_text(encoding="utf-8").splitlines()
    packages = parse_maven(lines)
    for lock in LOCKS:
        packages |= parse_lock((ROOT / lock).read_text(encoding="utf-8"))
    found = advisories(sorted(packages))
    passed, failed = verdict(found, aliases_of(i for s in found.values() for i in s),
                             allowlist(args.allowlist.read_text(encoding="utf-8")))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"packages": len(packages), "allowed": passed, "failed": failed}, indent=1))
    print(f"{len(packages)} packages, {len(passed)} advisories allowed with a reason, {len(failed)} not")
    for row in failed:
        print(f"FAIL {row['package']} {row['version']}: {row['id']} {' '.join(row['aliases'])}")  # type: ignore[arg-type]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
