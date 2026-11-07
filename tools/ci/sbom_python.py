#!/usr/bin/env python3
"""A CycloneDX 1.5 SBOM of what a `uv.lock` resolves, with no dependency beyond the standard library.

    python tools/ci/sbom_python.py ports/pyagentic/uv.lock target/sbom-pyagentic.json
"""

from __future__ import annotations

import json
import pathlib
import sys
import tomllib
import uuid
from typing import Any, Dict


def sbom(lock_text: str, name: str) -> Dict[str, Any]:
    packages = [p for p in tomllib.loads(lock_text).get("package", []) if "version" in p]
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {"component": {"type": "application", "name": name}},
        "components": [{"type": "library", "name": p["name"], "version": p["version"],
                        "purl": f"pkg:pypi/{p['name'].lower()}@{p['version']}"}
                       for p in sorted(packages, key=lambda p: p["name"].lower())],
    }


def main(argv: list) -> int:
    lock, out = pathlib.Path(argv[0]), pathlib.Path(argv[1])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(sbom(lock.read_text(encoding="utf-8"), lock.parent.name), indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
