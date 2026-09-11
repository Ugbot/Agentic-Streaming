#!/usr/bin/env python3
"""Validate workflow documents and conformance fixtures against the v1 schemas.

    python spec/tools/validate_spec.py                 # spec/ + examples/pipelines
    python spec/tools/validate_spec.py path/to/spec.yaml

Exits non-zero on the first invalid document, printing the JSON Pointer of the failure.
"""

from __future__ import annotations

import sys
from pathlib import Path

import json
import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "spec"
WORKFLOW_SCHEMA = SPEC / "v1" / "workflow.schema.json"
FIXTURE_SCHEMA = SPEC / "conformance" / "v1" / "fixture.schema.json"


def load(path: Path):
    text = path.read_text(encoding="utf-8")
    return json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)


def validator(schema_path: Path) -> Draft202012Validator:
    return Draft202012Validator(load(schema_path))


def report(path: Path, errors) -> int:
    failures = 0
    for err in errors:
        pointer = "/".join(str(p) for p in err.absolute_path)
        print(f"{path.relative_to(ROOT)}: /{pointer}: {err.message}")
        failures += 1
    return failures


def check_fixture(path: Path, doc, wf_validator) -> int:
    """A fixture's referenced or inline workflow must itself be a valid workflow, and every
    expectation must name a turn the fixture actually delivers."""
    failures = 0
    workflow = doc.get("workflow")
    if workflow is None:
        ref = (path.parent / doc["workflow_ref"]).resolve()
        if not ref.exists():
            print(f"{path.relative_to(ROOT)}: workflow_ref does not exist: {doc['workflow_ref']}")
            return 1
        workflow = load(ref)
    failures += report(path, wf_validator.iter_errors(workflow))

    turn_ids = {t["turn_id"] for t in doc["turns"]}
    for exp in doc["expect"]:
        if exp["turn_id"] not in turn_ids:
            print(f"{path.relative_to(ROOT)}: expectation for unknown turn_id {exp['turn_id']}")
            failures += 1
    return failures


def main(argv: list[str]) -> int:
    wf = validator(WORKFLOW_SCHEMA)
    fx = validator(FIXTURE_SCHEMA)

    if argv:
        targets = [Path(a).resolve() for a in argv]
    else:
        targets = sorted(
            list((SPEC / "conformance" / "v1" / "fixtures").glob("*.yaml"))
            + list((SPEC / "conformance" / "v1" / "workflows").glob("*.yaml"))
            + list((ROOT / "examples" / "pipelines").glob("*.yaml"))
        )

    failures = 0
    checked = 0
    for path in targets:
        doc = load(path)
        if not isinstance(doc, dict):
            print(f"{path}: not a mapping")
            failures += 1
            continue
        checked += 1
        if "expect" in doc or "turns" in doc:
            failures += report(path, fx.iter_errors(doc))
            if not list(fx.iter_errors(doc)):
                failures += check_fixture(path, doc, wf)
        else:
            failures += report(path, wf.iter_errors(doc))

    print(f"checked {checked} document(s), {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
