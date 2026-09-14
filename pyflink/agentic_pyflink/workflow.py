"""Loading the portable workflow document (``spec/v1/workflow.schema.json``).

The document is the same YAML/JSON file the JVM, Pekko and Clojure runtimes load
(``examples/pipelines/*.yaml``, ``spec/conformance/v1/workflows/support.yaml``); this module only
parses it. Structural validation happens on the JVM side (``WorkflowValidator``) when the job is
built, so there is exactly one set of rules.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from ._contract import required_capabilities as _required_capabilities

SPEC_VERSION = "agentic/v1"

Workflow = dict[str, Any]


class WorkflowLoadError(ValueError):
    """The file is not a workflow document this runtime can hand to the core."""


def load_workflow(path: str | Path) -> Workflow:
    """Parse a ``.yaml``/``.yml``/``.json`` workflow document into a plain mapping."""
    p = Path(path)
    if not p.is_file():
        raise WorkflowLoadError(f"workflow file not found: {p}")
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() == ".json":
        doc = json.loads(text)
    elif p.suffix.lower() in (".yaml", ".yml"):
        doc = yaml.safe_load(text)
    else:
        raise WorkflowLoadError(f"unsupported workflow file type {p.suffix!r} (use .yaml, .yml or .json)")
    return as_workflow(doc)


def as_workflow(doc: Any) -> Workflow:
    """Accept a mapping (or an object exposing ``to_dict()``) and reject the wrong spec version."""
    if hasattr(doc, "to_dict") and not isinstance(doc, Mapping):
        doc = doc.to_dict()
    if not isinstance(doc, Mapping):
        raise WorkflowLoadError(f"workflow document must be a mapping, got {type(doc).__name__}")
    version = doc.get("spec_version")
    if version is not None and version != SPEC_VERSION:
        raise WorkflowLoadError(f"unsupported spec_version {version!r} (this runtime implements {SPEC_VERSION})")
    return dict(doc)


def workflow_json(spec: Mapping[str, Any]) -> str:
    return json.dumps(spec, separators=(",", ":"))


def required_capabilities(spec: Mapping[str, Any]) -> list[str]:
    """Capability ids (``spec/v1/primitives.md`` section 6) a document needs from its runtime.

    Delegates to the canonical derivation in ``agentic.runtime`` (``ports/pyagentic``) through
    :mod:`agentic_pyflink._contract`; every Python binding therefore reports the same list for
    the same document.
    """
    return list(_required_capabilities(spec))
