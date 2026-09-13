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


def required_capabilities(spec: Mapping[str, Any]) -> set[str]:
    """Capability ids (``spec/v1/primitives.md`` §6) a document needs from its runtime.

    Derived from which sections and kinds the document uses; a plain router+paths document needs
    only ``routing``, ``rule_brain`` and ``memory`` (every turn appends to the transcript).
    """
    needs: set[str] = {"routing", "rule_brain", "memory"}
    agent = spec.get("agent") or {}
    paths = agent.get("paths") or {}
    for path in paths.values():
        if not isinstance(path, Mapping):
            continue
        if path.get("brain") == "llm":
            needs.add("llm_brain")
        if path.get("tools") or path.get("tool_triggers"):
            needs.add("tools")
        if path.get("x-suspend-until"):
            needs.add("suspend_resume")
        if path.get("verifier"):
            needs.add("verifier")
    router = agent.get("router") or {}
    if router.get("kind") == "llm":
        needs.add("llm_brain")
    verifier = agent.get("verifier") or {}
    if verifier and verifier.get("kind") not in (None, "none"):
        needs.add("verifier")
    if spec.get("tools"):
        needs.add("tools")
        if any(isinstance(t, Mapping) and t.get("compensation") for t in spec["tools"]):
            needs.add("saga")
    if spec.get("guardrails"):
        needs.add("guardrails")
    policies = spec.get("policies") or {}
    if policies.get("retry"):
        needs.add("retry")
    if policies.get("ordering") == "per-conversation":
        needs.add("ordering")
    if policies.get("idempotency") == "turn-id":
        needs.add("idempotency")
    for section, capability in (
        ("retrieval", "retrieval"),
        ("context", "context_window"),
        ("saga", "saga"),
        ("a2a", "a2a"),
        ("cep", "cep"),
        ("timers", "timers"),
        ("stores", "durable_store"),
    ):
        if spec.get(section):
            needs.add(capability)
    return needs
