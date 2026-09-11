"""The workflow IR: loading YAML or JSON documents and validating them.

Validation is the JSON Schema in `schemas/workflow.schema.json` (a byte-identical copy of
`spec/v1/workflow.schema.json`, kept in step by a test) plus the cross-field rules of
`spec/README.md` that a schema cannot express. Unknown keys are rejected everywhere except
`x-*` keys and the `runtime:` block; the schema already encodes that policy.
"""

from __future__ import annotations

import copy
import json
from importlib import resources
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple, Union

import yaml
from jsonschema import Draft202012Validator

from .errors import ValidationError

SPEC_VERSION = "agentic/v1"
Document = Dict[str, Any]
Source = Union[str, Path, Mapping[str, Any]]


def _schema(name: str) -> Dict[str, Any]:
    text = resources.files(__package__).joinpath("schemas").joinpath(name).read_text(encoding="utf-8")
    loaded: Dict[str, Any] = json.loads(text)
    return loaded


WORKFLOW_SCHEMA: Dict[str, Any] = _schema("workflow.schema.json")
RESULT_SCHEMA: Dict[str, Any] = _schema("result.schema.json")
_WORKFLOW_VALIDATOR = Draft202012Validator(WORKFLOW_SCHEMA)
_RESULT_VALIDATOR = Draft202012Validator(RESULT_SCHEMA)


def read_document(source: Source) -> Document:
    """Read a workflow document from a path (YAML or JSON), a YAML/JSON string, or a mapping.

    Mappings are deep-copied so later mutation by the caller cannot leak into a spec.
    """
    if isinstance(source, Mapping):
        return copy.deepcopy(dict(source))
    if isinstance(source, Path) or (isinstance(source, str) and _looks_like_path(source)):
        path = Path(source)
        if not path.exists():
            raise ValidationError(f"workflow file not found: {path}")
        text = path.read_text(encoding="utf-8")
        doc = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    else:
        doc = yaml.safe_load(source)
    if not isinstance(doc, dict):
        raise ValidationError(f"a workflow document must be a mapping, got {type(doc).__name__}")
    return doc


def _looks_like_path(text: str) -> bool:
    stripped = text.strip()
    if "\n" in stripped or stripped.startswith("{"):
        return False
    return stripped.endswith((".yaml", ".yml", ".json")) or Path(stripped).exists()


def validate_document(doc: Mapping[str, Any]) -> Document:
    """Validate a document against the schema and the cross-field rules.

    Returns a normalized deep copy with `spec_version` filled in. Raises `ValidationError`
    with the JSON Pointer of the first offending key.
    """
    version = doc.get("spec_version", SPEC_VERSION)
    if version != SPEC_VERSION:
        raise ValidationError(
            f"unsupported spec_version {version!r}; this runtime reads {SPEC_VERSION} only",
            "/spec_version",
        )
    errors = sorted(_WORKFLOW_VALIDATOR.iter_errors(doc), key=lambda e: list(e.absolute_path))
    if errors:
        err = errors[0]
        raise ValidationError(err.message, _pointer(err.absolute_path))

    normalized: Document = copy.deepcopy(dict(doc))
    normalized["spec_version"] = SPEC_VERSION
    for pointer, message in _cross_field_problems(normalized):
        raise ValidationError(message, pointer)
    return normalized


def validate_result(result: Mapping[str, Any]) -> None:
    """Raise `ValidationError` unless `result` is a valid normalized turn result."""
    errors = sorted(_RESULT_VALIDATOR.iter_errors(result), key=lambda e: list(e.absolute_path))
    if errors:
        raise ValidationError(errors[0].message, _pointer(errors[0].absolute_path))


def _pointer(parts: Any) -> str:
    return "/" + "/".join(str(p) for p in parts)


def tool_ids(doc: Mapping[str, Any]) -> Dict[str, str]:
    """Every registered tool id with the pointer that declares it, across tools, mcp, and a2a."""
    ids: Dict[str, str] = {}
    for i, tool in enumerate(doc.get("tools") or []):
        ids.setdefault(tool["id"], f"/tools/{i}/id")
    for i, server in enumerate(doc.get("mcp") or []):
        for j, tool in enumerate(server.get("tools") or []):
            name = tool if isinstance(tool, str) else tool.get("id") or tool.get("name")
            if name:
                ids.setdefault(name, f"/mcp/{i}/tools/{j}")
    for i, peer in enumerate(doc.get("a2a") or []):
        ids.setdefault(peer.get("name") or peer["id"], f"/a2a/{i}")
    return ids


def _cross_field_problems(doc: Document) -> Iterator[Tuple[str, str]]:
    agent = doc["agent"]
    paths: Dict[str, Any] = agent["paths"]
    router = agent.get("router") or {}

    for name in (router.get("rules") or {}):
        if name not in paths:
            yield f"/agent/router/rules/{name}", f"router rule names unknown path {name!r}"
    default = router.get("default")
    if default is not None and default not in paths:
        yield "/agent/router/default", f"router.default names unknown path {default!r}"

    registered = _registered_ids(doc)
    if registered is None:
        return
    known = set(registered)

    for name, path in paths.items():
        for i, tool in enumerate(path.get("tools") or []):
            if tool not in known:
                yield f"/agent/paths/{name}/tools/{i}", f"path {name!r} names unregistered tool {tool!r}"
        for keyword, tool in (path.get("tool_triggers") or {}).items():
            if tool not in known:
                yield (f"/agent/paths/{name}/tool_triggers/{keyword}",
                       f"tool trigger {keyword!r} names unregistered tool {tool!r}")
        skills = {s["name"]: s for s in doc.get("skills") or []}
        for i, skill in enumerate(path.get("skills") or []):
            if skill not in skills:
                yield f"/agent/paths/{name}/skills/{i}", f"path {name!r} names unknown skill {skill!r}"
        if path.get("brain") == "llm" and "llm" not in doc:
            yield f"/agent/paths/{name}/brain", f"path {name!r} has brain: llm but the document has no llm block"
        names = {g.get("name") for g in doc.get("guardrails") or [] if g.get("name")}
        for i, rail in enumerate(path.get("guardrails") or []):
            if rail not in names:
                yield f"/agent/paths/{name}/guardrails/{i}", f"path {name!r} names unknown guardrail {rail!r}"

    for i, skill in enumerate(doc.get("skills") or []):
        for j, tool in enumerate(skill.get("tools") or []):
            if tool not in known:
                yield f"/skills/{i}/tools/{j}", f"skill {skill['name']!r} names unregistered tool {tool!r}"

    saga = doc.get("saga")
    if saga:
        for i, step in enumerate(saga["steps"]):
            if step["tool"] not in known:
                yield f"/saga/steps/{i}/tool", f"saga step names unregistered tool {step['tool']!r}"
            undo = step.get("compensate_with")
            if undo is not None and undo not in known:
                yield f"/saga/steps/{i}/compensate_with", f"compensate_with names unregistered tool {undo!r}"
    for i, tool in enumerate(doc.get("tools") or []):
        undo = tool.get("compensation")
        if undo is not None and undo not in known:
            yield f"/tools/{i}/compensation", f"compensation names unregistered tool {undo!r}"

    for i, timer in enumerate(doc.get("timers") or []):
        tool = timer.get("tool")
        if tool is not None and tool not in known:
            yield f"/timers/{i}/tool", f"timer names unregistered tool {tool!r}"

    retrieval = doc.get("retrieval") or {}
    embeddings = doc.get("embeddings") or {}
    if "dim" in retrieval and "dim" in embeddings and retrieval["dim"] != embeddings["dim"]:
        yield "/retrieval/dim", f"retrieval.dim {retrieval['dim']} disagrees with embeddings.dim {embeddings['dim']}"


def _registered_ids(doc: Document) -> Optional[List[str]]:
    """Tool ids in declaration order, or None after reporting a duplicate."""
    seen: Dict[str, str] = {}
    for i, tool in enumerate(doc.get("tools") or []):
        pointer = f"/tools/{i}/id"
        if tool["id"] in seen:
            raise ValidationError(f"duplicate tool id {tool['id']!r} (also {seen[tool['id']]})", pointer)
        seen[tool["id"]] = pointer
    for i, server in enumerate(doc.get("mcp") or []):
        for j, tool in enumerate(server.get("tools") or []):
            name = tool if isinstance(tool, str) else tool.get("id") or tool.get("name")
            if not name:
                continue
            pointer = f"/mcp/{i}/tools/{j}"
            if name in seen:
                raise ValidationError(f"duplicate tool id {name!r} (also {seen[name]})", pointer)
            seen[name] = pointer
    for i, peer in enumerate(doc.get("a2a") or []):
        name = peer.get("name") or peer["id"]
        pointer = f"/a2a/{i}"
        if name in seen:
            raise ValidationError(f"duplicate tool id {name!r} (also {seen[name]})", pointer)
        seen[name] = pointer
    return list(seen)


def dumps(doc: Mapping[str, Any], fmt: str = "yaml") -> str:
    if fmt == "json":
        return json.dumps(doc, indent=2, sort_keys=False)
    if fmt == "yaml":
        return str(yaml.safe_dump(dict(doc), sort_keys=False))
    raise ValidationError(f"unknown format {fmt!r}; use 'yaml' or 'json'")
