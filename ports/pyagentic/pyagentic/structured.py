"""Structured output — validate an LLM's final answer against a small JSON-schema-lite
contract. Portable analogue of the Flink ``OutputSchema``: no external deps, just enough
to enforce ``{type: object, required: [...], properties: {field: {type: ...}}}``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

_PY_TYPES = {
    "string": str,
    "number": (int, float),
    "integer": int,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def validate(value: Any, schema: Dict[str, Any]) -> List[str]:
    """Return a list of validation errors (empty == valid)."""
    errors: List[str] = []
    expected = schema.get("type", "object")
    py = _PY_TYPES.get(expected)
    if py and not isinstance(value, py):
        return [f"expected {expected}, got {type(value).__name__}"]
    if expected == "object" and isinstance(value, dict):
        for req in schema.get("required", []):
            if req not in value:
                errors.append(f"missing required field {req!r}")
        for field, sub in (schema.get("properties") or {}).items():
            if field in value:
                errors.extend(f"{field}.{e}" for e in validate(value[field], sub))
    return errors


def parse_structured(text: str, schema: Dict[str, Any]) -> Tuple[Optional[dict], List[str]]:
    """Parse ``text`` as one JSON object (tolerant of fences/prose) and validate it
    against ``schema``. Returns (object|None, errors)."""
    s = (text or "").strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end <= start:
        return None, ["no JSON object found in output"]
    try:
        obj = json.loads(s[start : end + 1])
    except json.JSONDecodeError as exc:
        return None, [f"invalid JSON: {exc}"]
    return obj, validate(obj, schema)


def schema_instruction(schema: Dict[str, Any]) -> str:
    """A system-prompt fragment telling the model to answer with conforming JSON."""
    return ("Respond with a single JSON object conforming to this schema: "
            + json.dumps(schema))
