"""Takes what must not be recorded out of a value, before it reaches the log or the journal.

The Python peer of `org.jagentic.core.Redactor`. Credentials are always covered, by field name and by
shape; `policies.redact` adds a workflow's own fields and patterns, such as personal data. A pattern finds
what it describes and nothing else, so this narrows what is recorded and does not promise it is clean.
"""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Pattern, Set

MARK = "[redacted]"

SECRET_FIELDS = frozenset({"password", "passwd", "secret", "token", "api_key", "apikey", "authorization",
                           "credential", "credentials", "private_key"})
SECRET_SHAPES = (
    re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]{8,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
)


class Redactor:
    def __init__(self, doc: Mapping[str, Any]) -> None:
        block = (doc.get("policies") or {}).get("redact") or {}
        self.fields: Set[str] = {str(f).lower() for f in block.get("fields") or []}
        self.patterns: List[Pattern[str]] = [re.compile(p) for p in block.get("patterns") or []]

    def apply(self, value: Any) -> Any:
        """A copy of `value` with what must not be recorded taken out."""
        if isinstance(value, str):
            for pattern in (*SECRET_SHAPES, *self.patterns):
                value = pattern.sub(MARK, value)
            return value
        if isinstance(value, Mapping):
            return {k: MARK if self._named(str(k)) else self.apply(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.apply(v) for v in value]
        return value

    def _named(self, field: str) -> bool:
        name = field.lower()
        return name in SECRET_FIELDS or name in self.fields
