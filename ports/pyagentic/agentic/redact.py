"""Takes what must not be recorded out of a value, before it reaches the log or the journal.

The Python peer of `org.jagentic.core.Redactor`. Credentials are always covered, by field name and by
shape; `policies.redact` adds a workflow's own fields and patterns, such as personal data. A pattern finds
what it describes and nothing else, so this narrows what is recorded and does not promise it is clean.
"""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Pattern, Set

MARK = "[redacted]"

# A field is a credential when one of these is a whole word of its name, so `access_token`,
# `client_secret`, `x-api-key` and `refreshToken` are, and `max_tokens` is not.
SECRET_FIELDS = frozenset({"password", "passwd", "passphrase", "secret", "token", "api_key", "apikey",
                           "authorization", "credential", "credentials", "private_key", "cookie"})
SECRET_SHAPES = (
    re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]{8,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
    re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"\b[sr]k_(?:live|test)_[A-Za-z0-9]{16,}"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{35}"),
)


def words(field: str) -> str:
    """A field name as `_word_word_`: camel case split, lower case, anything else a separator."""
    split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", field).lower()
    return "_" + re.sub(r"[^a-z0-9]+", "_", split).strip("_") + "_"


def is_secret_field(field: str) -> bool:
    name = words(field)
    return any(f"_{f}_" in name for f in SECRET_FIELDS)


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
        return is_secret_field(field) or field.lower() in self.fields
