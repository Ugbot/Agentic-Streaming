"""A secret is named in a document, never written in one: `env:NAME` or `file:/path`.

It is resolved at the moment of the call, so it reaches no document, argument, log or journal entry.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

_REFERENCE = re.compile(r"env:[A-Za-z_][A-Za-z0-9_]*|file:/.+")


class SecretUnresolved(Exception):
    """A reference that is not one, or names nothing. It never carries a secret's value."""


def resolve(reference: str) -> str:
    if not isinstance(reference, str) or not _REFERENCE.fullmatch(reference):
        raise SecretUnresolved("not a secret reference; use env:NAME or file:/path")
    if reference.startswith("env:"):
        value = os.environ.get(reference[4:])
        if not value:
            raise SecretUnresolved(f"{reference} is not set")
        return value
    try:
        return Path(reference[5:]).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SecretUnresolved(f"{reference} cannot be read") from exc
