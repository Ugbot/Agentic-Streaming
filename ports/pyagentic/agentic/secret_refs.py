"""A secret is named in a document, never written in one: `env:NAME` or `file:/path`.

It is resolved at the moment of the call, so it reaches no document, argument, log or journal entry.

A document could name any variable or file the process can read, so a reference must also be on the
operator's list: `AGENTIC_SECRET_REFS`, comma-separated prefixes such as `env:AGENTIC_,file:/run/secrets/`.
That is the default. A file's path is normalised before it is compared, so `..` does not get out; a
symbolic link inside a listed directory is followed, so do not put one there.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

_REFERENCE = re.compile(r"env:[A-Za-z_][A-Za-z0-9_]*|file:/.+")


DEFAULT_ALLOWED = "env:AGENTIC_,file:/run/secrets/,file:/var/run/secrets/"


class SecretUnresolved(Exception):
    """A reference that is not one, or names nothing. It never carries a secret's value."""


def allowed(reference: str) -> bool:
    """True when the operator's list covers `reference`, a reference already."""
    listed = os.environ.get("AGENTIC_SECRET_REFS", "").strip() or DEFAULT_ALLOWED
    subject = reference
    if reference.startswith("file:"):
        subject = "file:" + os.path.normpath(reference[5:])
    return any(p.strip() and subject.startswith(p.strip()) for p in listed.split(","))


def _off_the_list(reference: str) -> str:
    return f"{reference} is not on the operator's list of secret references (AGENTIC_SECRET_REFS)"


def expand(value: str) -> str:
    """The value of `${NAME}` in a connection link, under the same list as `env:NAME`."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        reference = "env:" + value[2:-1]
        if not _REFERENCE.fullmatch(reference) or not allowed(reference):
            raise ValueError(_off_the_list(value))
        return os.environ.get(value[2:-1], "")
    return value


def resolve(reference: str) -> str:
    if not isinstance(reference, str) or not _REFERENCE.fullmatch(reference):
        raise SecretUnresolved("not a secret reference; use env:NAME or file:/path")
    if not allowed(reference):
        raise SecretUnresolved(_off_the_list(reference))
    if reference.startswith("env:"):
        value = os.environ.get(reference[4:])
        if not value:
            raise SecretUnresolved(f"{reference} is not set")
        return value
    try:
        return Path(reference[5:]).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SecretUnresolved(f"{reference} cannot be read") from exc
