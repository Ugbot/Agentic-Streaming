"""Every Python adapter must construct ``Event`` (and ``AgentContext``) with keyword
arguments. ``Event`` field order differs across cores (Python: conversation_id, text,
user_id; Java and Go: conversationId, userId, text), so a positional call in an adapter is
a latent bug when the same code is ported. This test parses every adapter source file and
fails on the first positional argument to an ``Event(...)`` call. The Python core itself
is not in scope (its positional signature is the documented Python API).
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from typing import List, Tuple

import pytest

EXPERIMENTAL = Path(__file__).resolve().parents[1]
ADAPTER_DIRS = ["airflow", "celery", "dask", "faust", "nats", "ray", "gateway-fastapi"]
KEYWORD_ONLY_CONSTRUCTORS = ("Event", "AgentContext")


def adapter_sources() -> List[Path]:
    files: List[Path] = []
    for name in ADAPTER_DIRS:
        root = EXPERIMENTAL / name
        assert root.is_dir(), f"adapter directory {root} is missing"
        files.extend(sorted(root.rglob("*.py")))
    files.extend(sorted((EXPERIMENTAL / "tests").glob("*.py")))
    return [f for f in files if ".venv" not in f.parts and "build" not in f.parts]


def _constructor_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def positional_constructor_calls(source: str, filename: str) -> List[Tuple[str, int, str]]:
    """(constructor, line, snippet) for every Event/AgentContext call with positional args."""
    tree = ast.parse(source, filename=filename)
    lines = source.splitlines()
    found: List[Tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _constructor_name(node)
        if name in KEYWORD_ONLY_CONSTRUCTORS and node.args:
            found.append((name, node.lineno, lines[node.lineno - 1].strip()))
    return found


def test_adapter_source_set_is_not_empty():
    files = adapter_sources()
    assert len(files) >= len(ADAPTER_DIRS)
    for name in ADAPTER_DIRS:
        assert any(name in f.parts for f in files), f"no Python sources found under {name}"


@pytest.mark.parametrize("path", adapter_sources(), ids=lambda p: str(p.relative_to(EXPERIMENTAL)))
def test_adapters_construct_events_with_keywords(path: Path):
    offenders = positional_constructor_calls(path.read_text(encoding="utf-8"), str(path))
    assert not offenders, "positional Event/AgentContext construction in an adapter:\n" + "\n".join(
        f"  {path.relative_to(EXPERIMENTAL)}:{line}: {name}(...) -> {snippet}" for name, line, snippet in offenders
    )


def test_checker_flags_positional_and_accepts_keyword_calls():
    """The checker itself: a positional call is reported (with its line), a keyword call,
    an attribute-qualified keyword call and an unrelated positional call are not."""
    positional = textwrap.dedent(
        """
        from pyagentic.core import Event
        e = Event("c1", "hello", "alice")
        f = core.Event("c2", text="hi")
        """
    )
    hits = positional_constructor_calls(positional, "positional.py")
    assert [(name, line) for name, line, _ in hits] == [("Event", 3), ("Event", 4)]

    keyword = textwrap.dedent(
        """
        from pyagentic.core import AgentContext, Event
        e = Event(conversation_id="c1", text="hello", user_id="alice")
        f = core.Event(conversation_id="c2", text="hi")
        ctx = AgentContext(conversation_id="c1", user_id="a", store=s, state=st, tools=t, retriever=None)
        other = TurnResult("c1", "reply", "cards", True, [])
        """
    )
    assert positional_constructor_calls(keyword, "keyword.py") == []
