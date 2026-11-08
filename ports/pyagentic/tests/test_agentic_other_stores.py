"""`stores.long_term`, `keyed_state` and `vector`. The local runtime keeps conversations on its event log
and has no separate store for these, so it takes `kind: memory` and refuses a store it would not use,
rather than load the document and quietly do less. `on_unavailable: degrade` says doing less is fine."""

from __future__ import annotations

from typing import Any, Dict

import pytest

from agentic import Turn
from agentic.errors import ValidationError
from agentic.events import FileEventLog
from agentic.journal import FileEffectJournal
from agentic.runtime import LocalRuntime


def document(**stores: Any) -> Dict[str, Any]:
    return {"agent": {"id": "a", "router": {"kind": "static", "default": "main"},
                      "paths": {"main": {"brain": "rule"}}},
            "stores": stores}


@pytest.mark.parametrize("name", ["long_term", "keyed_state", "vector"])
def test_memory_is_taken(name):
    rt = LocalRuntime()
    rt.deploy(document(**{name: {"kind": "memory"}}))
    assert rt.submit(Turn("c1", "t1", "hi"))["status"] == "completed"


@pytest.mark.parametrize("name,kind", [("long_term", "postgres"), ("keyed_state", "redis"), ("vector", "qdrant")])
def test_a_store_it_would_not_use_is_refused(name, kind):
    with pytest.raises(ValidationError, match=f"stores.{name}"):
        LocalRuntime().deploy(document(**{name: {"kind": kind, "url": "x"}}))


def test_degrade_takes_memory_and_says_so():
    rt = LocalRuntime()
    rt.deploy(document(long_term={"kind": "postgres", "url": "x", "on_unavailable": "degrade"}))
    result = rt.submit(Turn("c1", "t1", "hi"))
    assert result["status"] == "completed"
    assert any("stores.long_term" in d for d in result["runtime_detail"]["degraded"])


def test_a_store_directory_holds_the_log_whatever_the_document_defaults_to(tmp_path):
    """`store_dir` is the operator's say on where the log goes. A document that spelt out the default,
    `conversation: {kind: memory}`, left the log in memory and lost it between runs."""
    rt = LocalRuntime(store_dir=str(tmp_path))
    rt.deploy(document(conversation={"kind": "memory"}))
    assert isinstance(rt.log, FileEventLog) and isinstance(rt.journal, FileEffectJournal)
    rt.submit(Turn("c1", "t1", "hi"))
    rt.close()
    again = LocalRuntime(store_dir=str(tmp_path))
    again.deploy(document(conversation={"kind": "memory"}))
    assert again.submit(Turn("c1", "t1", "hi"))["status"] == "duplicate"
    again.close()
