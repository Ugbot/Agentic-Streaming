"""Fixtures for the adapter tests; the helpers live in adapter_support, the engine markers in ../pytest.ini."""

from __future__ import annotations

import pytest
from adapter_support import extended_graph, extended_tools, random_conversation_id


@pytest.fixture
def conversation_id():
    return random_conversation_id("t")


@pytest.fixture
def extension():
    """(graph, tools) of the extended banking system, for adapters that inject deps."""
    return extended_graph(), extended_tools()
