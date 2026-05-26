"""Tiny adapter around PyFlink's Py4J gateway.

Centralizes the ``invoke_method`` call so the rest of the package doesn't have to
know about PyFlink's internal API. Importing this module requires PyFlink at
runtime (it raises a clear error otherwise).
"""

from __future__ import annotations

from typing import Any


def _gateway():
    try:
        from pyflink.java_gateway import get_gateway
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "PyFlink is required for the pyflink-native path. "
            "Install with: pip install 'agentic-flink[pyflink]'"
        ) from e
    return get_gateway()


def compile_utils():
    """Return the ``CompileUtils`` Java class via the gateway."""
    gw = _gateway()
    return gw.jvm.org.agentic.flink.compile.CompileUtils


def attach_agent(j_keyed_stream: Any, plan_json: str) -> Any:
    """Call ``CompileUtils.attachAgent(stream, planJson)`` on the JVM side."""
    return compile_utils().attachAgent(j_keyed_stream, plan_json)
