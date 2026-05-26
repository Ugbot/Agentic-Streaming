"""Cloudpickle + base64 helpers for plan payloads."""

from __future__ import annotations

import base64
from typing import Any


def encode(obj: Any) -> str:
    """Cloudpickle ``obj`` and return base64-ascii. Lazy-imports cloudpickle so the
    rest of the package stays importable without the ``pyflink`` extra."""
    try:
        import cloudpickle
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "cloudpickle is required for the PyFlink-native plan path. "
            "Install with: pip install 'agentic-flink[pyflink]'"
        ) from e
    return base64.b64encode(cloudpickle.dumps(obj)).decode("ascii")


def decode(b64: str) -> Any:
    """Inverse of :func:`encode`. Used by tests and by Python-side replay paths."""
    try:
        import cloudpickle
    except ImportError as e:  # pragma: no cover
        raise ImportError("cloudpickle required to decode") from e
    return cloudpickle.loads(base64.b64decode(b64))
