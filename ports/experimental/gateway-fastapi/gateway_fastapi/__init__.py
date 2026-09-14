"""Agentic-Flink FastAPI gateway — an HTTP front door for the pure-Python ports.

Exposes the portable banking ``router -> path -> verifier`` agent over HTTP with an
A2A-style Agent Card, a turn endpoint, and conversation-transcript inspection. The
agent logic, tools, and retrieval are reused verbatim from ``pyagentic``; this package
only adds the HTTP seam and a pluggable backend runtime (local / celery / nats).
"""

from __future__ import annotations

from .app import create_app
from .backends import (
    Backend,
    CeleryBackend,
    LocalBackend,
    NatsBackend,
    make_backend,
)

__all__ = [
    "create_app",
    "Backend",
    "LocalBackend",
    "CeleryBackend",
    "NatsBackend",
    "make_backend",
]
