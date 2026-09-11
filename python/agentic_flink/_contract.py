"""The shared Tier-2 runtime contract: ``Runtime`` ABC + the runtime registry.

The canonical home of these names is ``agentic.runtime`` in the pure-Python package
(``ports/pyagentic``). When that module is importable and exposes the registry, this module
re-exports it so both packages share one registry. Until then, this is a thin shim that
implements the same contract, so the JVM-backed runtimes in :mod:`agentic_flink.runtimes`
can be developed and tested against it. Nothing here knows about the JVM.

Runtimes are discovered through the ``agentic.runtimes`` entry-point group and through
explicit :func:`register_runtime` calls. Selecting a runtime that is not installed raises
:class:`RuntimeNotAvailable` naming the extra to install; there is no silent fallback.
"""

from __future__ import annotations

import abc
from importlib import metadata
from typing import Any, Callable, Dict, Mapping, Optional

try:  # the pure-Python package owns these names once it ships them
    from agentic.runtime import Runtime as _SharedRuntime  # type: ignore[import-not-found]
    from agentic.runtime import get_runtime as _shared_get_runtime  # type: ignore[import-not-found]
    from agentic.runtime import register_runtime as _shared_register_runtime  # type: ignore[import-not-found]
except ImportError:
    _SHARED = None
    CONTRACT_SOURCE = "agentic_flink._contract (local shim)"
else:
    _SHARED = (_SharedRuntime, _shared_get_runtime, _shared_register_runtime)
    CONTRACT_SOURCE = "agentic.runtime"

ENTRY_POINT_GROUP = "agentic.runtimes"

CAPABILITY_VALUES = ("supported", "partial", "unsupported", "not_tested")

CAPABILITY_IDS = (
    "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails",
    "verifier", "ordering", "idempotency", "retry", "memory", "retrieval", "context_window",
    "replay", "suspend_resume", "timers", "saga", "a2a", "cep", "event_time",
    "checkpoint_recovery", "parallelism", "durable_store",
)


class RuntimeNotAvailable(LookupError):
    """The selected runtime is not registered, or its package/extra is not installed."""


class UnsupportedRequirements(ValueError):
    """``deploy`` was asked for capabilities the runtime does not support."""

    def __init__(self, runtime: str, requirements: Mapping[str, str]) -> None:
        self.runtime = runtime
        self.requirements = dict(requirements)
        listing = ", ".join(f"{k} ({v})" for k, v in sorted(self.requirements.items()))
        super().__init__(f"runtime {runtime!r} cannot run this workflow; unsupported requirements: {listing}")


class Runtime(abc.ABC):
    """One execution substrate for an ``agentic/v1`` workflow.

    ``deploy`` binds a workflow document; ``submit`` runs one event on it and returns a
    normalized result mapping (``spec/v1/result.schema.json``); ``close`` releases resources.
    """

    name: str = "runtime"

    @abc.abstractmethod
    def capabilities(self) -> Dict[str, str]:
        """``{capability_id: "supported"|"partial"|"unsupported"|"not_tested"}``."""

    @abc.abstractmethod
    def deploy(self, spec: Any) -> None:
        """Bind a workflow (an ``AgentSpec`` or plain workflow mapping).

        Raises :class:`UnsupportedRequirements` listing every requirement the runtime
        cannot honour, before anything is started."""

    @abc.abstractmethod
    def submit(self, event: Any) -> Dict[str, Any]:
        """Run one event and return its normalized result."""

    @abc.abstractmethod
    def close(self) -> None:
        """Release everything ``deploy`` acquired."""

    def __enter__(self) -> "Runtime":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


RuntimeFactory = Callable[..., Runtime]

_REGISTRY: Dict[str, RuntimeFactory] = {}


def register_runtime(name: str, factory: RuntimeFactory) -> None:
    """Register ``factory`` under ``name``; explicit registrations win over entry points."""
    if not callable(factory):
        raise TypeError(f"runtime factory for {name!r} must be callable, got {factory!r}")
    _REGISTRY[name] = factory


def unregister_runtime(name: str) -> None:
    _REGISTRY.pop(name, None)


def _entry_points() -> Dict[str, metadata.EntryPoint]:
    eps = metadata.entry_points()
    selected = eps.select(group=ENTRY_POINT_GROUP) if hasattr(eps, "select") else eps.get(ENTRY_POINT_GROUP, ())
    return {ep.name: ep for ep in selected}


def available_runtimes() -> Dict[str, str]:
    """Every runtime name that can be selected, mapped to where it comes from."""
    out = {name: "entry-point" for name in _entry_points()}
    out.update({name: "registered" for name in _REGISTRY})
    return dict(sorted(out.items()))


def _resolve_factory(name: str) -> Optional[RuntimeFactory]:
    if name in _REGISTRY:
        return _REGISTRY[name]
    ep = _entry_points().get(name)
    if ep is None:
        return None
    try:
        return ep.load()
    except ImportError as e:
        raise RuntimeNotAvailable(
            f"runtime {name!r} is registered by {ep.value} but its dependencies are not installed: {e}. "
            f"Install the matching extra (for example `pip install \"agentic-flink[{name}]\"`)."
        ) from e


def get_runtime(name: str, **options: Any) -> Runtime:
    """Instantiate the runtime registered as ``name`` with runtime-specific ``options``."""
    factory = _resolve_factory(name)
    if factory is None:
        known = ", ".join(available_runtimes()) or "none"
        raise RuntimeNotAvailable(
            f"no runtime named {name!r} is installed (available: {known}). Install the package "
            f"that provides it, e.g. `pip install \"agentic-flink[{name}]\"`, or call "
            f"register_runtime({name!r}, factory)."
        )
    rt = factory(**options)
    if not isinstance(rt, Runtime):
        raise TypeError(f"runtime factory for {name!r} returned {type(rt).__name__}, not a Runtime")
    return rt


if _SHARED is not None:
    Runtime, get_runtime, register_runtime = _SHARED  # type: ignore[misc,assignment]


__all__ = [
    "CAPABILITY_IDS",
    "CONTRACT_SOURCE",
    "CAPABILITY_VALUES",
    "ENTRY_POINT_GROUP",
    "Runtime",
    "RuntimeFactory",
    "RuntimeNotAvailable",
    "UnsupportedRequirements",
    "available_runtimes",
    "get_runtime",
    "register_runtime",
    "unregister_runtime",
]
