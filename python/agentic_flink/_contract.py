"""The shared Tier-2 runtime contract: ``Runtime`` ABC + the runtime registry.

The canonical home of these names is ``agentic.runtime`` / ``agentic.errors`` in the
pure-Python package (``ports/pyagentic``, PR #25). When that package is importable this
module is a straight re-export of it, so every Tier-2 package shares one registry. Until
it is installed, the fallback below implements the same names with the same shapes
(``RuntimeNotAvailableError``, ``CapabilityError(runtime, requirements)``, the
``agentic.runtimes`` entry-point group, ``register_runtime`` beating entry points) so the
JVM-backed runtimes in :mod:`agentic_flink.runtimes` behave identically either way.
Nothing here knows about the JVM.

Selecting a runtime that is not installed raises :class:`RuntimeNotAvailableError` naming
the extra to install; there is no silent fallback.
"""

from __future__ import annotations

import abc
import threading
from importlib import metadata
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

ENTRY_POINT_GROUP = "agentic.runtimes"

CAPABILITY_VALUES: Tuple[str, ...] = ("supported", "partial", "unsupported", "not_tested")

CAPABILITY_IDS: Tuple[str, ...] = (
    "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails",
    "verifier", "ordering", "idempotency", "retry", "memory", "retrieval", "context_window",
    "replay", "suspend_resume", "timers", "saga", "a2a", "cep", "event_time",
    "checkpoint_recovery", "parallelism", "durable_store",
)

# Runtime names this project provides elsewhere, with the extra that installs them.
KNOWN_EXTRAS: Dict[str, str] = {
    "local-jvm": "agentic-flink",
    "flink-jvm": "agentic-flink[flink]",
    "pyflink": "agentic-pyflink",
    "pekko": "agentic-flink (plus the agentic-pekko jars; no pip extra yet)",
    "local": "pyagentic",
}

try:
    from agentic.errors import CapabilityError, RuntimeNotAvailableError  # type: ignore[import-not-found]
    from agentic.runtime import (  # type: ignore[import-not-found]
        Runtime,
        available_runtimes,
        get_runtime,
        register_runtime,
        required_capabilities,
        unregister_runtime,
    )
except ImportError:
    CONTRACT_SOURCE = "agentic_flink._contract (fallback until pyagentic is installed)"

    class RuntimeNotAvailableError(LookupError):  # type: ignore[no-redef]
        """The selected runtime is not installed or not registered."""

        error_class = "validation"

    class CapabilityError(ValueError):  # type: ignore[no-redef]
        """A workflow requires capabilities the selected runtime declares unsupported."""

        error_class = "validation"

        def __init__(self, runtime: str, requirements: Iterable[str]) -> None:
            self.runtime = runtime
            self.requirements = sorted(requirements)
            listing = "\n".join(f"  - {item}" for item in self.requirements)
            super().__init__(
                f"runtime {runtime!r} cannot run this workflow; unsupported requirements:\n{listing}"
            )

    class Runtime(abc.ABC):  # type: ignore[no-redef]
        """One deployed workflow, one runtime. ``deploy`` before ``submit``; ``close`` when done."""

        name: str = "abstract"

        @abc.abstractmethod
        def capabilities(self) -> Dict[str, str]:
            """Every v1 capability id mapped to supported | partial | unsupported | not_tested."""

        @abc.abstractmethod
        def deploy(self, spec: Any) -> None:
            """Validate ``spec`` (an ``AgentSpec`` or workflow document) against this runtime and
            make it live. Raises :class:`CapabilityError` listing every unsupported requirement."""

        @abc.abstractmethod
        def submit(self, event: Any) -> Dict[str, Any]:
            """Process one turn to a terminal status and return the normalized result."""

        @abc.abstractmethod
        def close(self) -> None: ...

        def __enter__(self) -> "Runtime":
            return self

        def __exit__(self, *exc: object) -> None:
            self.close()

    Factory = Callable[..., Runtime]
    _registry: Dict[str, Factory] = {}
    _registry_guard = threading.Lock()

    def register_runtime(name: str, factory: Factory) -> None:  # type: ignore[no-redef]
        """Make ``get_runtime(name, **options)`` return ``factory(**options)``."""
        if not name or not callable(factory):
            raise ValueError("register_runtime needs a non-empty name and a callable factory")
        with _registry_guard:
            _registry[name] = factory

    def unregister_runtime(name: str) -> None:  # type: ignore[no-redef]
        with _registry_guard:
            _registry.pop(name, None)

    def _entry_points() -> Dict[str, metadata.EntryPoint]:
        eps = metadata.entry_points()
        selected = eps.select(group=ENTRY_POINT_GROUP) if hasattr(eps, "select") else eps.get(ENTRY_POINT_GROUP, ())
        return {ep.name: ep for ep in selected}

    def available_runtimes() -> Dict[str, str]:  # type: ignore[no-redef]
        """Runtime names that would resolve right now, mapped to where they come from."""
        found = {name: f"entry point {ep.value}" for name, ep in _entry_points().items()}
        with _registry_guard:
            for name in _registry:
                found[name] = "register_runtime"
        return dict(sorted(found.items()))

    def _resolve_factory(name: str) -> Optional[Factory]:
        with _registry_guard:
            factory = _registry.get(name)
        if factory is not None:
            return factory
        ep = _entry_points().get(name)
        if ep is None:
            return None
        try:
            return ep.load()
        except ImportError as e:
            extra = KNOWN_EXTRAS.get(name, f"agentic-flink[{name}]")
            raise RuntimeNotAvailableError(
                f"runtime {name!r} is registered by {ep.value} but its dependencies are not installed: {e}. "
                f"Install it with `pip install '{extra}'`."
            ) from e

    def get_runtime(name: str = "local", **options: Any) -> Runtime:  # type: ignore[no-redef]
        factory = _resolve_factory(name)
        if factory is None:
            extra = KNOWN_EXTRAS.get(name)
            hint = (f"install it with `pip install '{extra}'`" if extra
                    else "install the package that provides it, or call register_runtime()")
            known = ", ".join(available_runtimes()) or "none"
            raise RuntimeNotAvailableError(
                f"runtime {name!r} is not available; {hint}. Available runtimes: {known}")
        runtime = factory(**options)
        if not isinstance(runtime, Runtime):
            raise RuntimeNotAvailableError(
                f"factory for runtime {name!r} returned {type(runtime).__name__}, not a Runtime")
        return runtime

    def required_capabilities(doc: Mapping[str, Any]) -> List[str]:  # type: ignore[no-redef]
        """The capability ids a validated workflow document needs from any runtime.

        Mirrors ``agentic.runtime.required_capabilities`` line for line; the pure package is the
        source of truth and ``tests/test_capabilities_canonical.py`` checks the two agree on every fixture.
        """
        agent = doc["agent"]
        paths: Mapping[str, Mapping[str, Any]] = agent["paths"]
        policies = doc.get("policies") or {}
        needs = ["routing"]
        brains = {p.get("brain", "rule") for p in paths.values() if "x-brain" not in p}
        if "rule" in brains:
            needs.append("rule_brain")
        if "llm" in brains:
            needs.append("llm_brain")
        if doc.get("tools") or doc.get("mcp") or doc.get("a2a"):
            needs.append("tools")
        if any("parameters" in t for t in doc.get("tools") or []):
            needs.append("structured_tool_args")
        if doc.get("guardrails"):
            needs.append("guardrails")
        verifiers = [agent.get("verifier") or {}] + [p.get("verifier") or {} for p in paths.values()]
        if any(v.get("kind", "prefix") != "none" for v in verifiers):
            needs.append("verifier")
        if policies.get("ordering", "per-conversation") == "per-conversation":
            needs.append("ordering")
        if policies.get("idempotency", "turn-id") == "turn-id":
            needs.append("idempotency")
        if (policies.get("retry") or {}).get("kind", "none") != "none":
            needs.append("retry")
        needs.append("memory")
        if doc.get("retrieval"):
            needs.append("retrieval")
        if doc.get("context"):
            needs.append("context_window")
        if any("x-suspend-until" in p for p in paths.values()):
            needs.append("suspend_resume")
        if doc.get("timers"):
            needs.append("timers")
        if doc.get("saga"):
            needs.append("saga")
        if doc.get("a2a"):
            needs.append("a2a")
        if doc.get("cep"):
            needs.append("cep")
        if doc.get("stores"):
            needs.append("durable_store")
        return needs
else:
    CONTRACT_SOURCE = "agentic.runtime"

# Names used by earlier revisions of this package.
RuntimeNotAvailable = RuntimeNotAvailableError
UnsupportedRequirements = CapabilityError
RuntimeFactory = Callable[..., Runtime]

__all__ = [
    "CAPABILITY_IDS",
    "CAPABILITY_VALUES",
    "CONTRACT_SOURCE",
    "ENTRY_POINT_GROUP",
    "KNOWN_EXTRAS",
    "CapabilityError",
    "Runtime",
    "RuntimeFactory",
    "RuntimeNotAvailable",
    "RuntimeNotAvailableError",
    "UnsupportedRequirements",
    "available_runtimes",
    "get_runtime",
    "register_runtime",
    "required_capabilities",
    "unregister_runtime",
]
