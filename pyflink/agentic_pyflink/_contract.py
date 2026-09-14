"""The shared runtime contract this package implements.

The canonical ``Runtime`` ABC and ``required_capabilities`` live in ``agentic.runtime`` of the
pure Python package (``ports/pyagentic``). When that package is importable this module
re-exports them, so ``agentic.runtime.get_runtime("pyflink")`` gets an object that passes its
``isinstance(runtime, Runtime)`` check and derives requirements exactly like every other binding.
When it is absent (a wheel installed without ``pyagentic``) the fallback below defines the same
ABC with the same abstract methods and the same derivation, so behaviour does not change.
"""

from __future__ import annotations

import abc
from collections.abc import Mapping
from typing import Any

try:
    from agentic.runtime import Runtime, required_capabilities  # type: ignore[import-not-found]
except ImportError:
    CONTRACT_SOURCE = "agentic_pyflink._contract (fallback until pyagentic is installed)"

    class Runtime(abc.ABC):  # type: ignore[no-redef]
        """One deployed workflow, one runtime. ``deploy`` before ``submit``; ``close`` when done."""

        name: str = "abstract"

        @abc.abstractmethod
        def capabilities(self) -> dict[str, str]:
            """Every v1 capability id mapped to supported | partial | unsupported | not_tested."""

        @abc.abstractmethod
        def deploy(self, spec: Any) -> None:
            """Validate ``spec`` against this runtime and make it live."""

        @abc.abstractmethod
        def submit(self, event: Mapping[str, Any]) -> dict[str, Any]:
            """Process one turn to a terminal status and return the normalized result."""

        @abc.abstractmethod
        def close(self) -> None: ...

        def __enter__(self) -> Runtime:
            return self

        def __exit__(self, *exc: object) -> None:
            self.close()

    def required_capabilities(doc: Mapping[str, Any]) -> list[str]:  # type: ignore[no-redef]
        """The capability ids a validated workflow document needs from any runtime.

        Mirrors ``agentic.runtime.required_capabilities`` line for line; the pure package is the
        source of truth and ``tests/test_contract.py`` checks the two agree on every fixture.
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

__all__ = ["CONTRACT_SOURCE", "Runtime", "required_capabilities"]
