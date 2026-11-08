"""YAML pipeline loader: parse a ``pipeline.yaml``, build the system and select the backend. The
result is a runnable system: an object with ``submit(Event) -> TurnResult``.

    from agentic_pipeline import Event, load
    system = load("banking.yaml")          # backend chosen by the YAML
    print(system.submit(Event("c1", "what is my balance?")).reply)

Every pipeline runs on the ``agentic`` core: a turn is a turn of ``spec/v1``, on the event log,
deduplicated by ``turn_id``, under the tool policy, with its effects journaled. A document that names
something the core cannot do is refused when it loads, with the reason, rather than run with less.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from agentic import ir

from . import backends
from .backends import CoreBackend, TurnResult


class PipelineSystem:
    """A built, deployed agentic system: the backend runtime and the spec that produced it."""

    def __init__(self, spec: Dict[str, Any], backend: CoreBackend) -> None:
        self.spec = spec
        self.backend = backend

    @property
    def backend_name(self) -> str:
        return self.backend.name

    @property
    def tools(self) -> Any:
        return self.backend.tools

    def submit(self, event: Any) -> TurnResult:
        """Run one turn, and any turn a ``cep`` pattern injects after it, on the core."""
        return self.backend.submit(event)


def build_system(spec: Dict[str, Any], backend: Optional[str] = None,
                 store_dir: Optional[str] = None) -> PipelineSystem:
    """Validate a spec dict and deploy it on the chosen backend (spec ``backend:`` unless overridden).
    ``store_dir`` puts the event log and the journal on disk."""
    backends.resolve(backend or spec.get("backend", "local"))
    return PipelineSystem(spec, CoreBackend(ir.validate_document(spec), store_dir))


def load(path: str, backend: Optional[str] = None, store_dir: Optional[str] = None) -> PipelineSystem:
    """Load a ``pipeline.yaml`` and build the system."""
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return build_system(spec, backend=backend, store_dir=store_dir)
