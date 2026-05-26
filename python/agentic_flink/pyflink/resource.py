"""Plan-side descriptors for Java SPIs and tools.

Mirrors the Java ``ResourceSpec`` / ``ToolSpec`` / ``ActionSpec`` JSON shape. Pure
dataclasses; no JVM dependency, so plan building is testable without a running
gateway.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ResourceRef:
    """Reference to a Java SPI by FQN + an init config map.

    The framework's Java side instantiates ``fqn`` via reflection and either passes
    ``config`` to a Map-arg constructor or calls ``initialize(config)`` after a no-arg
    construction. Identical convention to ``StorageFactory.createLongTermStore``.
    """

    fqn: str
    config: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"fqn": self.fqn, "config": dict(self.config)}


@dataclass
class _ToolSpec:
    kind: str  # "java" or "python"
    name: str
    description: Optional[str] = None
    fqn: Optional[str] = None
    config: Dict[str, str] = field(default_factory=dict)
    cloudpickle_b64: Optional[str] = None
    param_names: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"kind": self.kind, "name": self.name}
        if self.description is not None:
            d["description"] = self.description
        if self.kind == "java":
            d["fqn"] = self.fqn
            d["config"] = dict(self.config)
        else:
            d["cloudpickle_b64"] = self.cloudpickle_b64
            d["param_names"] = list(self.param_names)
        return d


@dataclass
class _ActionSpec:
    name: str
    events: List[str]
    cloudpickle_b64: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "events": list(self.events),
            "cloudpickle_b64": self.cloudpickle_b64,
        }


@dataclass
class _ListenerSpec:
    kind: str  # "java" or "python"
    fqn: Optional[str] = None
    cloudpickle_b64: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"kind": self.kind}
        if self.kind == "java":
            d["fqn"] = self.fqn
        else:
            d["cloudpickle_b64"] = self.cloudpickle_b64
        return d
