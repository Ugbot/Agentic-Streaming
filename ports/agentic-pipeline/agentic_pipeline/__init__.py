"""agentic-pipeline — a declarative YAML pipeline that builds to the agentic system of
your choice.

    from agentic_pipeline import Event, load
    system = load("pipeline.yaml")
    system.submit(Event("c1", "what is my balance?"))
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version

from .backends import Event, TurnResult
from .loader import PipelineSystem, build_system, legacy_features, load

__all__ = ["load", "build_system", "legacy_features", "PipelineSystem", "Event", "TurnResult"]
try:
    __version__ = _distribution_version("agentic-pipeline")
except PackageNotFoundError:  # imported from a checkout that was never installed
    __version__ = "0+unknown"
