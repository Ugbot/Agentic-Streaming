"""agentic-pipeline — a declarative YAML pipeline that builds to the agentic system of
your choice, on any backend.

    from agentic_pipeline import load
    system = load("pipeline.yaml")
    system.submit(Event("c1", "what is my balance?"))
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version

from .loader import PipelineSystem, build_system, load

__all__ = ["load", "build_system", "PipelineSystem"]
try:
    __version__ = _distribution_version("agentic-pipeline")
except PackageNotFoundError:  # imported from a checkout that was never installed
    __version__ = "0+unknown"
