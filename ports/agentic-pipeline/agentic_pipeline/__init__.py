"""agentic-pipeline — a declarative YAML pipeline that builds to the agentic system of
your choice, on any backend.

    from agentic_pipeline import load
    system = load("pipeline.yaml")
    system.submit(Event("c1", "what is my balance?"))
"""

from .loader import PipelineSystem, build_system, load

__all__ = ["load", "build_system", "PipelineSystem"]
__version__ = "0.1.0"
