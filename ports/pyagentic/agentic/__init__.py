"""agentic: define a workflow once, select a runtime, keep the same observable behavior.

    from agentic import Agent, load
    spec = Agent("support").route(rules={"billing": ["refund"]}, default="general") \\
                           .path("billing").path("general").build()
    spec.run(runtime="local", text="refund me", conversation_id="c1", turn_id="t1")
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version

from .bindings import Bindings, BrainContext
from .errors import (
    AgenticError,
    CapabilityError,
    GuardrailError,
    OutcomeUnknown,
    RuntimeNotAvailableError,
    ToolError,
    ValidationError,
    VerificationError,
)
from .events import Event, Turn
from .journal import EffectJournal, FileEffectJournal, InMemoryEffectJournal
from .runtime import Runtime, available_runtimes, get_runtime, register_runtime
from .spec import Agent, AgentSpec, load
from .tools import Delegation, ToolCall, current_call

try:
    __version__ = _distribution_version("pyagentic")
except PackageNotFoundError:  # imported from a checkout that was never installed
    __version__ = "0+unknown"

__all__ = [
    "Agent", "AgentSpec", "load", "Turn", "Event", "Delegation", "Bindings", "BrainContext",
    "Runtime", "get_runtime", "register_runtime", "available_runtimes",
    "AgenticError", "ValidationError", "CapabilityError", "RuntimeNotAvailableError",
    "ToolError", "OutcomeUnknown", "GuardrailError", "VerificationError",
    "EffectJournal", "InMemoryEffectJournal", "FileEffectJournal", "ToolCall", "current_call",
    "__version__",
]
