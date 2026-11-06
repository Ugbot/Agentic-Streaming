"""Error classes, named after the error classes of `spec/v1/primitives.md` section 5."""

from __future__ import annotations

from typing import Iterable, Optional


class AgenticError(Exception):
    """Base of every error this package raises. `error_class` is the spec's error class."""

    error_class = "fatal"


class ValidationError(AgenticError, ValueError):
    """The workflow document, a tool argument, or a runtime selection is not acceptable.

    `pointer` is the JSON Pointer of the offending key when one is known.
    """

    error_class = "validation"

    def __init__(self, message: str, pointer: Optional[str] = None) -> None:
        self.pointer = pointer
        super().__init__(f"{pointer or '/'}: {message}" if pointer is not None else message)


class CapabilityError(ValidationError):
    """A workflow requires capabilities the selected runtime declares unsupported."""

    def __init__(self, runtime: str, requirements: Iterable[str]) -> None:
        self.runtime = runtime
        self.requirements = sorted(requirements)
        listing = "\n".join(f"  - {item}" for item in self.requirements)
        super().__init__(
            f"runtime {runtime!r} cannot run this workflow; unsupported requirements:\n{listing}"
        )


class RuntimeNotAvailableError(AgenticError, LookupError):
    """The selected runtime is not installed or not registered."""

    error_class = "validation"


class ToolError(AgenticError):
    """A tool raised. The message carries no runtime detail so results stay comparable."""

    error_class = "tool"


class TransientError(AgenticError):
    """A failure that may pass if the same request is made again: a model or network outage.
    Retried under `policies.retry`; a turn that outlasts the budget does not spend its turn_id."""

    error_class = "transient"


class LogUnavailable(AgenticError):
    """The event log could not be read or written: storage, not anything a turn did. The one
    failure the turn boundary lets through, since a turn cannot record its end in that log."""


class CorruptLog(AgenticError):
    """One conversation's log cannot be trusted: a bad record inside it, or a gap in its sequence.
    That is a failure of that conversation alone, not of the store."""


class GuardrailError(AgenticError):
    error_class = "guardrail"


class VerificationError(AgenticError):
    error_class = "verification"
