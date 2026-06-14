"""Compensation / saga — register reversible steps; if a later step fails, run the
recorded compensations in reverse order. Portable analogue of the Flink
``CompensationHandler``. Use as a context manager so a raised exception triggers rollback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, TypeVar

T = TypeVar("T")


@dataclass
class CompensationAction:
    name: str
    undo: Callable[[], None]


class Saga:
    """Records completed steps and their compensations; rolls back in reverse on failure.

    >>> with Saga() as saga:
    ...     saga.step("charge", do_charge, undo=refund)
    ...     saga.step("ship", do_ship, undo=cancel_ship)   # if this raises, refund runs
    """

    def __init__(self) -> None:
        self._done: List[CompensationAction] = []

    def step(self, name: str, do: Callable[[], T], undo: Callable[[], None]) -> T:
        """Run ``do``; on success record ``undo`` for later rollback and return its result.
        If ``do`` itself raises, the saga compensates everything recorded so far, then
        re-raises."""
        try:
            result = do()
        except Exception:
            self.compensate()
            raise
        self._done.append(CompensationAction(name, undo))
        return result

    def compensate(self) -> List[str]:
        """Run all recorded compensations in reverse order; returns the names compensated."""
        compensated: List[str] = []
        for action in reversed(self._done):
            try:
                action.undo()
            finally:
                compensated.append(action.name)
        self._done.clear()
        return compensated

    def __enter__(self) -> "Saga":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            self.compensate()
        return False  # never suppress the original exception
