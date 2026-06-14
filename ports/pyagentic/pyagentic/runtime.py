"""Runtime — binds the agent core to an execution substrate.

``Runtime`` is the engine seam (the Python mirror of the Java "Engine SPI"): give it
an event, it builds an ``AgentContext`` from the shared stores and runs the graph,
guaranteeing single-writer-per-conversation. ``LocalRuntime`` is the zero-dependency
in-process implementation (a lock per conversation id); the Faust/Ray/etc. adapters
provide their own ``Runtime`` where the engine supplies the per-key ordering.
"""

from __future__ import annotations

from threading import Lock
from typing import Dict, Optional, Protocol, Union

from .core import Agent, AgentContext, Event, RoutedGraph, TurnResult
from .memory import ConversationStore, InMemoryConversationStore, InMemoryKeyedStateStore, KeyedStateStore
from .retrieval import TwoTierRetriever
from .tools import ToolRegistry

Handler = Union[Agent, RoutedGraph]


class Runtime(Protocol):
    def submit(self, event: Event) -> TurnResult: ...


class LocalRuntime:
    """In-process runtime: shared stores + a per-conversation lock so each
    conversation is processed by a single writer at a time (the local stand-in for
    Flink's keyBy / a Kafka partition / a Ray actor)."""

    def __init__(
        self,
        handler: Handler,
        store: Optional[ConversationStore] = None,
        state: Optional[KeyedStateStore] = None,
        tools: Optional[ToolRegistry] = None,
        retriever: Optional[TwoTierRetriever] = None,
    ) -> None:
        self.handler = handler
        self.store = store or InMemoryConversationStore()
        self.state = state or InMemoryKeyedStateStore()
        self.tools = tools or ToolRegistry()
        self.retriever = retriever
        self._locks: Dict[str, Lock] = {}
        self._locks_guard = Lock()

    def _lock_for(self, cid: str) -> Lock:
        with self._locks_guard:
            lk = self._locks.get(cid)
            if lk is None:
                lk = Lock()
                self._locks[cid] = lk
            return lk

    def submit(self, event: Event) -> TurnResult:
        ctx = AgentContext(
            conversation_id=event.conversation_id,
            user_id=event.user_id,
            store=self.store,
            state=self.state,
            tools=self.tools,
            retriever=self.retriever,
        )
        with self._lock_for(event.conversation_id):
            if isinstance(self.handler, RoutedGraph):
                return self.handler.handle(event, ctx)
            return self.handler.turn(event, ctx)
