"""The engine-agnostic agent core: Event, AgentContext, Brain, Agent, and the
``router -> path -> verifier`` RoutedGraph — the essence the design docs distil.

Nothing here knows about Flink (or any engine). An engine adapter supplies the
``ConversationStore`` / ``KeyedStateStore`` / tools, then calls ``RoutedGraph.handle``
(or ``Agent.turn``) per inbound event, with single-writer-per-conversation provided
by the engine (Kafka partition, Ray actor, …) or by the LocalRuntime here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol

from .memory import ChatMessage, ConversationStore, KeyedStateStore
from .retrieval import TwoTierRetriever
from .tools import ToolRegistry


@dataclass(frozen=True)
class Event:
    """One inbound message. ``conversation_id`` is the partition/state key."""

    conversation_id: str
    text: str
    user_id: str = "anonymous"
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class TurnResult:
    conversation_id: str
    reply: str
    path: Optional[str] = None
    ok: bool = True
    tool_calls: List[str] = field(default_factory=list)


@dataclass
class AgentContext:
    """Everything an agent's brain needs for one turn — the per-conversation handle.

    Bundles the conversation key, the (shared, durable) ConversationStore, the
    keyed state store, the tool registry, and an optional retriever. This is what
    decouples agent logic from the engine: give it a context, it runs a turn.
    """

    conversation_id: str
    user_id: str
    store: ConversationStore
    state: KeyedStateStore
    tools: ToolRegistry
    retriever: Optional[TwoTierRetriever] = None
    tool_calls: List[str] = field(default_factory=list)

    def call_tool(self, tool_id: str, params: Dict[str, object]) -> object:
        self.tool_calls.append(tool_id)
        return self.tools.execute(tool_id, params)


class Brain(Protocol):
    """Produces a reply for a turn. A real brain runs an LLM ReAct loop; tests use
    a deterministic rule brain so the port runs with no model."""

    def turn(self, user_text: str, ctx: AgentContext) -> str: ...


@dataclass
class Agent:
    """A named agent = id + system prompt + a brain. Stateless itself; all state is
    in the ConversationStore/KeyedStateStore reached through the context."""

    agent_id: str
    system_prompt: str
    brain: Brain

    def turn(self, event: Event, ctx: AgentContext) -> TurnResult:
        ctx.store.associate_user(event.conversation_id, event.user_id)
        ctx.store.append(event.conversation_id, ChatMessage.user(event.text))
        reply = self.brain.turn(event.text, ctx)
        ctx.store.append(event.conversation_id, ChatMessage.assistant(reply))
        return TurnResult(event.conversation_id, reply, tool_calls=list(ctx.tool_calls))


# router(event, ctx) -> path name (string key into the paths map)
Router = Callable[[Event, AgentContext], str]
# verifier(reply, ctx) -> (ok, possibly-annotated reply)
Verifier = Callable[[str, AgentContext], "tuple[bool, str]"]

PHASE_ATTR = "graph.phase"
PATH_ATTR = "graph.path"


class RoutedGraph:
    """The canonical topology: classify (router) -> dispatch to a specialized path
    agent -> validate (verifier). The chosen path + phase are persisted to the
    ConversationStore so a multi-turn conversation stays on its path and the next
    turn can resume — exactly what the Java ``BankingAgentGraph`` does with its
    routed keyed state.
    """

    def __init__(
        self,
        router: Router,
        paths: Dict[str, Agent],
        verifier: Optional[Verifier] = None,
        guardrails: Optional[list] = None,
        listeners: Optional[list] = None,
    ):
        if not paths:
            raise ValueError("RoutedGraph requires at least one path")
        self.router = router
        self.paths = dict(paths)
        self.verifier = verifier
        self.guardrails = list(guardrails or [])
        self.listeners = list(listeners or [])

    def handle(self, event: Event, ctx: AgentContext) -> TurnResult:
        cid = event.conversation_id
        for ln in self.listeners:
            ln.on_turn_start(event, ctx)

        # Input guardrails: short-circuit a blocked turn before routing.
        for g in self.guardrails:
            reason = g.check_input(event.text)
            if reason:
                ctx.store.put_attribute(cid, PHASE_ATTR, "blocked")
                blocked = TurnResult(cid, f"[blocked] {reason}", path="blocked", ok=False)
                for ln in self.listeners:
                    ln.on_turn_end(blocked, ctx)
                return blocked

        ctx.store.put_attribute(cid, PHASE_ATTR, "router")
        path = self.router(event, ctx)
        if path not in self.paths:
            path = next(iter(self.paths))  # fall back to the first declared path
        ctx.store.put_attribute(cid, PATH_ATTR, path)
        ctx.store.put_attribute(cid, PHASE_ATTR, "path:" + path)
        for ln in self.listeners:
            ln.on_routed(path, ctx)

        result = self.paths[path].turn(event, ctx)
        result.path = path

        if self.verifier is not None:
            ctx.store.put_attribute(cid, PHASE_ATTR, "verifier")
            ok, annotated = self.verifier(result.reply, ctx)
            result.ok = ok
            result.reply = annotated

        # Output guardrails: redact/replace a disallowed reply.
        for g in self.guardrails:
            reason = g.check_output(result.reply)
            if reason:
                result.reply = f"[blocked] {reason}"
                result.ok = False

        ctx.store.put_attribute(cid, PHASE_ATTR, "done")
        for ln in self.listeners:
            ln.on_turn_end(result, ctx)
        return result
