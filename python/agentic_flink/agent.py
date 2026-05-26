"""Agent + AgentBuilder Pythonic facade.

The most-used surface of the framework. ``Agent.builder()`` returns a
Pythonic fluent builder whose methods use snake_case and accept Python
values; conversion to Java happens transparently in :meth:`build`.

Example::

    from agentic_flink import start_jvm, Agent
    from agentic_flink.llm import ChatSetup, langchain4j_ollama
    from agentic_flink.tools import tool

    start_jvm()

    @tool
    def add(a: int, b: int) -> int:
        '''Add two numbers.'''
        return a + b

    agent = (
        Agent.builder()
            .with_id("calc-bot")
            .with_system_prompt("You are a calculator.")
            .with_chat_connection(langchain4j_ollama())
            .with_chat_setup(ChatSetup(model="qwen2.5:3b", temperature=0.3))
            .with_tools(add)
            .with_max_iterations(5)
            .build()
    )

    print(agent.id, agent.system_prompt)
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterable, Optional

from ._jvm import jclass
from .tools import PythonTool


class Agent:
    """Pythonic wrapper around a Java :class:`org.agentic.flink.dsl.Agent`.

    Returned by :meth:`AgentBuilder.build`. Hold the Java object behind a thin
    facade so callers see Python idioms.
    """

    def __init__(self, java_agent):
        self._java = java_agent

    # ---- accessors ------------------------------------------------------

    @property
    def java(self):
        """The underlying Java :class:`Agent` object — pass this to operators
        that take the framework's native type."""
        return self._java

    @property
    def id(self) -> str:
        return str(self._java.getAgentId())

    @property
    def name(self) -> str:
        return str(self._java.getAgentName()) if self._java.getAgentName() else ""

    @property
    def system_prompt(self) -> str:
        return str(self._java.getSystemPrompt())

    @property
    def max_iterations(self) -> int:
        return int(self._java.getMaxIterations())

    @property
    def temperature(self) -> float:
        return float(self._java.getTemperature())

    @property
    def allowed_tools(self) -> set[str]:
        java_set = self._java.getAllowedTools()
        return {str(t) for t in java_set}

    def __repr__(self) -> str:
        return f"Agent(id={self.id!r}, tools={sorted(self.allowed_tools)})"

    # ---- builder entry --------------------------------------------------

    @staticmethod
    def builder() -> "AgentBuilder":
        return AgentBuilder()


class AgentBuilder:
    """Pythonic fluent builder. Mirrors the Java
    :class:`org.agentic.flink.dsl.AgentBuilder` with snake_case names.

    Every ``with_*`` method returns ``self`` for chaining. Call :meth:`build`
    once you're done to materialize an :class:`Agent`.
    """

    def __init__(self):
        AgentJ = jclass("org.agentic.flink.dsl.Agent")
        self._b = AgentJ.builder()
        self._python_tools: list[PythonTool] = []
        self._state_machine_explicit = False

    # ---- identity -------------------------------------------------------

    def with_id(self, agent_id: str) -> "AgentBuilder":
        self._b = self._b.withId(agent_id)
        return self

    def with_name(self, name: str) -> "AgentBuilder":
        self._b = self._b.withName(name)
        return self

    def with_description(self, description: str) -> "AgentBuilder":
        self._b = self._b.withDescription(description)
        return self

    def with_type(self, agent_type: str) -> "AgentBuilder":
        """Set the agent type. One of ``"executor"``, ``"validator"``,
        ``"corrector"``, ``"supervisor"``, ``"coordinator"``, ``"researcher"``,
        ``"react"``, ``"custom"``."""
        AgentType = jclass("org.agentic.flink.dsl.Agent$AgentType")
        self._b = self._b.withType(AgentType.valueOf(agent_type.upper()))
        return self

    # ---- prompts --------------------------------------------------------

    def with_system_prompt(self, prompt: str) -> "AgentBuilder":
        self._b = self._b.withSystemPrompt(prompt)
        return self

    # ---- chat -----------------------------------------------------------

    def with_chat_connection(self, connection) -> "AgentBuilder":
        """Set the chat backend. Accepts either a raw Java
        :class:`ChatConnection` or a Python wrapper exposing
        :meth:`_to_java`."""
        java_conn = connection._to_java() if hasattr(connection, "_to_java") else connection
        self._b = self._b.withChatConnection(java_conn)
        return self

    def with_chat_setup(self, setup) -> "AgentBuilder":
        java_setup = setup._to_java() if hasattr(setup, "_to_java") else setup
        self._b = self._b.withChatSetup(java_setup)
        return self

    def with_output_schema(self, schema) -> "AgentBuilder":
        java_schema = schema._to_java() if hasattr(schema, "_to_java") else schema
        self._b = self._b.withOutputSchema(java_schema)
        return self

    # ---- tools ----------------------------------------------------------

    def with_tools(self, *tools_) -> "AgentBuilder":
        """Register one or more tools. Each tool may be a :class:`PythonTool`
        (from the ``@tool`` decorator) or a plain string tool name.

        The framework distinguishes tool *names* (declared on the agent's
        allow-list, via the Java ``withTools(String...)`` method) from tool
        *implementations* (registered against a ``ToolRegistry``). For
        :class:`PythonTool`s we add the name to the allow-list here and stash
        the implementation on :attr:`python_tools` for the caller to register
        in a ``ToolRegistry`` when wiring the job graph."""
        names: list[str] = []
        for t in tools_:
            if isinstance(t, PythonTool):
                names.append(t.name)
                self._python_tools.append(t)
            elif isinstance(t, str):
                names.append(t)
            else:
                raise TypeError(
                    f"with_tools expects PythonTool or str; got {type(t).__name__}"
                )
        if names:
            self._b = self._b.withTools(*names)
        return self

    def with_required_tools(self, *tool_names: str) -> "AgentBuilder":
        self._b = self._b.withRequiredTools(*tool_names)
        return self

    def with_max_iterations(self, n: int) -> "AgentBuilder":
        self._b = self._b.withMaxIterations(n)
        return self

    def with_timeout(self, duration: timedelta) -> "AgentBuilder":
        Duration = jclass("java.time.Duration")
        self._b = self._b.withTimeout(Duration.ofMillis(int(duration.total_seconds() * 1000)))
        return self

    def with_tool_timeout(self, duration: timedelta) -> "AgentBuilder":
        Duration = jclass("java.time.Duration")
        self._b = self._b.withToolTimeout(Duration.ofMillis(int(duration.total_seconds() * 1000)))
        return self

    def with_max_retries(self, n: int) -> "AgentBuilder":
        self._b = self._b.withMaxRetries(n)
        return self

    # ---- memory ---------------------------------------------------------

    def with_short_term_ttl(self, ttl: timedelta) -> "AgentBuilder":
        Duration = jclass("java.time.Duration")
        self._b = self._b.withShortTermTtl(
            Duration.ofMillis(int(ttl.total_seconds() * 1000))
        )
        return self

    def with_short_term_memory(self, spec) -> "AgentBuilder":
        java = spec._to_java() if hasattr(spec, "_to_java") else spec
        self._b = self._b.withShortTermMemory(java)
        return self

    def with_long_term_store(self, store) -> "AgentBuilder":
        self._b = self._b.withLongTermStore(store)
        return self

    def with_memory_channel(self, *channels) -> "AgentBuilder":
        java_channels = [c._to_java() if hasattr(c, "_to_java") else c for c in channels]
        if java_channels:
            self._b = self._b.withMemoryChannel(*java_channels)
        return self

    def with_vector_memory(self, spec) -> "AgentBuilder":
        java = spec._to_java() if hasattr(spec, "_to_java") else spec
        self._b = self._b.withVectorMemory(java)
        return self

    # ---- embedding ------------------------------------------------------

    def with_embedding_connection(self, connection) -> "AgentBuilder":
        java = connection._to_java() if hasattr(connection, "_to_java") else connection
        self._b = self._b.withEmbeddingConnection(java)
        return self

    def with_embedding_setup(self, setup) -> "AgentBuilder":
        java = setup._to_java() if hasattr(setup, "_to_java") else setup
        self._b = self._b.withEmbeddingSetup(java)
        return self

    # ---- listeners + skills + MCP --------------------------------------

    def with_listener(self, *listeners) -> "AgentBuilder":
        java = [l._to_java() if hasattr(l, "_to_java") else l for l in listeners]
        if java:
            self._b = self._b.withListener(*java)
        return self

    def with_skill(self, *skills) -> "AgentBuilder":
        java = [s._to_java() if hasattr(s, "_to_java") else s for s in skills]
        if java:
            self._b = self._b.withSkill(*java)
        return self

    def with_mcp_server(self, *servers) -> "AgentBuilder":
        java = [s._to_java() if hasattr(s, "_to_java") else s for s in servers]
        if java:
            self._b = self._b.withMcpServer(*java)
        return self

    # ---- inference / guardrails ----------------------------------------

    def with_inference_connection(self, name: str, connection) -> "AgentBuilder":
        java = connection._to_java() if hasattr(connection, "_to_java") else connection
        self._b = self._b.withInferenceConnection(name, java)
        return self

    def with_guardrail(self, *guardrails) -> "AgentBuilder":
        java = [g._to_java() if hasattr(g, "_to_java") else g for g in guardrails]
        if java:
            self._b = self._b.withGuardrail(*java)
        return self

    # ---- terminal -------------------------------------------------------

    @property
    def python_tools(self) -> list[PythonTool]:
        """Python tools registered via :meth:`with_tools`. Surface them to the
        caller so a ``ToolRegistry`` can be wired separately if needed."""
        return list(self._python_tools)

    def with_state_machine(self, state_machine) -> "AgentBuilder":
        """Set the agent's state machine. If unset, :meth:`build` supplies a
        permissive minimal one that satisfies the framework's validator
        (every non-terminal state has an outgoing transition)."""
        self._state_machine_explicit = True
        self._b = self._b.withStateMachine(
            state_machine._to_java() if hasattr(state_machine, "_to_java") else state_machine
        )
        return self

    def build(self) -> Agent:
        # The Java builder's default state machine doesn't cover every enum
        # value, which trips the validator. Supply a minimal-but-complete one
        # if the user hasn't set one explicitly.
        if not getattr(self, "_state_machine_explicit", False):
            self._b = self._b.withStateMachine(_default_minimal_state_machine())
        return Agent(self._b.build())


def _default_minimal_state_machine():
    """A permissive state machine that satisfies the framework validator.

    Every non-terminal ``AgentState`` (everything except ``COMPLETED``,
    ``FAILED``, ``COMPENSATED``) has at least one outgoing transition, and
    the initial state (``INITIALIZED``) is reachable from outside.
    Override via :meth:`AgentBuilder.with_state_machine` when you want
    real semantics.
    """
    import uuid as _uuid

    AgentStateMachine = jclass(
        "org.agentic.flink.statemachine.AgentStateMachine"
    )
    AgentTransition = jclass(
        "org.agentic.flink.statemachine.AgentTransition"
    )
    AgentState = jclass("org.agentic.flink.statemachine.AgentState")
    AgentEventType = jclass("org.agentic.flink.core.AgentEventType")

    def t(from_state, to_state, event):
        return AgentTransition.builder().from_(from_state).to(to_state).on(event).build()

    b = (
        AgentStateMachine.builder()
        .withId("sm-" + str(_uuid.uuid4()))
        .withInitialState(AgentState.INITIALIZED)
    )
    b.addTransition(t(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED))
    b.addTransition(t(AgentState.EXECUTING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED))
    b.addTransition(t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED))
    b.addTransition(t(AgentState.CORRECTING, AgentState.COMPLETED, AgentEventType.CORRECTION_COMPLETED))
    b.addTransition(t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED))
    b.addTransition(t(AgentState.PAUSED, AgentState.COMPLETED, AgentEventType.FLOW_RESUMED))
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED))
    b.addTransition(t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED))
    return b.build()


__all__ = ["Agent", "AgentBuilder"]
