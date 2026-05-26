"""Python ``Agent`` base class for the PyFlink-native path.

Mirrors upstream Apache Flink Agents' API: subclass :class:`Agent`, decorate
methods with ``@tool`` / ``@action`` / ``@listener``, override or assign
``chat_connection`` / ``system_prompt`` / ``resources``. The plan builder turns
the decorated subclass into an :class:`AgentPlan` JSON string.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .resource import ResourceRef


class Agent:
    """Base class for declarative Python agents.

    Subclasses set class- or instance-level attributes:

    * ``agent_id`` (str) — required; defaults to the class name.
    * ``system_prompt`` (str) — optional system prompt.
    * ``chat_connection`` (:class:`ResourceRef`) — optional; FQN of a Java
      ``ChatConnection`` plus its init config.
    * ``chat_setup`` (dict[str, str]) — optional; serialized into the plan
      verbatim; the Java side maps it onto ``ChatSetup``.
    * ``resources`` (dict[str, :class:`ResourceRef`]) — named Java SPIs the
      operator should bind in ``open()`` (embedder, corpus, vector memory, …).
    """

    agent_id: Optional[str] = None
    system_prompt: Optional[str] = None
    chat_connection: Optional[ResourceRef] = None
    chat_setup: Dict[str, str] = {}
    resources: Dict[str, ResourceRef] = {}

    def get_agent_id(self) -> str:
        return self.agent_id or type(self).__name__

    # Convenience setters so users can build agents imperatively without subclassing.
    def with_id(self, agent_id: str) -> "Agent":
        self.agent_id = agent_id
        return self

    def with_system_prompt(self, prompt: str) -> "Agent":
        self.system_prompt = prompt
        return self

    def with_chat_connection(self, ref: ResourceRef) -> "Agent":
        self.chat_connection = ref
        return self

    def with_chat_setup(self, **kwargs: Any) -> "Agent":
        self.chat_setup = {k: str(v) for k, v in kwargs.items()}
        return self

    def with_resource(self, name: str, ref: ResourceRef) -> "Agent":
        # Copy on write so subclass-level defaults aren't mutated across instances.
        new = dict(self.resources)
        new[name] = ref
        self.resources = new
        return self
