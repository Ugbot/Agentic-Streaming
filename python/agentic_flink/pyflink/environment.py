"""``AgentsExecutionEnvironment``: PyFlink-native entry point.

Usage::

    s_env = StreamExecutionEnvironment.get_execution_environment()
    ae = environment(s_env)
    out = (
        ae.from_datastream(tickets, key_selector=lambda t: t["id"])
          .apply(MyAgent())
          .to_datastream()
    )
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .agent import Agent
from .bridge import attach_agent
from .plan import build_plan_json


class AgentsExecutionEnvironment:
    """Wraps a PyFlink ``StreamExecutionEnvironment``.

    Holds no state of its own; the methods are thin builders that return a
    :class:`AgentStream` configured for one or more agent attachments.
    """

    def __init__(self, s_env: Any):
        self._s_env = s_env

    def from_datastream(
        self,
        data_stream: Any,
        key_selector: Optional[Callable[[Any], Any]] = None,
    ) -> "AgentStream":
        return AgentStream(self._s_env, data_stream, key_selector)


def environment(s_env: Any) -> AgentsExecutionEnvironment:
    return AgentsExecutionEnvironment(s_env)


class AgentStream:
    """Builder that pairs a PyFlink DataStream with an :class:`Agent`."""

    def __init__(
        self,
        s_env: Any,
        data_stream: Any,
        key_selector: Optional[Callable[[Any], Any]],
    ):
        self._s_env = s_env
        self._data_stream = data_stream
        self._key_selector = key_selector
        self._agent: Optional[Agent] = None
        self._j_output = None

    def apply(self, agent: Agent) -> "AgentStream":
        self._agent = agent
        return self

    def to_datastream(self) -> Any:
        """Materialize the agent operator and return the resulting DataStream.

        Requires PyFlink (raised by :mod:`bridge` if absent). The result is the
        same PyFlink type the input was — callers can keep chaining ``.map``,
        ``.sink_to`` etc.
        """
        if self._agent is None:
            raise RuntimeError("AgentStream.apply(agent) must be called before to_datastream()")

        plan_json = build_plan_json(self._agent)

        try:
            from pyflink.common.typeinfo import Types
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "PyFlink is required to materialize an agent stream. "
                "Install with: pip install 'agentic-flink[pyflink]'"
            ) from e

        keyed = self._data_stream
        if self._key_selector is not None:
            keyed = self._data_stream.key_by(self._key_selector)

        # Hand the underlying Java KeyedStream to CompileUtils.
        j_keyed = keyed._j_data_stream
        j_out = attach_agent(j_keyed, plan_json)

        # Re-wrap the returned Java DataStream as a PyFlink DataStream.
        from pyflink.datastream import DataStream

        ds = DataStream(j_out, Types.PICKLED_BYTE_ARRAY())
        self._j_output = j_out
        return ds

    @property
    def plan_json(self) -> str:
        """The plan JSON that would be sent to the JVM. Useful for debugging."""
        if self._agent is None:
            raise RuntimeError("No agent applied yet")
        return build_plan_json(self._agent)
