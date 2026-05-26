"""PyFlink-native entry point for Agentic Flink.

This subpackage is the recommended path when you want to ship a Python-
defined agent as part of a real PyFlink job (``flink run -py …``). The
agent definition is collected into an :class:`AgentPlan` (JSON), shipped
to the JVM through PyFlink's existing Py4J gateway, and run by a Java
``AgentPlanProcessFunction`` that invokes Python callbacks via PEMJA.

The JPype standalone path (``import agentic_flink as af; af.start_jvm()``)
is unchanged and lives at the top level of the package — it is a
different use case (notebooks, ad-hoc scripts, no PyFlink dependency).
"""

from __future__ import annotations

from .agent import Agent
from .decorators import action, chat_model_connection, listener, tool
from .environment import AgentsExecutionEnvironment, environment
from .plan import build_plan
from .resource import ResourceRef

__all__ = [
    "Agent",
    "AgentsExecutionEnvironment",
    "ResourceRef",
    "action",
    "build_plan",
    "chat_model_connection",
    "environment",
    "listener",
    "tool",
]
