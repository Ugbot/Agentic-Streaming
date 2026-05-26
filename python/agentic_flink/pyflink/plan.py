"""Build an :class:`AgentPlan` JSON payload from a decorated :class:`Agent` instance."""

from __future__ import annotations

import inspect
import json
from typing import Any, Dict, List

from .agent import Agent
from .decorators import ACTION_ATTR, CHAT_ATTR, LISTENER_ATTR, TOOL_ATTR
from .resource import ResourceRef, _ActionSpec, _ListenerSpec, _ToolSpec
from .serialize import encode


def build_plan(agent: Agent) -> Dict[str, Any]:
    """Walk decorated methods and return the plan as a Python dict.

    The dict is the canonical JSON shape the Java side expects; serialize with
    ``json.dumps(build_plan(agent))`` when handing it to ``CompileUtils``.
    """

    tools: List[_ToolSpec] = []
    actions: List[_ActionSpec] = []
    listeners: List[_ListenerSpec] = []

    # Walk every callable on the class (including inherited).
    for cls in type(agent).__mro__:
        # Snapshot the mapping — getattr on bound methods can mutate __dict__ in
        # some Python versions (cached method objects), which would otherwise
        # raise "dictionary changed size during iteration".
        for name, member in list(cls.__dict__.items()):
            if not callable(member):
                continue
            if hasattr(member, TOOL_ATTR):
                meta = getattr(member, TOOL_ATTR)
                if any(t.name == meta["name"] for t in tools):
                    continue  # subclass override wins; first hit (most-derived) kept
                bound = getattr(agent, name)
                tools.append(
                    _ToolSpec(
                        kind="python",
                        name=meta["name"],
                        description=meta["description"],
                        cloudpickle_b64=encode(bound),
                        param_names=list(meta["param_names"]),
                    )
                )
            elif hasattr(member, ACTION_ATTR):
                meta = getattr(member, ACTION_ATTR)
                if any(a.name == meta["name"] for a in actions):
                    continue
                bound = getattr(agent, name)
                actions.append(
                    _ActionSpec(
                        name=meta["name"],
                        events=list(meta["events"]),
                        cloudpickle_b64=encode(bound),
                    )
                )
            elif hasattr(member, LISTENER_ATTR):
                bound = getattr(agent, name)
                listeners.append(
                    _ListenerSpec(kind="python", cloudpickle_b64=encode(bound))
                )
            elif hasattr(member, CHAT_ATTR):
                # If the user used @chat_model_connection, the spec lives on the marker;
                # override only if the agent hasn't set chat_connection imperatively.
                if agent.chat_connection is None:
                    spec = getattr(member, CHAT_ATTR)["spec"]
                    if isinstance(spec, ResourceRef):
                        agent.chat_connection = spec

    plan: Dict[str, Any] = {
        "agent_id": agent.get_agent_id(),
    }
    if agent.system_prompt:
        plan["system_prompt"] = agent.system_prompt
    if agent.chat_connection:
        plan["chat_connection"] = agent.chat_connection.to_dict()
    if agent.chat_setup:
        plan["chat_setup"] = {k: str(v) for k, v in agent.chat_setup.items()}
    plan["tools"] = [t.to_dict() for t in tools]
    plan["actions"] = [a.to_dict() for a in actions]
    plan["resources"] = {k: v.to_dict() for k, v in agent.resources.items()}
    plan["listeners"] = [l.to_dict() for l in listeners]
    return plan


def build_plan_json(agent: Agent) -> str:
    return json.dumps(build_plan(agent))
