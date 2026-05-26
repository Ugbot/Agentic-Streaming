"""AgentEventListener proxy — implement listeners in Python.

Subclass :class:`PyAgentEventListener` and override the hooks you care
about; the rest stay as no-ops via the Java interface defaults. Pass the
instance to :meth:`AgentBuilder.with_listener`.

Example::

    class MyListener(PyAgentEventListener):
        def on_chat_response(self, agent_id, model, length, tokens):
            print(f"{agent_id}: {model} returned {length} chars")

    agent.builder().with_listener(MyListener()).build()
"""

from __future__ import annotations

import jpype

from ._jvm import jclass


def _impl():
    """Build the JPype-implemented anonymous class on first call. Lazy because
    importing this module must not require the JVM."""
    AEL = jclass("org.agentic.flink.listener.AgentEventListener")

    @jpype.JImplements(AEL)
    class _PyListenerImpl:
        def __init__(self, owner):
            self._owner = owner

        # ---- chat lifecycle ----
        @jpype.JOverride
        def onAgentStart(self, agent_id):
            self._owner.on_agent_start(str(agent_id))

        @jpype.JOverride
        def onChatRequest(self, agent_id, model_name, message_count):
            self._owner.on_chat_request(str(agent_id), str(model_name), int(message_count))

        @jpype.JOverride
        def onChatResponse(self, agent_id, model_name, response_length, tokens_used):
            self._owner.on_chat_response(
                str(agent_id),
                str(model_name),
                int(response_length),
                int(tokens_used) if tokens_used is not None else None,
            )

        # ---- tool lifecycle ----
        @jpype.JOverride
        def onToolCallStart(self, agent_id, tool_name, tool_call_id):
            self._owner.on_tool_call_start(str(agent_id), str(tool_name), str(tool_call_id))

        @jpype.JOverride
        def onToolCallEnd(self, agent_id, tool_name, tool_call_id, success, duration_ms):
            self._owner.on_tool_call_end(
                str(agent_id),
                str(tool_name),
                str(tool_call_id),
                bool(success),
                int(duration_ms),
            )

        # ---- memory lifecycle ----
        @jpype.JOverride
        def onCompaction(self, agent_id, flow_id, items_before, items_after, duration_ms):
            self._owner.on_compaction(
                str(agent_id), str(flow_id), int(items_before), int(items_after), int(duration_ms)
            )

        @jpype.JOverride
        def onLongTermSync(self, agent_id, flow_id, facts_written):
            self._owner.on_long_term_sync(str(agent_id), str(flow_id), int(facts_written))

        @jpype.JOverride
        def onError(self, agent_id, stage, error):
            self._owner.on_error(str(agent_id), str(stage), str(error.getMessage()) if error else "")

        # ---- inference + guardrails ----
        @jpype.JOverride
        def onInference(self, agent_id, model_name, task, duration_ms):
            self._owner.on_inference(
                str(agent_id), str(model_name), str(task), int(duration_ms)
            )

        @jpype.JOverride
        def onGuardrailBlock(self, agent_id, model_name, label):
            self._owner.on_guardrail_block(str(agent_id), str(model_name), str(label))

        @jpype.JOverride
        def onGuardrailRewrite(self, agent_id, model_name, reason):
            self._owner.on_guardrail_rewrite(str(agent_id), str(model_name), str(reason))

        @jpype.JOverride
        def name(self):
            return self._owner.name

    return _PyListenerImpl


class PyAgentEventListener:
    """Override the hooks you care about. All methods are no-ops by default.

    To pass an instance to the agent: ``builder.with_listener(my_listener)``
    — the wrapper calls :meth:`_to_java` automatically.
    """

    name: str = "PyAgentEventListener"

    # User-overridable hooks (snake_case mirrors of the Java interface).
    def on_agent_start(self, agent_id: str) -> None: pass
    def on_chat_request(self, agent_id: str, model: str, message_count: int) -> None: pass
    def on_chat_response(self, agent_id: str, model: str, response_length: int, tokens_used: int | None) -> None: pass
    def on_tool_call_start(self, agent_id: str, tool_name: str, tool_call_id: str) -> None: pass
    def on_tool_call_end(self, agent_id: str, tool_name: str, tool_call_id: str, success: bool, duration_ms: int) -> None: pass
    def on_compaction(self, agent_id: str, flow_id: str, items_before: int, items_after: int, duration_ms: int) -> None: pass
    def on_long_term_sync(self, agent_id: str, flow_id: str, facts_written: int) -> None: pass
    def on_error(self, agent_id: str, stage: str, error: str) -> None: pass
    def on_inference(self, agent_id: str, model: str, task: str, duration_ms: int) -> None: pass
    def on_guardrail_block(self, agent_id: str, model: str, label: str) -> None: pass
    def on_guardrail_rewrite(self, agent_id: str, model: str, reason: str) -> None: pass

    def _to_java(self):
        if not hasattr(self, "_java"):
            self._java = _impl()(self)
        return self._java


__all__ = ["PyAgentEventListener"]
