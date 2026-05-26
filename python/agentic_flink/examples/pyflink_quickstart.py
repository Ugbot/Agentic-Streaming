"""PyFlink-native quickstart for Agentic Flink.

A two-event triage agent: one ``@tool`` (intent classification) and one
``@action`` that the operator dispatches per element. The agent definition is
serialized as an ``AgentPlan`` and run by ``AgentPlanProcessFunction`` in the
JVM; the Python callbacks are invoked through PEMJA on the operator thread.

Run::

    flink run -py path/to/pyflink_quickstart.py

…against a Flink 1.20+ cluster with the agentic-flink jar on the classpath.
"""

from __future__ import annotations

import json

from agentic_flink.pyflink import (
    Agent,
    ResourceRef,
    action,
    environment,
    tool,
)


class TriageAgent(Agent):
    agent_id = "triage-quickstart"
    system_prompt = "You triage support tickets."
    chat_setup = {"model": "qwen2.5:3b", "temperature": "0.2"}

    # Wire any Java ChatConnection via FQN + config. For local dev:
    chat_connection = ResourceRef(
        "org.agentic.flink.llm.langchain4j.LangChain4jChatConnection",
        {"provider": "OLLAMA", "base_url": "http://localhost:11434"},
    )

    @tool
    def classify_intent(self, text: str) -> str:
        low = text.lower()
        if "refund" in low or "charge" in low:
            return "billing"
        if "error" in low or "broken" in low:
            return "technical"
        return "general"

    @action("ticket")
    def draft_reply(self, event, ctx):
        intent = self.classify_intent(event["body"])
        return {
            "id": event["id"],
            "intent": intent,
            "agent": ctx.get("agent_id"),
        }


def main():
    from pyflink.datastream import StreamExecutionEnvironment

    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)

    tickets = s_env.from_collection(
        [
            {"type": "ticket", "id": "t-1", "body": "Please issue a refund."},
            {"type": "ticket", "id": "t-2", "body": "App is broken on startup."},
            {"type": "ticket", "id": "t-3", "body": "How do I change my plan?"},
        ]
    )

    ae = environment(s_env)
    answers = (
        ae.from_datastream(tickets, key_selector=lambda t: t["id"])
        .apply(TriageAgent())
        .to_datastream()
    )
    answers.print()

    s_env.execute("triage-quickstart")


def show_plan():
    """Dump the plan JSON to stdout — useful for sanity-checking offline."""
    ae_agent = TriageAgent()
    from agentic_flink.pyflink.plan import build_plan

    print(json.dumps(build_plan(ae_agent), indent=2))


if __name__ == "__main__":
    import sys

    if "--show-plan" in sys.argv:
        show_plan()
    else:
        main()
