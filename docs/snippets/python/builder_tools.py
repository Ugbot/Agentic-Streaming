"""High-level builder: the same support workflow written in Python, with a Python function as a tool.

    python docs/snippets/python/builder_tools.py
"""
import json

from agentic import Agent


def lookup_charge(user: str) -> float:
    """Look up the most recent charge for a user."""
    return 42.5


spec = (
    Agent("support")
    .route(kind="keyword", rules={"billing": ["refund", "charge", "balance"]}, default="general")
    .path("billing", brain="rule", tools=["lookup_charge"], tool_triggers={"balance": "lookup_charge"})
    .path("general", brain="rule")
    .use_tool("lookup_charge", lookup_charge)
    .with_memory(conversation="memory")
    .policies(ordering="per-conversation", idempotency="turn-id")
    .build()
)

result = spec.run(runtime="local", text="what is my balance?", conversation_id="c1", turn_id="t1")
print(json.dumps({k: result[k] for k in ("status", "path", "reply")}, sort_keys=True))
