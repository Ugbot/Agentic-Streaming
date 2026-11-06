"""High-level API: load one agentic/v1 workflow, run one turn, print the normalized result.

    python docs/snippets/python/high_level.py
"""
import json

from agentic import load

spec = load("spec/conformance/v1/workflows/support.yaml")
result = spec.run(runtime="local", text="what is my balance?", conversation_id="c1", turn_id="t1")

print(json.dumps({k: result[k] for k in ("status", "path", "reply", "tool_calls")}, sort_keys=True))
