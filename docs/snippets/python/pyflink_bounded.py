"""PyFlink bounded run: a list of turns in, normalized results out, the job ends when drained.

    python docs/snippets/python/pyflink_bounded.py
"""
import json

from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

spec = load_workflow("spec/conformance/v1/workflows/support.yaml")
with FlinkRuntime(FlinkConfig(mode="local", parallelism=1)) as rt:
    results = rt.run(spec, [{"conversation_id": "c1", "turn_id": "t1", "text": "what is my balance?"}])
print(json.dumps({k: results[0][k] for k in ("status", "path", "reply")}, sort_keys=True))
