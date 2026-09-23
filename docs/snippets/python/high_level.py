"""High-level API: load one agentic/v1 workflow, run one turn, print the normalized result.

    python docs/snippets/python/high_level.py local        # pure Python, ports/pyagentic
    python docs/snippets/python/high_level.py local-jvm    # JVM core through the facade, python/
    python docs/snippets/python/high_level.py flink-jvm    # Flink MiniCluster through the facade
    python docs/snippets/python/high_level.py pyflink      # PyFlink job, agentic_pyflink registered as a runtime
"""
import json
import sys

runtime = sys.argv[1] if len(sys.argv) > 1 else "local"

if runtime == "local":
    from agentic import load  # ports/pyagentic: pure Python
else:
    from agentic_flink import load  # python/: the facade; its spec is accepted by every JVM-backed runtime

spec = load("spec/conformance/v1/workflows/support.yaml")
result = spec.run(runtime=runtime, text="what is my balance?", conversation_id="c1", turn_id="t1")

print(json.dumps({k: result[k] for k in ("status", "path", "reply", "tool_calls")}, sort_keys=True))
