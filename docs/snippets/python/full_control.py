"""Full-control API: construct the runtime, deploy, submit, restart, submit the same turn, close.

    python docs/snippets/python/full_control.py local        # agentic.runtime.LocalRuntime
    python docs/snippets/python/full_control.py local-jvm    # agentic_flink.JvmLocalRuntime
    python docs/snippets/python/full_control.py flink-jvm    # agentic_flink.FlinkRuntime
    python docs/snippets/python/full_control.py pyflink      # agentic_pyflink.FlinkRuntime
"""
import json
import sys

runtime = sys.argv[1] if len(sys.argv) > 1 else "local"
workflow = "spec/conformance/v1/workflows/support.yaml"
turn = {"conversation_id": "c1", "turn_id": "t1", "text": "what is my balance?"}

if runtime == "local":
    from agentic import load
    from agentic.runtime import LocalRuntime, Turn

    rt = LocalRuntime()
    spec = load(workflow)
    event = Turn(**turn)
elif runtime == "local-jvm":
    from agentic_flink import Event, JvmLocalRuntime, load

    rt = JvmLocalRuntime()
    spec = load(workflow)
    event = Event(**turn)
elif runtime == "flink-jvm":
    from agentic_flink import Event, FlinkRuntime, load

    rt = FlinkRuntime(parallelism=1, checkpoint_interval="1s")
    spec = load(workflow)
    event = Event(**turn)
else:
    from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

    rt = FlinkRuntime(FlinkConfig(mode="local", parallelism=1, checkpoint_interval="1s"))
    spec = load_workflow(workflow)
    event = turn

rt.deploy(spec)
first = rt.submit(event)
# Drop every materialized view; only the event log survives. The pure Python LocalRuntime
# returns a fresh instance over the same log; the JVM-backed runtimes restart in place.
rt = rt.restart() or rt
again = rt.submit(event)  # same turn_id after the restart: answered as a duplicate, nothing re-runs
rt.close()


def show(result):
    return {k: result[k] for k in ("status", "path", "reply")}


print(json.dumps({"first": show(first), "after_restart": show(again)}, sort_keys=True))
