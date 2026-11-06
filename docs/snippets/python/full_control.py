"""Full-control API: construct the runtime, deploy, submit, restart, submit the same turn, close.

    python docs/snippets/python/full_control.py
"""
import json

from agentic import load
from agentic.runtime import LocalRuntime, Turn

event = Turn(conversation_id="c1", turn_id="t1", text="what is my balance?")

rt = LocalRuntime()
rt.deploy(load("spec/conformance/v1/workflows/support.yaml"))
first = rt.submit(event)
# Drop every materialized view; only the event log survives. restart() returns a fresh instance
# over the same log, so rebind it.
rt = rt.restart()
again = rt.submit(event)  # same turn_id after the restart: answered as a duplicate, nothing re-runs
rt.close()


def show(result):
    return {k: result[k] for k in ("status", "path", "reply")}


print(json.dumps({"first": show(first), "after_restart": show(again)}, sort_keys=True))
