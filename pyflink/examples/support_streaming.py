"""Long-running mode: deploy the shared support workflow once, feed turns one at a time, restart
from a savepoint half-way and show that conversation state survived.

    ~/.venv-pyflink/bin/python pyflink/examples/support_streaming.py

Local MiniCluster with a file spool; the same code targets a real cluster with
``FlinkConfig(mode="cluster", rest_address=..., savepoint_dir="s3://...")`` and Kafka connectors
(see docs/python.md, "Cluster submission").
"""

from __future__ import annotations

from pathlib import Path

from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

REPO = Path(__file__).resolve().parents[2]


def main() -> None:
    spec = load_workflow(REPO / "spec" / "conformance" / "v1" / "workflows" / "support.yaml")
    with FlinkRuntime(FlinkConfig(mode="local", checkpoint_interval="200ms")) as rt:
        rt.deploy(spec)
        r1 = rt.submit({"conversation_id": "c1", "turn_id": "t1", "text": "I lost my password"})
        print("t1:", r1["status"], r1["path"], r1["state"])
        rt.restart()
        r2 = rt.submit({"conversation_id": "c1", "turn_id": "t2", "text": "what did I just ask?"})
        print("t2 after restart:", r2["status"], r2["path"], r2["state"])
        r3 = rt.submit({"conversation_id": "c1", "turn_id": "t2", "text": "what did I just ask?"})
        print("t2 redelivered:", r3["status"], "events appended:", len(r3["events"]))


if __name__ == "__main__":
    main()
