"""Run ``examples/pipelines/banking.yaml`` -- the very document the JVM, Pekko and Clojure runtimes
run -- as a PyFlink job on the local MiniCluster, then show the normalized results.

    ~/.venv-pyflink/bin/python pyflink/examples/banking_local.py

Prerequisites: the JVM side built (see docs/python.md, "Build") and ``pip install -e pyflink``.
"""

from __future__ import annotations

import json
from pathlib import Path

from agentic_pyflink import FlinkConfig, FlinkRuntime, load_workflow

REPO = Path(__file__).resolve().parents[2]


def main() -> None:
    spec = load_workflow(REPO / "examples" / "pipelines" / "banking.yaml")
    config = FlinkConfig(mode="local", parallelism=2, checkpoint_interval="1s", state_backend="hashmap")
    turns = [
        {"conversation_id": "alice", "turn_id": "t1", "text": "what is my balance?"},
        {"conversation_id": "alice", "turn_id": "t1", "text": "what is my balance?"},  # redelivery
        {"conversation_id": "bob", "turn_id": "t2", "text": "I lost my card"},
    ]
    with FlinkRuntime(config) as rt:
        print("capabilities:", json.dumps(rt.capabilities(), sort_keys=True))
        for result in rt.run(spec, turns):
            print(f"{result['conversation_id']}/{result['turn_id']} {result['status']:9} "
                  f"path={result['path']!s:9} tools={[c['tool'] for c in result['tool_calls']]} "
                  f"reply={result['reply']!r}")


if __name__ == "__main__":
    main()
