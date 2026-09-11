"""``python -m agentic_pyflink run <workflow.yaml> --text "..."``: one turn on the local MiniCluster.

The Python twin of ``FlinkPipelineRunner.main`` and ``org.jagentic.pekko.PipelineMain``: same
document in, same normalized result document out.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from .config import FlinkConfig
from .runtime import FlinkRuntime
from .workflow import load_workflow


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m agentic_pyflink")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run one turn through a workflow document on the local MiniCluster")
    run.add_argument("workflow", help="examples/pipelines/*.yaml or any agentic/v1 document")
    run.add_argument("--text", default="what is my balance?")
    run.add_argument("--conversation-id", default="c1")
    run.add_argument("--turn-id", default="t1")
    run.add_argument("--user-id", default="anonymous")
    run.add_argument("--parallelism", type=int, default=1)
    run.add_argument("--state-backend", default="hashmap", choices=("hashmap", "rocksdb", "forst"))
    run.add_argument("--checkpoint-interval", default="500ms")
    args = parser.parse_args(argv)

    spec = load_workflow(args.workflow)
    config = FlinkConfig(
        mode="local",
        parallelism=args.parallelism,
        state_backend=args.state_backend,
        checkpoint_interval=args.checkpoint_interval,
    )
    turn = {
        "conversation_id": args.conversation_id,
        "turn_id": args.turn_id,
        "user_id": args.user_id,
        "text": args.text,
    }
    with FlinkRuntime(config) as rt:
        for result in rt.run(spec, [turn]):
            print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
