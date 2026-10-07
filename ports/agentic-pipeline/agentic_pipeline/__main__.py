"""CLI: build a pipeline.yaml on the chosen backend and run turns through it.

    python -m agentic_pipeline run banking.yaml --text "what is my balance?"
    python -m agentic_pipeline run banking.yaml --backend celery --conv c1 --text "hi"
"""

from __future__ import annotations

import argparse
import sys

from pyagentic.core import Event

from .backends import BackendUnavailableError, backend_names, demonstration_only_names
from .loader import load


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="agentic_pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run a turn through a pipeline.yaml")
    run.add_argument("pipeline", help="path to a pipeline.yaml")
    run.add_argument(
        "--backend",
        default=None,
        help=f"override backend ({', '.join(backend_names())}); demonstration-only adapters that are "
        f"rejected: {', '.join(demonstration_only_names())}",
    )
    run.add_argument("--text", default="what is my balance?", help="the turn text")
    run.add_argument("--conv", default="c1", help="conversation id")
    run.add_argument("--user", default="demo", help="user id")
    args = parser.parse_args(argv)

    try:
        system = load(args.pipeline, backend=args.backend)
    except (ValueError, BackendUnavailableError) as exc:
        print(f"agentic_pipeline: {exc}", file=sys.stderr)
        return 2
    try:
        res = system.submit(Event(conversation_id=args.conv, text=args.text, user_id=args.user))
        print(f"backend={system.backend_name} path={res.path} ok={res.ok}")
        print(f"reply: {res.reply}")
        if res.tool_calls:
            print(f"tools: {res.tool_calls}")
    finally:
        system.backend.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
