"""CLI: build a pipeline.yaml on the chosen backend and run turns through it.

    python -m agentic_pipeline run banking.yaml --text "what is my balance?"
    python -m agentic_pipeline run banking.yaml --backend celery --conv c1 --text "hi"
"""

from __future__ import annotations

import argparse
import sys

from pyagentic.core import Event

from .backends import backend_names
from .loader import load


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="agentic_pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run a turn through a pipeline.yaml")
    run.add_argument("pipeline", help="path to a pipeline.yaml")
    run.add_argument("--backend", default=None, help=f"override backend ({', '.join(backend_names())})")
    run.add_argument("--text", default="what is my balance?", help="the turn text")
    run.add_argument("--conv", default="c1", help="conversation id")
    run.add_argument("--user", default="demo", help="user id")
    args = parser.parse_args(argv)

    system = load(args.pipeline, backend=args.backend)
    res = system.submit(Event(args.conv, args.text, args.user))
    print(f"backend={system.backend_name} path={res.path} ok={res.ok}")
    print(f"reply: {res.reply}")
    if res.tool_calls:
        print(f"tools: {res.tool_calls}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
