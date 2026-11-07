"""CLI: build a pipeline.yaml on the chosen backend and run turns through it.

    python -m agentic_pipeline run banking.yaml --text "what is my balance?"
    python -m agentic_pipeline run banking.yaml --backend local --conv c1 --text "hi"
    python -m agentic_pipeline run banking.yaml --store-dir state --conv c1 --turn t1 --text "hi"

`--turn` is the idempotency key: delivering it again answers `duplicate`. Without one, every run is
a new turn. `--store-dir` keeps the event log and the effect journal on disk between runs.
"""

from __future__ import annotations

import argparse
import sys

from agentic.errors import AgenticError

from .backends import Event, backend_names
from .loader import load


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="agentic_pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run a turn through a pipeline.yaml")
    run.add_argument("pipeline", help="path to a pipeline.yaml")
    run.add_argument(
        "--backend",
        default=None,
        help=f"override backend ({', '.join(backend_names())})",
    )
    run.add_argument("--text", default="what is my balance?", help="the turn text")
    run.add_argument("--conv", default="c1", help="conversation id")
    run.add_argument("--user", default="demo", help="user id")
    run.add_argument("--turn", default=None, help="turn id, the idempotency key (default: a new one)")
    run.add_argument("--store-dir", default=None, help="keep the event log and journal in this directory")
    args = parser.parse_args(argv)

    try:
        system = load(args.pipeline, backend=args.backend, store_dir=args.store_dir)
    except (ValueError, AgenticError) as exc:
        print(f"agentic_pipeline: {exc}", file=sys.stderr)
        return 2
    try:
        if system.legacy_features:
            print(f"agentic_pipeline: this pipeline needs {', '.join(system.legacy_features)}, so it runs on the "
                  "legacy engine: no event log, no turn_id, no effect journal", file=sys.stderr)
            if args.turn is not None:
                print("agentic_pipeline: --turn needs the core, and this pipeline is on the legacy engine",
                      file=sys.stderr)
                return 2
            res = system.submit(_legacy_event(args))
            print(f"backend={system.backend_name} path={res.path} ok={res.ok}")
        else:
            res = system.submit(Event(args.conv, args.text, args.user, turn_id=args.turn))
            print(f"backend={system.backend_name} path={res.path} ok={res.ok} status={res.status}")
        print(f"reply: {res.reply}")
        if res.tool_calls:
            print(f"tools: {res.tool_calls}")
    finally:
        system.backend.close()
    return 0


def _legacy_event(args):
    from pyagentic.core import Event as LegacyEvent

    return LegacyEvent(conversation_id=args.conv, text=args.text, user_id=args.user)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
