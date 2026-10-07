"""``python -m agentic_nats``: the live stream -> worker -> KV -> reply round-trip."""

import asyncio

from agentic_nats import _demo

if __name__ == "__main__":
    asyncio.run(_demo())
