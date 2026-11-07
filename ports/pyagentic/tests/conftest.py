"""Tests talk to local stubs. Nothing else is allowed, so the egress refusals can still be proved."""

import os

os.environ.setdefault("AGENTIC_EGRESS_ALLOW_HOSTS", "127.0.0.1,localhost")
os.environ.setdefault("AGENTIC_EGRESS_ALLOW_PRIVATE", "true")
