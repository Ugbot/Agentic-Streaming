#!/usr/bin/env bash
# Idempotently creates the agentic-flink-network external network that every compose file
# expects. Safe to run multiple times. Call it once on a fresh machine, or let the
# scenario-specific run-*.sh wrappers call it for you.

set -euo pipefail

NETWORK="${NETWORK:-agentic-flink-network}"

ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }
info(){ printf '\033[34m→\033[0m %s\n' "$*"; }
err() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }

if ! command -v podman >/dev/null 2>&1; then
  err "podman not on PATH. Install with: brew install podman"
  exit 1
fi

if podman network exists "$NETWORK" >/dev/null 2>&1; then
  ok "network $NETWORK already exists"
else
  info "creating podman network $NETWORK"
  podman network create "$NETWORK" >/dev/null
  ok "network $NETWORK created"
fi
