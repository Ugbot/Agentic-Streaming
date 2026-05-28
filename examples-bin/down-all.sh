#!/usr/bin/env bash
# Tears down every service brought up by any of the scenario composes. Safe to run when
# only some services are up. Doesn't remove the agentic-flink-network (run
# `podman network rm agentic-flink-network` if you want a full reset).

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

info(){ printf '\033[34m→\033[0m %s\n' "$*"; }
ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }

info "stopping everything from docker-compose-all.yml"
podman compose -f "$REPO/docker-compose-all.yml" down --remove-orphans 2>/dev/null || true

ok "all services stopped. Run \`podman network rm agentic-flink-network\` to remove the network."
