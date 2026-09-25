#!/usr/bin/env bash
# Tears down every service brought up by any of the scenario composes plus the standalone
# Ollama container started by run-ollama.sh. Safe to run when only some services are up.
# Keeps the agentic-flink-network and the Ollama model volume; remove them with
# `podman network rm agentic-flink-network` and `podman volume rm agentic-flink-ollama`.
#
# Prerequisites: Podman with `podman compose` or podman-compose.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

info "stopping everything from docker-compose-all.yml"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-unused}" REDIS_PASSWORD="${REDIS_PASSWORD:-unused}" \
  podman_compose -f docker-compose-all.yml down --remove-orphans 2>/dev/null || true

if podman container exists "$OLLAMA_CONTAINER" >/dev/null 2>&1; then
  info "removing container $OLLAMA_CONTAINER"
  podman rm -f "$OLLAMA_CONTAINER" >/dev/null
fi

ok "all services stopped"
