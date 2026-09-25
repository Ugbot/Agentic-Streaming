#!/usr/bin/env bash
# Idempotently creates the agentic-flink-network external Podman network that every compose
# file expects. Safe to run multiple times. Call it once on a fresh machine, or let the
# scenario-specific run-*.sh wrappers call it for you.
#
# Prerequisites: Podman.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

PODMAN_NETWORK="${NETWORK:-$PODMAN_NETWORK}"
ensure_podman_network
