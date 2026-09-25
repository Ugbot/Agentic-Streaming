#!/usr/bin/env bash
# Starts the Ollama service from docker-compose.yml in Podman and makes sure OLLAMA_MODEL is
# pulled. This is the same container the RAG and full stacks use, so the two never collide.
#
# Prerequisites: Podman with `podman compose` or podman-compose, curl, outbound internet for
# the image and the first model pull. Idempotent: a running agentic-flink-ollama is reused.
# The server listens on 127.0.0.1:11434; models persist in the compose ollama_data volume.
# Only the ollama service is started, so POSTGRES_PASSWORD and REDIS_PASSWORD (required by the
# other services in the same file) are filled with placeholders when unset.
#
#   bash examples-bin/run-ollama.sh                 # qwen2.5:3b (default)
#   OLLAMA_MODEL=nomic-embed-text bash examples-bin/run-ollama.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

OLLAMA_PORT="${OLLAMA_PORT:-11434}"

require_podman
require_curl
ensure_podman_network

if podman container exists "$OLLAMA_CONTAINER" >/dev/null 2>&1 \
   && [ "$(podman inspect -f '{{.State.Running}}' "$OLLAMA_CONTAINER")" = "true" ]; then
  ok "container $OLLAMA_CONTAINER already running"
else
  info "starting the ollama service from docker-compose.yml as $OLLAMA_CONTAINER"
  POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-unused}" REDIS_PASSWORD="${REDIS_PASSWORD:-unused}" \
    podman_compose -f docker-compose.yml up -d ollama
fi

wait_for_port 127.0.0.1 "$OLLAMA_PORT" 60 "Ollama"
check_ollama
