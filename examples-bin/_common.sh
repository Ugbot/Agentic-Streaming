#!/usr/bin/env bash
# Shared bootstrap for run-*.sh scripts.
# Verifies Ollama is reachable, the requested model is pulled, and re-exec's
# Maven with the example's main class.

set -euo pipefail

OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-qwen2.5:3b}"

err() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }
ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }
info(){ printf '\033[34m→\033[0m %s\n' "$*"; }

check_ollama() {
  info "checking Ollama at $OLLAMA_URL"
  if ! curl -sf --max-time 2 "$OLLAMA_URL/api/tags" >/dev/null; then
    err "Ollama is not reachable at $OLLAMA_URL"
    err "start it with: docker compose up -d ollama"
    exit 1
  fi
  ok "Ollama reachable"

  if ! curl -sf "$OLLAMA_URL/api/tags" | grep -q "\"$OLLAMA_MODEL\""; then
    info "pulling $OLLAMA_MODEL (one-time)"
    curl -sf -X POST "$OLLAMA_URL/api/pull" -d "{\"name\":\"$OLLAMA_MODEL\"}" \
      | tail -n 1
  fi
  ok "model $OLLAMA_MODEL ready"
}

run_example() {
  local main_class="$1"
  info "running $main_class"
  exec mvn -q exec:java -Dexec.mainClass="$main_class"
}
