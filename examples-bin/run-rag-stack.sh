#!/usr/bin/env bash
# Brings up the RAG + Fluss stack: Ollama + Postgres + Redis + Fluss coordinator/tablet.
# Polls readiness and prints next-step hints for notebooks 02/03 and the RagResearchExample.

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }
info(){ printf '\033[34m→\033[0m %s\n' "$*"; }
err() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }

bash "$HERE/setup-network.sh"

info "starting Ollama + Postgres + Redis + Fluss"
podman compose -f "$REPO/docker-compose-rag.yml" up -d

info "waiting for Ollama on localhost:11434"
for _ in {1..60}; do
  if curl -fsS http://localhost:11434/api/tags >/dev/null 2>&1; then
    ok "Ollama reachable"
    break
  fi
  sleep 1
done

info "waiting for Fluss coordinator on localhost:9123"
for _ in {1..60}; do
  if (printf '' >/dev/tcp/localhost/9123) 2>/dev/null; then
    ok "Fluss coordinator reachable"
    break
  fi
  sleep 1
done

info "waiting for Postgres on localhost:5432"
for _ in {1..60}; do
  if (printf '' >/dev/tcp/localhost/5432) 2>/dev/null; then
    ok "Postgres reachable"
    break
  fi
  sleep 1
done

cat <<'EOF'

RAG stack up. Next:
  - notebooks/03_scraper_researcher_fluss.ipynb (Fluss-backed durable vector store)
  - notebooks/02_live_scrape_rag.ipynb           (live scrape → embed → answer)
  - bash examples-bin/run-rag.sh                 (RagResearchExample standalone)

Pull the embedding + chat models (one time, ~1GB each):
  podman exec agentic-flink-ollama ollama pull nomic-embed-text
  podman exec agentic-flink-ollama ollama pull qwen2.5:3b
EOF
