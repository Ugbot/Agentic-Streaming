#!/usr/bin/env bash
# Brings up the RAG plus Fluss stack in Podman: Ollama, Postgres, Redis, Fluss coordinator and
# tablet (docker-compose-rag.yml). Waits for each service and prints the next steps for
# notebooks 02/03 and run-rag.sh.
#
# Prerequisites: Podman with `podman compose` or podman-compose, curl, outbound internet for
# the images. docker-compose.yml refuses to start without POSTGRES_PASSWORD and REDIS_PASSWORD:
# export them or put them in .env (cp .env.example .env; generate values with openssl rand -hex 24).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

require_curl
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.env"
  set +a
fi
require_env POSTGRES_PASSWORD "Set it in .env (cp .env.example .env) or export it before running this script."
require_env REDIS_PASSWORD "Set it in .env (cp .env.example .env) or export it before running this script."

ensure_podman_network
info "starting Ollama, Postgres, Redis and Fluss (docker-compose-rag.yml)"
podman_compose -f docker-compose-rag.yml up -d

wait_for_port 127.0.0.1 11434 120 "Ollama"
wait_for_port 127.0.0.1 9123 120 "Fluss coordinator"
wait_for_port 127.0.0.1 5432 120 "Postgres"
check_ollama

cat <<'TXT'

RAG stack up. Next:
  - notebooks/03_scraper_researcher_fluss.ipynb (Fluss-backed durable vector store)
  - notebooks/02_live_scrape_rag.ipynb           (live scrape, embed, answer)
  - bash examples-bin/run-rag.sh                 (RagResearchExample standalone)

The notebooks also use the nomic-embed-text embedding model (one time, about 300 MB):
  podman exec agentic-flink-ollama ollama pull nomic-embed-text

Tear down: bash examples-bin/down-all.sh
TXT
