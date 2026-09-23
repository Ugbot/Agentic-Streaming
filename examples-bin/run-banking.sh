#!/usr/bin/env bash
# Runs the Rho-Bank A2A demo as two plain JVM processes: the personal agent on :9001 and the
# customer service agent on :9002, each serving the A2A agent card and message/send with the
# bounded ReAct brain plus safety stack in-process (org.agentic.flink.example.banking.BankingA2AServer).
# No Redis, no Quarkus, no containers beyond Ollama.
#
# Prerequisites: JDK 21, the Maven wrapper (first run compiles the root module), curl, and a
# reachable LLM provider. The default provider is Ollama (bash examples-bin/run-ollama.sh), which
# needs no API key. Paid providers are opt in:
#   LLM_PROVIDER=openai    with OPENAI_API_KEY     (default model gpt-5.4-nano)
#   LLM_PROVIDER=gemini    with GOOGLE_API_KEY     (default model gemini-3.5-flash)
#   LLM_PROVIDER=anthropic with ANTHROPIC_API_KEY  (default model claude-sonnet-4-6)
#   LLM_PROVIDER=ollama    (default model qwen2.5:3b; override with MODEL)
# EMBED_PROVIDER defaults to keyword (offline). Set EMBED_PROVIDER=djl for local vector search;
# the first run then downloads the embedding model from Hugging Face.
# ENV_API_URL/ENV_API_TOKEN are optional: without them the agents run chat and KB search only.
#
# Usage:
#   bash examples-bin/run-banking.sh                 # start both agents, Ctrl-C to stop
#   bash examples-bin/run-banking.sh --once "text"   # start, send one message/send, print the reply, exit
#   curl http://localhost:9001/.well-known/agent-card.json
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

ONCE_TEXT=""
if [ "${1:-}" = "--once" ]; then
  [ -n "${2:-}" ] || die "--once needs the message text as the next argument"
  ONCE_TEXT="$2"
  shift 2
fi
[ $# -eq 0 ] || die "unknown arguments: $*"
[ -z "$ONCE_TEXT" ] || require_python3

require_curl
PERSONAL_PORT="${PERSONAL_PORT:-9001}"
CS_PORT="${CS_PORT:-9002}"
LLM_PROVIDER="${LLM_PROVIDER:-ollama}"
case "$LLM_PROVIDER" in
  ollama)
    export OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-$OLLAMA_URL}"
    OLLAMA_URL="$OLLAMA_BASE_URL"
    OLLAMA_MODEL="${MODEL:-$OLLAMA_MODEL}"
    check_ollama ;;
  openai)    require_env OPENAI_API_KEY "LLM_PROVIDER=openai needs it. Use LLM_PROVIDER=ollama for the no-key path." ;;
  gemini)    require_env GOOGLE_API_KEY "LLM_PROVIDER=gemini needs it. Use LLM_PROVIDER=ollama for the no-key path." ;;
  anthropic) require_env ANTHROPIC_API_KEY "LLM_PROVIDER=anthropic needs it. Use LLM_PROVIDER=ollama for the no-key path." ;;
  *) die "LLM_PROVIDER must be one of ollama, openai, gemini, anthropic (got '$LLM_PROVIDER')" ;;
esac
export LLM_PROVIDER
# An empty MODEL would be passed through verbatim; only export it when set.
if [ -n "${MODEL:-}" ]; then export MODEL; else unset MODEL; fi
export EMBED_PROVIDER="${EMBED_PROVIDER:-keyword}"
export KB_PATH="${KB_PATH:-$REPO_ROOT/banking-kb/documents}"
export KB_POLICY_PATH="${KB_POLICY_PATH:-$REPO_ROOT/banking-kb/policy.md}"
export EMBED_CACHE_DIR="${EMBED_CACHE_DIR:-$REPO_ROOT/target/banking-kb-cache}"
require_dir "$KB_PATH" "the banking knowledge base directory is missing"
require_file "$KB_POLICY_PATH"
for p in "$PERSONAL_PORT" "$CS_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    die "port $p is already in use; stop the other process or set PERSONAL_PORT/CS_PORT"
  fi
done

CP="$(example_classpath)"
LOG_DIR="$REPO_ROOT/target/example-logs"
mkdir -p "$LOG_DIR"
MAIN=org.agentic.flink.example.banking.BankingA2AServer
PIDS=()
cleanup() { stop_pids "${PIDS[@]}"; }
trap cleanup INT TERM EXIT

info "starting CS agent on :$CS_PORT (log: target/example-logs/banking-cs.log)"
# shellcheck disable=SC2086
A2A_BANKING_ROLE=cs PORT="$CS_PORT" PUBLIC_URL="http://localhost:$CS_PORT" \
  ENV_API_TOKEN="${CS_ENV_API_TOKEN:-${ENV_API_TOKEN:-}}" \
  java $AGENTIC_ADD_OPENS -cp "$CP" "$MAIN" > "$LOG_DIR/banking-cs.log" 2>&1 &
PIDS+=($!)

info "starting personal agent on :$PERSONAL_PORT (log: target/example-logs/banking-personal.log)"
# shellcheck disable=SC2086
A2A_BANKING_ROLE=personal PORT="$PERSONAL_PORT" PUBLIC_URL="http://localhost:$PERSONAL_PORT" \
  CS_AGENT_URL="${CS_AGENT_URL:-http://localhost:$CS_PORT}" \
  ENV_API_TOKEN="${PERSONAL_ENV_API_TOKEN:-${ENV_API_TOKEN:-}}" \
  java $AGENTIC_ADD_OPENS -cp "$CP" "$MAIN" > "$LOG_DIR/banking-personal.log" 2>&1 &
PIDS+=($!)

for port in "$CS_PORT" "$PERSONAL_PORT"; do
  info "waiting for the agent card on :$port"
  for _ in $(seq 1 120); do
    if curl -sf -m 2 "http://localhost:$port/.well-known/agent-card.json" >/dev/null 2>&1; then
      break
    fi
    for pid in "${PIDS[@]}"; do
      kill -0 "$pid" 2>/dev/null || die "an agent process exited early; see $LOG_DIR/banking-*.log"
    done
    sleep 1
  done
  curl -sf -m 2 "http://localhost:$port/.well-known/agent-card.json" >/dev/null 2>&1 \
    || die "agent on :$port did not publish its card within 120s; see $LOG_DIR/banking-*.log"
  ok "agent card served on :$port"
done

if [ -n "$ONCE_TEXT" ]; then
  info "message/send to the personal agent: $ONCE_TEXT"
  body="$("$PYTHON" -c 'import json,sys; print(json.dumps({"jsonrpc":"2.0","id":"1","method":"message/send","params":{"message":{"role":"user","messageId":"m1","parts":[{"kind":"text","text":sys.argv[1]}]}}}))' "$ONCE_TEXT")"
  reply="$(curl -sf -m 300 -H 'Content-Type: application/json' -d "$body" "http://localhost:$PERSONAL_PORT/")"
  printf '%s\n' "$reply"
  printf '%s' "$reply" | grep -q '"result"' || die "the personal agent returned an error"
  ok "personal agent answered"
  exit 0
fi

printf '\npersonal=http://localhost:%s  cs=http://localhost:%s\n' "$PERSONAL_PORT" "$CS_PORT"
printf 'card:    curl http://localhost:%s/.well-known/agent-card.json\n' "$PERSONAL_PORT"
printf 'harness: cd hackathons/a2a-hackathon && uv run a2a-hack smoke --personal-url http://localhost:%s --cs-url http://localhost:%s\n' "$PERSONAL_PORT" "$CS_PORT"
printf 'Ctrl-C to stop.\n'
wait
