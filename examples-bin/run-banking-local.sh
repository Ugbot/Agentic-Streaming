#!/usr/bin/env bash
# Runs the banking demo in its production shape but as plain processes: per role, a Flink
# MiniCluster job (banking-job/target/banking-job.jar) plus the Quarkus A2A gateway
# (a2a-gateway/target/quarkus-app/quarkus-run.jar), bridged over Redis. The personal agent
# answers on :9001, customer service on :9002. Use run-banking.sh for the lighter
# single-process variant without Redis.
#
# Prerequisites: JDK 21, curl, Redis on localhost:6379 (podman run -d --name agentic-redis
# -p 127.0.0.1:6379:6379 docker.io/library/redis:7), and three jars built from the repository
# root with:
#   ./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
#   ./mvnw install -DskipTests
#   ./mvnw -f a2a-gateway/pom.xml package -DskipTests
#   ./mvnw -f banking-job/pom.xml package -DskipTests
# The script builds a missing jar itself when BUILD=1 is set.
#
# LLM provider: defaults to Ollama (bash examples-bin/run-ollama.sh), no API key. Paid providers
# are opt in through LLM_PROVIDER=openai|gemini|anthropic with the matching key, see
# run-banking.sh for the model defaults. .env in the repository root is loaded when present.
# EMBED_PROVIDER defaults to keyword (offline). ENV_API_URL is optional (environment tools).
# The gateways refuse unauthenticated calls: the script generates a bearer token for this run
# (override with AGENTIC_A2A_TOKEN) and prints it; send it as "Authorization: Bearer <token>".
#
# Latency: one message/send can involve up to three Ollama completions (personal agent, the
# customer-service agent it delegates to over A2A, personal agent again). On a CPU-only box with
# qwen2.5:3b that is typically 10 to 120 seconds but can exceed the gateway's task timeout, in
# which case the reply text is "Task timed out" and --once exits 1. Raise
# A2A_GATEWAY_REQUEST_TIMEOUT_MS (default 280000) and ONCE_CURL_TIMEOUT_S (default 300) together.
#
# Usage:
#   bash examples-bin/run-banking-local.sh                 # start, Ctrl-C to stop
#   bash examples-bin/run-banking-local.sh --once "text"   # start, send one message/send, exit
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

if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.env"
  set +a
fi

require_java21
require_curl
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
require_port "$REDIS_HOST:$REDIS_PORT" "Redis" \
  "Start it with: podman run -d --name agentic-redis -p 127.0.0.1:6379:6379 docker.io/library/redis:7"

GW_JAR="$REPO_ROOT/a2a-gateway/target/quarkus-app/quarkus-run.jar"
JOB_JAR="$REPO_ROOT/banking-job/target/banking-job.jar"
# The root module installs a dependency-reduced pom, so banking-job.jar carries Flink, Jedis and
# log4j but not the core's compile dependencies (Jackson, LangChain4j). The root uber jar supplies
# them on the same classpath.
CORE_UBER_JAR="$REPO_ROOT/target/agentic-flink-1.0.0-SNAPSHOT-uber.jar"
build_if_missing() {
  local jar="$1" module="$2"
  if [ -f "$jar" ]; then
    ok "$module jar present"
  elif [ "${BUILD:-0}" = "1" ]; then
    ensure_jagentic_core
    info "building $module (BUILD=1)"
    mvn_q install -DskipTests
    mvn_q -f "$module/pom.xml" package -DskipTests
    require_file "$jar" "the $module build did not produce it"
  else
    die "missing $jar. Build it with: ./mvnw install -DskipTests && ./mvnw -f $module/pom.xml package -DskipTests (or rerun with BUILD=1)"
  fi
}
if [ ! -f "$CORE_UBER_JAR" ]; then
  if [ "${BUILD:-0}" = "1" ]; then
    ensure_jagentic_core
    info "building the root module (BUILD=1)"
    mvn_q install -DskipTests
  else
    die "missing $CORE_UBER_JAR. Build it with: ./mvnw install -DskipTests (or rerun with BUILD=1)"
  fi
fi
require_file "$CORE_UBER_JAR"
ok "root uber jar present"
build_if_missing "$GW_JAR" a2a-gateway
build_if_missing "$JOB_JAR" banking-job

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
if [ -n "${MODEL:-}" ]; then export MODEL; else unset MODEL; fi
export EMBED_PROVIDER="${EMBED_PROVIDER:-keyword}"
export KB_PATH="${KB_PATH:-$REPO_ROOT/banking-kb/documents}"
export KB_POLICY_PATH="${KB_POLICY_PATH:-$REPO_ROOT/banking-kb/policy.md}"
export EMBED_CACHE_DIR="${EMBED_CACHE_DIR:-$REPO_ROOT/target/banking-kb-cache}"
require_dir "$KB_PATH"
require_file "$KB_POLICY_PATH"
if [ -n "${ENV_API_URL:-}" ]; then export ENV_API_URL; else unset ENV_API_URL; fi

AGENTIC_A2A_TOKEN="${AGENTIC_A2A_TOKEN:-$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')}"
export AGENTIC_A2A_TOKEN
LOG_DIR="$REPO_ROOT/target/example-logs/banking-local"
mkdir -p "$LOG_DIR"

# The Redis bridge is a pair of lists per role (RPUSH/BLPOP). Entries left behind by a previous
# run (a request whose job died, a response whose gateway exited) would be consumed first and
# stall this run, so the four lists are cleared before the jobs start.
drain_bridge_lists() {
  local keys="a2a:cs:req a2a:cs:resp a2a:personal:req a2a:personal:resp"
  if command -v redis-cli >/dev/null 2>&1; then
    # shellcheck disable=SC2086
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" DEL $keys >/dev/null
  else
    exec 3<>"/dev/tcp/$REDIS_HOST/$REDIS_PORT"
    printf 'DEL %s\r\nQUIT\r\n' "$keys" >&3
    cat <&3 >/dev/null
    exec 3>&-
  fi
  ok "cleared the Redis bridge lists"
}
drain_bridge_lists
PIDS=()
cleanup() { stop_pids "${PIDS[@]}"; }
trap cleanup INT TERM EXIT

start_role() {
  local role="$1" port="$2" tok="$3" agentname="$4"
  local common=(
    "AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT=redis"
    "AGENTIC_FLINK_REDIS_HOST=$REDIS_HOST"
    "AGENTIC_FLINK_REDIS_PORT=$REDIS_PORT"
    "AGENTIC_FLINK_CONVERSATION_STORE=redis"
    "AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT=a2a:$role:req"
    "AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT=a2a:$role:resp"
  )
  # shellcheck disable=SC2086
  env "${common[@]}" A2A_BANKING_ROLE="$role" ENV_API_TOKEN="$tok" \
    CS_AGENT_URL="${CS_AGENT_URL:-http://localhost:9002}" CS_AGENT_TOKEN="$AGENTIC_A2A_TOKEN" \
    java $AGENTIC_ADD_OPENS -cp "$JOB_JAR:$CORE_UBER_JAR" org.agentic.flink.example.banking.BankingFlinkJob \
    > "$LOG_DIR/job-$role.log" 2>&1 &
  PIDS+=($!)
  # shellcheck disable=SC2086
  env "${common[@]}" QUARKUS_HTTP_PORT="$port" \
    AGENTIC_FLINK_A2A_GATEWAY_PUBLIC_URL="http://localhost:$port" \
    AGENTIC_FLINK_A2A_GATEWAY_AGENT_NAME="$agentname" \
    AGENTIC_FLINK_A2A_GATEWAY_REQUEST_TIMEOUT_MS="${A2A_GATEWAY_REQUEST_TIMEOUT_MS:-280000}" \
    java $AGENTIC_ADD_OPENS -jar "$GW_JAR" > "$LOG_DIR/gw-$role.log" 2>&1 &
  PIDS+=($!)
  info "started $role: Flink job plus gateway on :$port"
}

start_role cs 9002 "${CS_ENV_API_TOKEN:-dev-agent-token}" "Rho-Bank Customer Service"
start_role personal 9001 "${PERSONAL_ENV_API_TOKEN:-dev-user-token}" "Rho-Bank Personal Assistant"

alive_or_die() {
  for pid in "${PIDS[@]}"; do
    kill -0 "$pid" 2>/dev/null || die "a process exited early; see $LOG_DIR/*.log"
  done
}
for role in cs personal; do
  info "waiting for the $role Flink job to start its graph"
  for _ in $(seq 1 180); do
    grep -q "Banking Flink job \[$role\] starting" "$LOG_DIR/job-$role.log" 2>/dev/null && break
    alive_or_die
    sleep 1
  done
  grep -q "Banking Flink job \[$role\] starting" "$LOG_DIR/job-$role.log" \
    || die "the $role job did not start within 180s; see $LOG_DIR/job-$role.log"
  ok "$role job started"
done
for port in 9002 9001; do
  info "waiting for the gateway agent card on :$port"
  for _ in $(seq 1 120); do
    curl -sf -m 2 "http://localhost:$port/.well-known/agent-card.json" >/dev/null 2>&1 && break
    alive_or_die
    sleep 1
  done
  curl -sf -m 2 "http://localhost:$port/.well-known/agent-card.json" >/dev/null 2>&1 \
    || die "gateway on :$port did not answer within 120s; see $LOG_DIR/gw-*.log"
  ok "agent card served on :$port"
done

if [ -n "$ONCE_TEXT" ]; then
  info "message/send to the personal agent: $ONCE_TEXT"
  body="$("$PYTHON" -c 'import json,sys; print(json.dumps({"jsonrpc":"2.0","id":"1","method":"message/send","params":{"message":{"role":"user","messageId":"m1","parts":[{"kind":"text","text":sys.argv[1]}]}}}))' "$ONCE_TEXT")"
  reply="$(curl -sf -m "${ONCE_CURL_TIMEOUT_S:-300}" -H 'Content-Type: application/json' -H "Authorization: Bearer $AGENTIC_A2A_TOKEN" \
    -d "$body" "http://localhost:9001/")"
  printf '%s\n' "$reply"
  printf '%s' "$reply" | grep -q '"result"' || die "the personal agent returned an error"
  printf '%s' "$reply" | grep -q 'Task timed out' \
    && die "the gateway timed out waiting for the personal Flink job; see $LOG_DIR/job-personal.log"
  ok "personal agent answered"
  exit 0
fi

printf '\nagents up. personal=http://localhost:9001  cs=http://localhost:9002\n'
printf 'bearer token for message/send: %s\n' "$AGENTIC_A2A_TOKEN"
printf 'logs in target/example-logs/banking-local/. Ctrl-C to stop.\n'
wait
