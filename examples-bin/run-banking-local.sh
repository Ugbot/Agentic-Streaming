#!/usr/bin/env bash
# Run the banking demo locally WITHOUT containers: per role, a Flink MiniCluster job
# (banking-job.jar) + the Quarkus A2A gateway (quarkus-run.jar), bridged over a local Redis.
# This mirrors the single-container shape (one Quarkus front + one embedded Flink job per role,
# Redis as the spine) but as plain processes, so we can run the a2a-hack harness against it.
#
#   redis-server (localhost:6379) must be running.
#   Build first:  mvn -o clean install -DskipTests \
#                 && mvn -o -f a2a-gateway/pom.xml package -DskipTests \
#                 && mvn -o -f banking-job/pom.xml package -DskipTests
#   Then:         ANTHROPIC_API_KEY=... bash examples-bin/run-banking-local.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

GW_JAR="$ROOT/a2a-gateway/target/quarkus-app/quarkus-run.jar"
JOB_JAR="$ROOT/banking-job/target/banking-job.jar"
[ -f "$GW_JAR" ] || { echo "missing $GW_JAR — build the gateway"; exit 1; }
[ -f "$JOB_JAR" ] || { echo "missing $JOB_JAR — build banking-job"; exit 1; }

# Load .env if present (ANTHROPIC_API_KEY / GOOGLE_API_KEY).
[ -f "$ROOT/.env" ] && set -a && . "$ROOT/.env" && set +a

LLM_PROVIDER="${LLM_PROVIDER:-openai}"
MODEL="${MODEL:-gpt-5.4-nano}"
EMBED_PROVIDER="${EMBED_PROVIDER:-keyword}"
ENV_API_URL="${ENV_API_URL:-http://localhost:8090}"
REDIS_HOST="${REDIS_HOST:-localhost}"
LOG=/tmp/banking-local
mkdir -p "$LOG"
PIDS=()

start_role() {
  local role="$1" port="$2" tok="$3" agentname="$4"
  local common=(
    "AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT=redis"
    "AGENTIC_FLINK_REDIS_HOST=$REDIS_HOST"
    "AGENTIC_FLINK_CONVERSATION_STORE=redis"
    "AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT=a2a:$role:req"
    "AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT=a2a:$role:resp"
  )
  # Flink job (the embedded MiniCluster running the routed graph + the LLM path operators).
  env "${common[@]}" \
    A2A_BANKING_ROLE="$role" \
    LLM_PROVIDER="$LLM_PROVIDER" MODEL="$MODEL" \
    ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" GOOGLE_API_KEY="${GOOGLE_API_KEY:-}" OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
    EMBED_PROVIDER="$EMBED_PROVIDER" \
    ENV_API_URL="$ENV_API_URL" ENV_API_TOKEN="$tok" \
    KB_PATH="$ROOT/banking-kb/documents" KB_POLICY_PATH="$ROOT/banking-kb/policy.md" \
    CS_AGENT_URL="${CS_AGENT_URL:-http://localhost:9002}" \
    java -jar "$JOB_JAR" > "$LOG/job-$role.log" 2>&1 &
  PIDS+=($!)
  # Quarkus A2A gateway (thin front; publishes to Redis, awaits the verifier).
  env "${common[@]}" \
    QUARKUS_HTTP_PORT="$port" \
    AGENTIC_FLINK_A2A_GATEWAY_PUBLIC_URL="http://localhost:$port" \
    AGENTIC_FLINK_A2A_GATEWAY_AGENT_NAME="$agentname" \
    AGENTIC_FLINK_A2A_GATEWAY_REQUEST_TIMEOUT_MS="${A2A_GATEWAY_REQUEST_TIMEOUT_MS:-280000}" \
    java -jar "$GW_JAR" > "$LOG/gw-$role.log" 2>&1 &
  PIDS+=($!)
  echo "started $role: job + gateway on :$port"
}

cleanup() { echo "stopping…"; kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

start_role cs 9002 "${CS_ENV_API_TOKEN:-dev-agent-token}" "Rho-Bank Customer Service"
start_role personal 9001 "${PERSONAL_ENV_API_TOKEN:-dev-user-token}" "Rho-Bank Personal Assistant"

echo "waiting for both Flink jobs to subscribe to Redis…"
for role in cs personal; do
  for i in $(seq 1 120); do
    n=$(redis-cli -h "$REDIS_HOST" pubsub numsub "a2a:$role:req" | tail -1)
    [ "${n:-0}" -ge 1 ] && { echo "  $role job subscribed"; break; }
    sleep 1
  done
done
echo "agents up. personal=http://localhost:9001  cs=http://localhost:9002"
echo "logs in $LOG/. Ctrl-C to stop."
wait
