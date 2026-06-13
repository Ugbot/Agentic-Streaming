#!/usr/bin/env bash
# Run the Rho-Bank A2A demo locally: two A2A agents (personal :9001, cs :9002) on the Agentic-Flink
# gateway, each running the bounded ReAct brain + safety stack in-process.
#
# Model: defaults to OpenAI for local testing (set OPENAI_API_KEY). Swap to the hackathon-mandated
# Gemini with LLM_PROVIDER=gemini + GOOGLE_API_KEY (no code change).
#
# Usage:
#   export OPENAI_API_KEY=sk-...
#   bash examples-bin/run-banking.sh
# Then point the harness at it:
#   cd hackathons/a2a-hackathon && uv run a2a-hack smoke \
#     --personal-url http://localhost:9001 --cs-url http://localhost:9002
#
# ENV_API_URL is optional: without it the agents run chat/RAG only (no env tools). With the harness
# running, set ENV_API_URL/ENV_API_TOKEN so the agents can call the bank/user environment tools.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROVIDER="${LLM_PROVIDER:-openai}"
if [ "$PROVIDER" = "openai" ] && [ -z "${OPENAI_API_KEY:-}" ]; then
  echo "Set OPENAI_API_KEY (or LLM_PROVIDER=gemini with GOOGLE_API_KEY)." >&2
  exit 1
fi

KB_DIR="${KB_PATH:-$ROOT/hackathons/a2a-hackathon-template/kb/documents}"
KB_POLICY="${KB_POLICY_PATH:-$ROOT/hackathons/a2a-hackathon-template/kb/policy.md}"
JAR="$ROOT/a2a-gateway/target/quarkus-app/quarkus-run.jar"

echo "==> Building core + banking gateway jar (a2a.mode=banking)"
mvn -q -o install -DskipTests
mvn -q -o -f a2a-gateway/pom.xml package -DskipTests -Da2a.mode=banking

common_env() {
  echo "LLM_PROVIDER=$PROVIDER MODEL=${MODEL:-} OPENAI_API_KEY=${OPENAI_API_KEY:-} \
GOOGLE_API_KEY=${GOOGLE_API_KEY:-} ENV_API_URL=${ENV_API_URL:-} ENV_API_TOKEN=${ENV_API_TOKEN:-}"
}

echo "==> Starting CS agent on :9002"
A2A_MODE=banking A2A_BANKING_ROLE=cs QUARKUS_HTTP_PORT=9002 \
  AGENTIC_FLINK_A2A_GATEWAY_AGENT_NAME="Rho-Bank Customer Service" \
  AGENTIC_FLINK_A2A_GATEWAY_PUBLIC_URL="http://localhost:9002" \
  KB_PATH="$KB_DIR" KB_POLICY_PATH="$KB_POLICY" \
  LLM_PROVIDER="$PROVIDER" MODEL="${MODEL:-}" OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  GOOGLE_API_KEY="${GOOGLE_API_KEY:-}" ENV_API_URL="${ENV_API_URL:-}" ENV_API_TOKEN="${CS_ENV_API_TOKEN:-${ENV_API_TOKEN:-}}" \
  java -jar "$JAR" > /tmp/banking-cs.log 2>&1 &
CS_PID=$!

echo "==> Starting personal agent on :9001 (CS_AGENT_URL=http://localhost:9002)"
A2A_MODE=banking A2A_BANKING_ROLE=personal QUARKUS_HTTP_PORT=9001 \
  AGENTIC_FLINK_A2A_GATEWAY_AGENT_NAME="Rho-Bank Personal Assistant" \
  AGENTIC_FLINK_A2A_GATEWAY_PUBLIC_URL="http://localhost:9001" \
  CS_AGENT_URL="${CS_AGENT_URL:-http://localhost:9002}" \
  LLM_PROVIDER="$PROVIDER" MODEL="${MODEL:-}" OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  GOOGLE_API_KEY="${GOOGLE_API_KEY:-}" ENV_API_URL="${ENV_API_URL:-}" ENV_API_TOKEN="${PERSONAL_ENV_API_TOKEN:-${ENV_API_TOKEN:-}}" \
  java -jar "$JAR" > /tmp/banking-personal.log 2>&1 &
PERSONAL_PID=$!

trap 'kill $CS_PID $PERSONAL_PID 2>/dev/null || true' INT TERM EXIT
echo "==> personal=:9001 (pid $PERSONAL_PID)  cs=:9002 (pid $CS_PID)"
echo "    logs: /tmp/banking-personal.log /tmp/banking-cs.log"
echo "    card: curl http://localhost:9001/.well-known/agent-card.json"
echo "    Ctrl-C to stop."
wait
