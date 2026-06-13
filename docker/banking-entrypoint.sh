#!/usr/bin/env bash
# One container = one agent role: the embedded Flink MiniCluster job (banking-job.jar) + the Quarkus
# A2A gateway (quarkus-run.jar), joined by Redis. The job runs the routed graph (and the LLM path
# operators); the gateway is the thin A2A front. We start the job first and wait until it has
# subscribed to its Redis request channel before starting the gateway — Redis pub/sub is lossy, so
# the front must not accept traffic before the job is listening.
set -uo pipefail

ROLE="${A2A_BANKING_ROLE:-personal}"
REDIS_HOST="${AGENTIC_FLINK_REDIS_HOST:-redis}"
# Role-namespaced bridge channels (both agent containers share one Redis). The job derives these
# too; set them here so the gateway (which reads them via AgenticFlinkConfig) matches.
export AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT="${AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT:-redis}"
export AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT="${AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT:-a2a:${ROLE}:req}"
export AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT="${AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT:-a2a:${ROLE}:resp}"
export AGENTIC_FLINK_CONVERSATION_STORE="${AGENTIC_FLINK_CONVERSATION_STORE:-redis}"
export AGENTIC_FLINK_REDIS_HOST="$REDIS_HOST"
export KB_PATH="${KB_PATH:-/app/kb/documents}"
export KB_POLICY_PATH="${KB_POLICY_PATH:-/app/kb/policy.md}"
export EMBED_CACHE_DIR="${EMBED_CACHE_DIR:-/app/.kb-cache}"

REQ_CHANNEL="$AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT"

echo "[entrypoint] role=$ROLE redis=$REDIS_HOST req=$REQ_CHANNEL"

java ${JOB_JAVA_OPTS:-} -jar /deployments/banking-job.jar &
JOB_PID=$!
java ${GW_JAVA_OPTS:-} -jar /deployments/quarkus-app/quarkus-run.jar &
GW_PID=$!

# Wait for the job to subscribe (or warm embeddings) — up to ~4 min for the gemini-embedding warm.
for i in $(seq 1 240); do
  if ! kill -0 "$JOB_PID" 2>/dev/null; then echo "[entrypoint] job exited early"; kill "$GW_PID" 2>/dev/null; exit 1; fi
  n=$(redis-cli -h "$REDIS_HOST" pubsub numsub "$REQ_CHANNEL" 2>/dev/null | tail -1)
  if [ "${n:-0}" -ge 1 ]; then echo "[entrypoint] job subscribed to $REQ_CHANNEL"; break; fi
  sleep 1
done

# Exit (and let the container restart) if either process dies.
wait -n "$JOB_PID" "$GW_PID"
echo "[entrypoint] a process exited; stopping the container"
kill "$JOB_PID" "$GW_PID" 2>/dev/null
exit 1
