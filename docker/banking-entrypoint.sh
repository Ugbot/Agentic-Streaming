#!/usr/bin/env bash
# One container = one agent role: the embedded Flink MiniCluster job (banking-job.jar) + the Quarkus
# A2A gateway (quarkus-run.jar), joined by Redis. The job runs the routed graph (and the LLM path
# operators); the gateway is the thin A2A front.
#
# The Redis bridge is non-lossy (RPUSH/BLPOP) — a request the gateway publishes before the job's
# source is consuming simply waits in the list — so we don't need to order startup or wait for a
# subscription: start both and exit (let the container restart) if either dies.
set -uo pipefail

ROLE="${A2A_BANKING_ROLE:-personal}"
# Role-namespaced bridge channels (both agent containers share one Redis). The job derives these
# too; set them here so the gateway (which reads them via AgenticFlinkConfig) matches.
export AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT="${AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT:-redis}"
export AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT="${AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT:-a2a:${ROLE}:req}"
export AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT="${AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT:-a2a:${ROLE}:resp}"
export AGENTIC_FLINK_CONVERSATION_STORE="${AGENTIC_FLINK_CONVERSATION_STORE:-redis}"
export AGENTIC_FLINK_REDIS_HOST="${AGENTIC_FLINK_REDIS_HOST:-redis}"
export KB_PATH="${KB_PATH:-/app/kb/documents}"
export KB_POLICY_PATH="${KB_POLICY_PATH:-/app/kb/policy.md}"
export EMBED_CACHE_DIR="${EMBED_CACHE_DIR:-/app/.kb-cache}"

echo "[entrypoint] role=$ROLE redis=$AGENTIC_FLINK_REDIS_HOST req=$AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT"

java ${JOB_JAVA_OPTS:-} -jar /deployments/banking-job.jar &
JOB_PID=$!
java ${GW_JAVA_OPTS:-} -jar /deployments/quarkus-app/quarkus-run.jar &
GW_PID=$!

# Exit (and let the container restart) if either process dies.
wait -n "$JOB_PID" "$GW_PID"
echo "[entrypoint] a process exited; stopping the container"
kill "$JOB_PID" "$GW_PID" 2>/dev/null
exit 1
