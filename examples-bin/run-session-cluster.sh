#!/usr/bin/env bash
# Brings up Fluss plus a Flink 2.2.1 session cluster in Podman (docker-compose-cluster.yml)
# for notebooks/09_session_cluster_levels.ipynb, waits until both answer, and prints the REST
# commands to upload and run the uber jar.
#
# Prerequisites: Podman with `podman compose` or podman-compose, curl, outbound internet for the
# images. No API key. Flink REST listens on http://localhost:8081, Fluss on localhost:9123.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

require_curl
ensure_podman_network
info "starting Fluss and the Flink session cluster (docker-compose-cluster.yml)"
podman_compose -f docker-compose-cluster.yml up -d

wait_for_port "${FLUSS_BOOTSTRAP%:*}" "${FLUSS_BOOTSTRAP#*:}" 120 "Fluss coordinator"
info "waiting for Flink REST on $FLINK_REST"
for _ in $(seq 1 120); do
  if curl -fsS "$FLINK_REST/overview" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
OVERVIEW="$(curl -fsS "$FLINK_REST/overview")" || die "Flink REST did not answer on $FLINK_REST within 120s"
ok "Flink REST reachable"
echo
echo "Cluster overview: $OVERVIEW"
echo
cat <<TXT
Next (from the repository root):
  1. Build the jar:           ./mvnw -DskipTests package
  2. Upload to the cluster:   curl -X POST -F "jarfile=@target/agentic-flink-1.0.0-SNAPSHOT-uber.jar" $FLINK_REST/jars/upload
  3. List jars:               curl $FLINK_REST/jars
  4. Run a level:             curl -X POST $FLINK_REST/jars/{id}/run -d '{"entryClass":"org.agentic.flink.session.SessionJobLauncher","programArgsList":["--level","producer","--out","tcp://0.0.0.0:5560"]}'

Or run notebooks/09_session_cluster_levels.ipynb, which does steps 1 to 4 through the Python session client.
Tear down: bash examples-bin/down-all.sh
TXT
