#!/usr/bin/env bash
# Brings up the Fluss + Flink session cluster (notebooks/09_session_cluster_levels.ipynb).
# Uses the docker-compose-cluster.yml umbrella so the per-service files stay the source of
# truth. Polls until both stacks answer, prints next-step hints.

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:9123}"
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }
info(){ printf '\033[34m→\033[0m %s\n' "$*"; }
err() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }

bash "$HERE/setup-network.sh"

info "starting Fluss + Flink session cluster (docker-compose-cluster.yml)"
podman compose -f "$REPO/docker-compose-cluster.yml" up -d

info "waiting for Fluss coordinator on $FLUSS_BOOTSTRAP"
for _ in {1..60}; do
  if (printf '' >/dev/tcp/${FLUSS_BOOTSTRAP%:*}/${FLUSS_BOOTSTRAP#*:}) 2>/dev/null; then
    ok "Fluss coordinator reachable"
    break
  fi
  sleep 1
done

info "waiting for Flink REST on $FLINK_REST"
for _ in {1..120}; do
  if curl -fsS "$FLINK_REST/overview" >/dev/null 2>&1; then
    ok "Flink REST reachable"
    break
  fi
  sleep 1
done

OVERVIEW=$(curl -fsS "$FLINK_REST/overview" || echo '{}')
echo
echo "Cluster overview: $OVERVIEW"
echo
cat <<EOF
Next:
  1. Build the jar:           mvn -DskipTests package
  2. Upload to the cluster:   curl -X POST -F "jarfile=@target/agentic-flink-1.0.0-SNAPSHOT-uber.jar" $FLINK_REST/jars/upload
  3. List jars:               curl $FLINK_REST/jars
  4. Run a level:             curl -X POST $FLINK_REST/jars/{id}/run -d '{"entryClass":"org.agentic.flink.session.SessionJobLauncher","programArgsList":["--level","producer","--out","tcp://0.0.0.0:5560"]}'

Or just run notebooks/09_session_cluster_levels.ipynb which does steps 1-4 via the Python session client.
EOF
