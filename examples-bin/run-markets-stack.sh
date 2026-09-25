#!/usr/bin/env bash
# Brings up Kafka plus a Flink 2.2.1 session cluster in Podman (docker-compose-markets.yml,
# which includes docker-compose-kafka.yml and docker-compose-session.yml) for the bond and
# crypto market examples, and waits until the broker and Flink REST answer.
#
# Prerequisites: Podman with `podman compose` or podman-compose, curl, outbound internet for
# the images (docker.io/confluentinc/cp-kafka, cp-zookeeper and docker.io/library/flink). No API
# key. Kafka is advertised on localhost:9092, Flink REST on http://localhost:8081.
#
#   bash examples-bin/run-markets-stack.sh
#   bash examples-bin/run-bond-market.sh      # or run-crypto-market.sh
#   bash examples-bin/down-all.sh             # tear down
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

FLINK_REST="${FLINK_REST:-http://localhost:8081}"

require_curl
ensure_podman_network
info "starting Kafka and the Flink session cluster (docker-compose-markets.yml)"
podman_compose -f docker-compose-markets.yml up -d

wait_for_port 127.0.0.1 9092 120 "Kafka"
info "waiting for Flink REST on $FLINK_REST"
for _ in $(seq 1 120); do
  if curl -fsS "$FLINK_REST/overview" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$FLINK_REST/overview" >/dev/null || die "Flink REST did not answer on $FLINK_REST within 120s"
ok "Flink REST reachable"

cat <<'TXT'

Markets stack up. Next:
  bash examples-bin/run-bond-market.sh      # bond feeds: checks, builds the uber jar, prints the producer and flink run commands
  bash examples-bin/run-crypto-market.sh    # live Coinbase feeds (outbound internet)
Tear down: bash examples-bin/down-all.sh
TXT
