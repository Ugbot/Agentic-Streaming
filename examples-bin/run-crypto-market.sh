#!/usr/bin/env bash
# Run the live-Coinbase crypto markets example. Same operator graph as the bond example,
# different topics and a WebSocket bridge in place of the synthesizers.

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

err() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }
ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }
info(){ printf '\033[34m→\033[0m %s\n' "$*"; }

info "checking Kafka at $KAFKA_BOOTSTRAP"
if ! (printf '' >/dev/tcp/${KAFKA_BOOTSTRAP%:*}/${KAFKA_BOOTSTRAP#*:}) 2>/dev/null; then
  err "Kafka not reachable. Start it with:"
  err "  podman compose -f docker-compose-kafka.yml up -d"
  exit 1
fi
ok "Kafka reachable"

JAR="$REPO/target/agentic-flink-1.0.0-SNAPSHOT.jar"
if [[ ! -f "$JAR" ]]; then
  info "building jar..."
  ( cd "$REPO" && mvn -q -DskipTests package )
fi
ok "jar at $JAR"

cat <<EOF

Start the Coinbase WebSocket bridge (needs internet):
  python examples-bin/markets/coinbase_producer.py --products BTC-USD,ETH-USD,SOL-USD

Optionally set ANTHROPIC_API_KEY for the LLM tier:
  export ANTHROPIC_API_KEY=sk-ant-...

Then submit the Flink job:
  flink run "$JAR" \\
      org.agentic.flink.example.markets.CryptoMarketAgentExample
EOF
