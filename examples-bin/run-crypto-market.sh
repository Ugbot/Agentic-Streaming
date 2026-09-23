#!/usr/bin/env bash
# Flink-runtime showcase: live Coinbase trades through Kafka into the same operator graph as
# the bond example (CryptoMarketAgentExample).
#
# Prerequisites: JDK 21, the Maven wrapper, python3, Kafka at KAFKA_BOOTSTRAP (default
# localhost:9092), and outbound internet for the Coinbase WebSocket bridge. Bring up Kafka and a
# Flink 2.2.1 session cluster with
#   bash examples-bin/run-markets-stack.sh
# No API key is required; ANTHROPIC_API_KEY enables the optional LLM tier.
#
#   bash examples-bin/run-crypto-market.sh            # check, build the uber jar, print commands
#   bash examples-bin/run-crypto-market.sh --submit   # additionally `flink run` the job
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"
# shellcheck source=_markets.sh
source "$HERE/_markets.sh"

[ $# -le 1 ] || die "usage: bash examples-bin/run-crypto-market.sh [--submit]"
markets_main org.agentic.flink.example.markets.CryptoMarketAgentExample "${1:-}" \
  "python3 examples-bin/markets/coinbase_producer.py --products BTC-USD,ETH-USD,SOL-USD"
