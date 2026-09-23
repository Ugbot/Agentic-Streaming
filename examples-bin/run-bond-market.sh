#!/usr/bin/env bash
# Flink-runtime showcase: anonymised bond market feeds through Kafka into the classic-Flink plus
# inline-agentic operator graph (BondMarketAgentExample).
#
# Prerequisites: JDK 21, the Maven wrapper, python3, Kafka at KAFKA_BOOTSTRAP (default
# localhost:9092). Bring up Kafka and a Flink 2.2.1 session cluster with
#   bash examples-bin/run-markets-stack.sh
# No API key is required; ANTHROPIC_API_KEY enables the optional LLM tier.
#
#   bash examples-bin/run-bond-market.sh            # check, build the uber jar, print commands
#   bash examples-bin/run-bond-market.sh --submit   # additionally `flink run` the job
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"
# shellcheck source=_markets.sh
source "$HERE/_markets.sh"

[ $# -le 1 ] || die "usage: bash examples-bin/run-bond-market.sh [--submit]"
markets_main org.agentic.flink.example.markets.BondMarketAgentExample "${1:-}" \
  "python3 examples-bin/markets/bond_securities_producer.py" \
  "python3 examples-bin/markets/bond_inventory_producer.py" \
  "python3 examples-bin/markets/bond_trades_producer.py"
