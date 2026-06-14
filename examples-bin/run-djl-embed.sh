#!/usr/bin/env bash
# Live DJL (Deep Java Library) embedding demo + micro-benchmark.
#
# Loads a real sentence-transformer (all-MiniLM-L6-v2) through DJL/PyTorch, embeds a small corpus
# into the RAG hot index, and verifies semantic recall (a paraphrased query retrieves the on-topic
# passage as top-1), then prints mean embed latency. DJL's pytorch-engine auto-downloads the
# platform-matched CPU native + the model on first run (cached under ~/.djl.ai); needs network the
# first time, runs offline thereafter.
#
#   bash examples-bin/run-djl-embed.sh
#
# This is just a thin wrapper over the @Tag("djl") test, run under the djl-native Maven profile
# (which selects that test group). The native PyTorch lib is NOT in the default build, so DJL tests
# are excluded from `mvn test` and only run here.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Running live DJL embedding recall + benchmark (downloads model + native on first run)…"
mvn test -P djl-native -Dtest=DjlRecallIT
