#!/usr/bin/env bash
# Flink-runtime showcase: retrieval-augmented research assistant (DJL embeddings and
# reranker, Ollama answer).
#
# Prerequisites: JDK 21, the Maven wrapper, Ollama at OLLAMA_URL with qwen2.5:3b
# (bash examples-bin/run-ollama.sh), and outbound internet on the first run: DJL downloads
# sentence-transformers/all-MiniLM-L6-v2, cross-encoder/mmarco-mMiniLMv2-L12-H384-v1 and the
# PyTorch native runtime (about 500 MB). No API key. Runs in an embedded MiniCluster.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_flink_example org.agentic.flink.example.rag.RagResearchExample
