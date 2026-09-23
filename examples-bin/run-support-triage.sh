#!/usr/bin/env bash
# Flink-runtime showcase: support-ticket triage (DJL sentiment, zero-shot intent and
# reranker models, Ollama drafts).
#
# Prerequisites: JDK 21, the Maven wrapper, Ollama at OLLAMA_URL with qwen2.5:3b
# (bash examples-bin/run-ollama.sh), and outbound internet on the first run: DJL downloads
# distilbert-base-uncased-finetuned-sst-2-english, facebook/bart-large-mnli (1.6 GB),
# cross-encoder/mmarco-mMiniLMv2-L12-H384-v1 and the PyTorch native runtime. No API key.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_flink_example org.agentic.flink.example.triage.SupportTriageExample
