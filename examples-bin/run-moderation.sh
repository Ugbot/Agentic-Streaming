#!/usr/bin/env bash
# Flink-runtime showcase: content moderation with a DJL toxicity classifier and an Ollama
# rewrite step.
#
# Prerequisites: JDK 21, the Maven wrapper, Ollama at OLLAMA_URL with qwen2.5:3b
# (bash examples-bin/run-ollama.sh), and outbound internet on the first run: DJL downloads
# the unitary/toxic-bert model from Hugging Face plus the PyTorch native runtime into
# the DJL cache directory (about 700 MB). No API key. Optional: AUDIT_ENDPOINT for the
# audit webhook (defaults to http://localhost:8081/audit; failures there are logged, not fatal).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_flink_example org.agentic.flink.example.moderation.ContentModerationExample
