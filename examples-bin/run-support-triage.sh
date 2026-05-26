#!/usr/bin/env bash
# Run the support-triage example end-to-end against local Ollama + DJL.
# Models are downloaded on first run by DJL into ~/.djl.ai (≈250 MB).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_example "org.agentic.flink.example.triage.SupportTriageExample"
