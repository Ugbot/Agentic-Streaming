#!/usr/bin/env bash
# Run the live-research example. Boots Ollama + DJL primitives; reads from local Wikipedia
# pages as seeds, drives a few queries, lets the LLM steer the crawler if it wants.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_example "org.agentic.flink.example.research.LiveResearchExample"
