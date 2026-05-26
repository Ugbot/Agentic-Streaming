#!/usr/bin/env bash
# Run the content-moderation Flink pipeline locally.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_example "org.agentic.flink.example.moderation.ContentModerationExample"
