#!/usr/bin/env bash
# Flink-runtime showcase: anomaly detection + CEP + one LLM call per confirmed incident.
#
# Prerequisites: JDK 21, the Maven wrapper, Ollama at OLLAMA_URL with qwen2.5:3b
# (bash examples-bin/run-ollama.sh starts it in Podman). No API key. Runs an embedded
# Flink MiniCluster and exits when the bounded input is drained (about one minute).
# Expected output: one "created INC-1 host=host-a metric=latency_ms" line followed by
# "incident#1 ticket=INC-1 plan=..." with the model's remediation steps.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

check_ollama
run_flink_example org.agentic.flink.example.incident.IncidentAgentExample
