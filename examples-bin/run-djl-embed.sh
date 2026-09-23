#!/usr/bin/env bash
# Live DJL (Deep Java Library) embedding demo and micro-benchmark.
#
# Loads sentence-transformers/all-MiniLM-L6-v2 through DJL/PyTorch, embeds a small corpus into
# the RAG hot index, checks that a paraphrased query retrieves the on-topic passage as top-1,
# and prints the mean embedding latency. It is a thin wrapper over the @Tag("djl") test
# DjlRecallIT, run under the djl-native Maven profile; DJL tests are excluded from a plain
# ./mvnw test because the PyTorch native library is not part of the default build.
#
# Prerequisites: JDK 21, the Maven wrapper, and outbound internet on the first run (DJL
# downloads the model and the CPU native library into its cache directory, DJL_CACHE_DIR or
# $HOME/.djl.ai by default). No API key, no containers.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

require_java21
require_mvnw
ensure_jagentic_core
info "running DjlRecallIT (downloads the model and native library on first run)"
cd "$REPO_ROOT"
mvn_run test -P djl-native -Dtest=DjlRecallIT
