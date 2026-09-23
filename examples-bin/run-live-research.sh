#!/usr/bin/env bash
# Flink-runtime showcase: live web research (design sketch, not runnable end to end).
#
# LiveResearchExample composes CrawlerCore, IngestionPipeline and RetrievalPipeline over a
# BroadcastCorpus(FlinkStateHnswVectorMemory). As of this checkout the composition does not run:
#   1. IngestionPipeline.into() and RetrievalPipeline.search() are plain (unkeyed) operators, and
#      FlinkStateHnswVectorMemory binds keyed MapState, so the job fails in open() with
#      "Keyed state 'vector.hnsw.entries' ... can only be used on a 'keyed stream'".
#   2. Even on a keyed stream the ingest and search operators would hold separate Flink state,
#      so the retrieve side would never see what the ingest side indexed; nothing in the
#      framework broadcasts the corpus between them yet.
#   3. The crawl-url ToolInvocationChannel opens an unbounded polling source, so the job never
#      finishes and the queries are answered before any page is indexed.
# Fixing this needs a shared corpus transport in the framework, which is outside example repair.
# For a working Flink RAG loop use examples-bin/run-rag.sh (RagResearchExample).
#
# Set AGENTIC_RUN_LIVE_RESEARCH=1 to run the example anyway and see the failure yourself.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

if [ "${AGENTIC_RUN_LIVE_RESEARCH:-0}" != "1" ]; then
  err "LiveResearchExample is a design sketch and does not run end to end in this checkout:"
  err "  its IngestionPipeline and RetrievalPipeline operators are unkeyed, so the Flink-state"
  err "  HNSW corpus cannot bind, the two operators would not share state anyway, and the crawl-url"
  err "  tool channel keeps the job open forever. See docs/examples/live-research.md."
  err "Use examples-bin/run-rag.sh for a working Flink RAG example, or set"
  err "AGENTIC_RUN_LIVE_RESEARCH=1 to run this one anyway."
  exit 2
fi

check_ollama
run_flink_example org.agentic.flink.example.research.LiveResearchExample
