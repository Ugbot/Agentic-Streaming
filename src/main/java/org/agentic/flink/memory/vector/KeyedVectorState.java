package org.agentic.flink.memory.vector;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Resolves the keyed state behind the Flink-state vector memories and turns Flink's generic "no
 * keyed state store" failure into an error that names the fix: bind the memory from a keyed
 * operator, for the shipped pipelines through {@code
 * RetrievalPipeline.StageSearch#search(CorpusSpec, int, DataStream)}.
 */
final class KeyedVectorState {
  private KeyedVectorState() {}

  static <K, V> MapState<K, V> mapState(
      RuntimeContext rc, MapStateDescriptor<K, V> descriptor, String provider) {
    try {
      return rc.getMapState(descriptor);
    } catch (NullPointerException | UnsupportedOperationException e) {
      throw unkeyed(provider, e);
    }
  }

  static <V> ValueState<V> valueState(
      RuntimeContext rc, ValueStateDescriptor<V> descriptor, String provider) {
    try {
      return rc.getState(descriptor);
    } catch (NullPointerException | UnsupportedOperationException e) {
      throw unkeyed(provider, e);
    }
  }

  private static IllegalStateException unkeyed(String provider, RuntimeException cause) {
    return new IllegalStateException(
        provider
            + " keeps its vectors in Flink keyed state and must be bound from a keyed operator"
            + " (keyBy the stream first). For the shipped corpus API wire ingest and queries into"
            + " one keyed operator with RetrievalPipeline.StageSearch.search(corpusSpec, k,"
            + " ingestionStage.embedded()).",
        cause);
  }
}
