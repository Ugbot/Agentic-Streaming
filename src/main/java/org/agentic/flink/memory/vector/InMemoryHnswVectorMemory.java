package org.agentic.flink.memory.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agentic.flink.context.core.ContextItem;

/**
 * In-JVM {@link VectorMemory} backed by a plain {@link HashMap} for entry storage and a shared
 * {@link HnswGraph} for ANN — the same graph algorithm as {@link FlinkStateHnswVectorMemory} but
 * with no Flink {@code MapState}/{@code RuntimeContext} dependency, so it runs in a plain JVM (e.g.
 * the standalone A2A gateway's CS-agent RAG).
 *
 * <p>Not thread-safe; one instance per agent/operator. For workloads that need checkpointed,
 * per-key vector state inside a Flink job, use {@link FlinkStateHnswVectorMemory} instead — the SPI
 * is identical, so swapping is a config change.
 */
public final class InMemoryHnswVectorMemory implements VectorMemory {

  private final int dimension;
  private final HnswGraph graph;
  private final Map<String, VectorEntry> entries = new HashMap<>();

  public InMemoryHnswVectorMemory(int dimension, HnswBuildConfig config) {
    this.dimension = dimension;
    this.graph = new HnswGraph(dimension, config);
  }

  /** Build with default HNSW parameters. */
  public static InMemoryHnswVectorMemory of(int dimension) {
    return new InMemoryHnswVectorMemory(dimension, HnswBuildConfig.defaults());
  }

  @Override
  public void put(String id, float[] embedding, ContextItem item) {
    put(new VectorEntry(id, embedding, item));
  }

  @Override
  public void put(VectorEntry entry) {
    if (entry.getEmbedding().length != dimension) {
      throw new IllegalArgumentException(
          "Embedding dimension " + entry.getEmbedding().length
              + " does not match configured dimension " + dimension);
    }
    entries.put(entry.getId(), entry);
    graph.insert(entry.getId(), entry.getEmbedding());
  }

  @Override
  public void remove(String id) {
    entries.remove(id);
    graph.remove(id);
  }

  @Override
  public List<ScoredItem> search(float[] query, int k) {
    List<HnswGraph.Hit> hits = graph.search(query, k);
    List<ScoredItem> out = new ArrayList<>(hits.size());
    for (HnswGraph.Hit hit : hits) {
      VectorEntry e = entries.get(hit.id);
      if (e != null) {
        out.add(new ScoredItem(hit.id, hit.score, e.getItem()));
      }
    }
    return out;
  }

  @Override
  public int size() {
    return entries.size();
  }

  @Override
  public void clear() {
    entries.clear();
    graph.clear();
  }
}
