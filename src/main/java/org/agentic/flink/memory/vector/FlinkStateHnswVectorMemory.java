package org.agentic.flink.memory.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vector memory backed by Flink {@code MapState<String, VectorEntry>} with a navigable-small-world
 * (NSW) proximity graph ({@link HnswGraph}) as an in-memory acceleration structure.
 *
 * <p>This is a graph-based ANN — a flat single-layer HNSW-style index. The graph algorithm lives in
 * the shared, Flink-free {@link HnswGraph}; this class adds <b>durable, checkpointed storage</b> of
 * the originating {@link VectorEntry}s in Flink state. The graph is a transient acceleration
 * structure rebuilt on operator {@code open()} by replaying MapState (see {@link #rebuildFromState}).
 * For larger graphs swap in a JVector- or Lucene-HNSW-backed {@link VectorMemorySpec} via the
 * {@code ServiceLoader} path — the abstraction here is identical. For a non-Flink (plain JVM) host,
 * use {@link InMemoryHnswVectorMemory}, which shares the same {@link HnswGraph}.
 */
public final class FlinkStateHnswVectorMemory implements VectorMemory {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkStateHnswVectorMemory.class);

  private final MapState<String, VectorEntry> state;
  private final int dimension;
  private final HnswGraph graph;

  private FlinkStateHnswVectorMemory(
      MapState<String, VectorEntry> state, int dimension, HnswBuildConfig config) {
    this.state = state;
    this.dimension = dimension;
    this.graph = new HnswGraph(dimension, config);
  }

  /** Build a spec at the given dimension with default HNSW parameters. */
  public static VectorMemorySpec spec(int dimension) {
    return spec(dimension, HnswBuildConfig.defaults());
  }

  /** Build a spec with a fully-specified {@link HnswBuildConfig}. */
  public static VectorMemorySpec spec(int dimension, HnswBuildConfig config) {
    return new Spec(dimension, config);
  }

  @Override
  public void put(String id, float[] embedding, org.agentic.flink.context.core.ContextItem item)
      throws Exception {
    put(new VectorEntry(id, embedding, item));
  }

  @Override
  public void put(VectorEntry entry) throws Exception {
    if (entry.getEmbedding().length != dimension) {
      throw new IllegalArgumentException(
          "Embedding dimension " + entry.getEmbedding().length
              + " does not match configured dimension " + dimension);
    }
    state.put(entry.getId(), entry);
    graph.insert(entry.getId(), entry.getEmbedding());
  }

  @Override
  public void remove(String id) throws Exception {
    state.remove(id);
    graph.remove(id);
  }

  @Override
  public List<ScoredItem> search(float[] query, int k) throws Exception {
    List<HnswGraph.Hit> hits = graph.search(query, k);
    List<ScoredItem> out = new ArrayList<>(hits.size());
    for (HnswGraph.Hit hit : hits) {
      VectorEntry e = state.get(hit.id);
      if (e != null) {
        out.add(new ScoredItem(hit.id, hit.score, e.getItem()));
      }
    }
    return out;
  }

  @Override
  public int size() throws Exception {
    return graph.size();
  }

  @Override
  public void clear() throws Exception {
    state.clear();
    graph.clear();
  }

  /** Bind-time rebuild: replay MapState into the in-memory graph. */
  private void rebuildFromState() throws Exception {
    long started = System.nanoTime();
    int count = 0;
    for (Map.Entry<String, VectorEntry> e : state.entries()) {
      VectorEntry entry = e.getValue();
      graph.insert(entry.getId(), entry.getEmbedding());
      count++;
    }
    long durationMs = (System.nanoTime() - started) / 1_000_000;
    if (count > 0) {
      LOG.info(
          "FlinkStateHnswVectorMemory rebuilt graph from MapState: {} vectors in {} ms",
          count, durationMs);
    }
  }

  /** Serializable spec. */
  static final class Spec implements VectorMemorySpec {
    private static final long serialVersionUID = 1L;
    private final int dimension;
    private final HnswBuildConfig config;

    Spec(int dimension, HnswBuildConfig config) {
      if (dimension <= 0) throw new IllegalArgumentException("dimension must be positive");
      this.dimension = dimension;
      this.config = config == null ? HnswBuildConfig.defaults() : config;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public Similarity similarity() {
      return config.getSimilarity();
    }

    @Override
    public VectorMemory bind(RuntimeContext rc) throws Exception {
      MapStateDescriptor<String, VectorEntry> descriptor =
          new MapStateDescriptor<>("vector.hnsw.entries", String.class, VectorEntry.class);
      FlinkStateHnswVectorMemory memory =
          new FlinkStateHnswVectorMemory(rc.getMapState(descriptor), dimension, config);
      memory.rebuildFromState();
      return memory;
    }

    @Override
    public String providerName() {
      return "FlinkStateHnswVectorMemory(d=" + dimension + ", M=" + config.getM()
          + ", beam=" + config.getBeamWidth() + ", search=" + config.getSearchBeam() + ")";
    }
  }
}
