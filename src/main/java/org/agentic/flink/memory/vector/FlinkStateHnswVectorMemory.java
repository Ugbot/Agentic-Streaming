package org.agentic.flink.memory.vector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vector memory backed by Flink {@code MapState<String, VectorEntry>} with a navigable-small-world
 * (NSW) proximity graph ({@link HnswGraph}) as an in-memory acceleration structure.
 *
 * <p>This is a graph-based ANN — a flat single-layer HNSW-style index. The graph algorithm lives in
 * the shared, Flink-free {@link HnswGraph}; this class adds <b>durable, checkpointed storage</b> of
 * the originating {@link VectorEntry}s in Flink state. The graph is a transient acceleration
 * structure that is <b>per key</b>: the MapState is keyed, so each Flink key owns its own set of
 * vectors and its own graph. Because {@code bind()} runs in {@code open()} where no key is set,
 * nothing is rebuilt there. Instead every operation first resolves the current key's graph:
 * a keyed {@code ValueState<String>} holds a per-key graph id, and a bounded LRU of graphs keyed
 * by that id is consulted; on a miss (first access for that key, or after a restore) the graph is
 * rebuilt by replaying that key's MapState (see {@link #graphForCurrentKey}). This keeps vectors
 * of different keys isolated and makes state restored from a checkpoint searchable again.
 * For larger graphs swap in a JVector- or Lucene-HNSW-backed {@link VectorMemorySpec} via the
 * {@code ServiceLoader} path — the abstraction here is identical. For a non-Flink (plain JVM) host,
 * use {@link InMemoryHnswVectorMemory}, which shares the same {@link HnswGraph}.
 */
public final class FlinkStateHnswVectorMemory implements VectorMemory {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkStateHnswVectorMemory.class);

  public static final String ENTRIES_STATE = "vector.hnsw.entries";
  public static final String GRAPH_ID_STATE = "vector.hnsw.graph-id";
  public static final int DEFAULT_MAX_CACHED_KEYS = 64;

  private final MapState<String, VectorEntry> state;
  private final ValueState<String> graphIdState;
  private final int dimension;
  private final HnswBuildConfig config;
  private final LinkedHashMap<String, HnswGraph> graphs;
  private final int maxCachedKeys;
  private int rebuilds;

  private FlinkStateHnswVectorMemory(
      MapState<String, VectorEntry> state,
      ValueState<String> graphIdState,
      int dimension,
      HnswBuildConfig config,
      int maxCachedKeys) {
    this.state = state;
    this.graphIdState = graphIdState;
    this.dimension = dimension;
    this.config = config;
    this.maxCachedKeys = maxCachedKeys;
    this.graphs = new LinkedHashMap<>(16, 0.75f, true);
  }

  /** Build a spec at the given dimension with default HNSW parameters. */
  public static VectorMemorySpec spec(int dimension) {
    return spec(dimension, HnswBuildConfig.defaults());
  }

  /** Build a spec with a fully-specified {@link HnswBuildConfig}. */
  public static VectorMemorySpec spec(int dimension, HnswBuildConfig config) {
    return new Spec(dimension, config, DEFAULT_MAX_CACHED_KEYS);
  }

  /**
   * Build a spec that keeps at most {@code maxCachedKeys} per-key graphs in memory; graphs for
   * other keys are rebuilt from state on their next access.
   */
  public static VectorMemorySpec spec(int dimension, HnswBuildConfig config, int maxCachedKeys) {
    return new Spec(dimension, config, maxCachedKeys);
  }

  /** Number of per-key graph rebuilds performed so far (diagnostics and tests). */
  public int rebuildCount() {
    return rebuilds;
  }

  /** Number of per-key graphs currently cached in memory. */
  public int cachedGraphCount() {
    return graphs.size();
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
    HnswGraph graph = graphForCurrentKey();
    state.put(entry.getId(), entry);
    graph.insert(entry.getId(), entry.getEmbedding());
  }

  @Override
  public void remove(String id) throws Exception {
    HnswGraph graph = graphForCurrentKey();
    state.remove(id);
    graph.remove(id);
  }

  @Override
  public List<ScoredItem> search(float[] query, int k) throws Exception {
    List<HnswGraph.Hit> hits = graphForCurrentKey().search(query, k);
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
    return graphForCurrentKey().size();
  }

  @Override
  public void clear() throws Exception {
    HnswGraph graph = graphForCurrentKey();
    state.clear();
    graph.clear();
  }

  /**
   * Resolves the graph of the key currently set on the keyed state backend, rebuilding it from
   * that key's MapState when it is not cached.
   */
  private HnswGraph graphForCurrentKey() throws Exception {
    String graphId = graphIdState.value();
    if (graphId == null) {
      graphId = UUID.randomUUID().toString();
      graphIdState.update(graphId);
    }
    HnswGraph graph = graphs.get(graphId);
    if (graph != null) {
      return graph;
    }
    graph = rebuildFromState();
    graphs.put(graphId, graph);
    while (graphs.size() > maxCachedKeys) {
      String eldest = graphs.keySet().iterator().next();
      graphs.remove(eldest);
    }
    return graph;
  }

  /** Replays the current key's MapState into a fresh graph. */
  private HnswGraph rebuildFromState() throws Exception {
    long started = System.nanoTime();
    HnswGraph graph = new HnswGraph(dimension, config);
    int count = 0;
    for (Map.Entry<String, VectorEntry> e : state.entries()) {
      VectorEntry entry = e.getValue();
      graph.insert(entry.getId(), entry.getEmbedding());
      count++;
    }
    rebuilds++;
    long durationMs = (System.nanoTime() - started) / 1_000_000;
    if (count > 0) {
      LOG.info(
          "FlinkStateHnswVectorMemory rebuilt per-key graph from MapState: {} vectors in {} ms",
          count, durationMs);
    }
    return graph;
  }

  /** Serializable spec. */
  static final class Spec implements VectorMemorySpec {
    private static final long serialVersionUID = 2L;
    private final int dimension;
    private final HnswBuildConfig config;
    private final int maxCachedKeys;

    Spec(int dimension, HnswBuildConfig config, int maxCachedKeys) {
      if (dimension <= 0) throw new IllegalArgumentException("dimension must be positive");
      if (maxCachedKeys <= 0) throw new IllegalArgumentException("maxCachedKeys must be positive");
      this.dimension = dimension;
      this.config = config == null ? HnswBuildConfig.defaults() : config;
      this.maxCachedKeys = maxCachedKeys;
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
          new MapStateDescriptor<>(ENTRIES_STATE, String.class, VectorEntry.class);
      ValueStateDescriptor<String> graphIdDescriptor =
          new ValueStateDescriptor<>(GRAPH_ID_STATE, String.class);
      return new FlinkStateHnswVectorMemory(
          rc.getMapState(descriptor), rc.getState(graphIdDescriptor), dimension, config,
          maxCachedKeys);
    }

    @Override
    public String providerName() {
      return "FlinkStateHnswVectorMemory(d=" + dimension + ", M=" + config.getM()
          + ", beam=" + config.getBeamWidth() + ", search=" + config.getSearchBeam() + ")";
    }
  }
}
