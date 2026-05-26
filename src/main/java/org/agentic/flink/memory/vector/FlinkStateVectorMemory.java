package org.agentic.flink.memory.vector;

import org.agentic.flink.context.core.ContextItem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;

/**
 * Default {@link VectorMemory}: exact brute-force KNN backed by Flink {@code MapState}.
 *
 * <p>Why brute-force rather than HNSW? For the workload this framework targets (a few hundred to
 * a few thousand vectors per conversation), brute-force at d=768 is sub-millisecond and removes
 * three sources of complexity that HNSW carries: graph serialization, rebuild-on-restore, and
 * approximate-recall tuning. Users who outgrow it (10⁵+ vectors per key) plug a JVector- or
 * Lucene-HNSW-backed {@link VectorMemorySpec} in via {@code ServiceLoader}.
 *
 * <p>The state itself is a {@code MapState<String, VectorEntry>}, so checkpoints serialize each
 * vector individually under the configured state backend. No graph is materialized in memory
 * outside of an active {@link #search(float[], int)} call.
 */
public final class FlinkStateVectorMemory implements VectorMemory {

  private final MapState<String, VectorEntry> state;
  private final int dimension;
  private final VectorMemorySpec.Similarity similarity;
  private final int maxItems;

  private FlinkStateVectorMemory(
      MapState<String, VectorEntry> state,
      int dimension,
      VectorMemorySpec.Similarity similarity,
      int maxItems) {
    this.state = state;
    this.dimension = dimension;
    this.similarity = similarity;
    this.maxItems = maxItems;
  }

  /** Build a per-key brute-force spec at the given dimension. */
  public static VectorMemorySpec spec(int dimension) {
    return new Spec(dimension, VectorMemorySpec.Similarity.COSINE, 10_000);
  }

  public static VectorMemorySpec spec(
      int dimension, VectorMemorySpec.Similarity similarity, int maxItems) {
    return new Spec(dimension, similarity, maxItems);
  }

  @Override
  public void put(String id, float[] embedding, ContextItem item) throws Exception {
    put(new VectorEntry(id, embedding, item));
  }

  @Override
  public void put(VectorEntry entry) throws Exception {
    if (entry.getEmbedding().length != dimension) {
      throw new IllegalArgumentException(
          "Embedding dimension "
              + entry.getEmbedding().length
              + " does not match configured dimension "
              + dimension);
    }
    // Soft cap: if exceeded, evict the oldest item (by ContextItem.createdAt) before inserting.
    if (!state.contains(entry.getId()) && size() >= maxItems) {
      evictOldest();
    }
    state.put(entry.getId(), entry);
  }

  @Override
  public void remove(String id) throws Exception {
    state.remove(id);
  }

  @Override
  public List<ScoredItem> search(float[] query, int k) throws Exception {
    if (query.length != dimension) {
      throw new IllegalArgumentException(
          "Query dimension " + query.length + " does not match configured dimension " + dimension);
    }
    if (k <= 0) return Collections.emptyList();

    // Precompute query norm for cosine.
    double qNorm = similarity == VectorMemorySpec.Similarity.COSINE ? norm(query) : 1.0;
    if (similarity == VectorMemorySpec.Similarity.COSINE && qNorm == 0.0) {
      return Collections.emptyList();
    }

    // Bounded top-k via a min-heap of size k (smallest score on top, ready to evict).
    java.util.PriorityQueue<ScoredItem> heap = new java.util.PriorityQueue<>(k, (a, b) -> Double.compare(a.getScore(), b.getScore()));
    for (Map.Entry<String, VectorEntry> entry : state.entries()) {
      VectorEntry ve = entry.getValue();
      double score = similarityScore(query, ve.getEmbedding(), qNorm);
      ScoredItem si = new ScoredItem(ve.getId(), score, ve.getItem());
      if (heap.size() < k) {
        heap.offer(si);
      } else if (!heap.isEmpty() && score > heap.peek().getScore()) {
        heap.poll();
        heap.offer(si);
      }
    }

    List<ScoredItem> out = new ArrayList<>(heap);
    Collections.sort(out); // descending by score (natural order of ScoredItem)
    return out;
  }

  @Override
  public int size() throws Exception {
    int n = 0;
    Iterator<String> it = state.keys().iterator();
    while (it.hasNext()) {
      it.next();
      n++;
    }
    return n;
  }

  @Override
  public void clear() throws Exception {
    state.clear();
  }

  private double similarityScore(float[] q, float[] v, double qNorm) {
    switch (similarity) {
      case COSINE:
        double dot = dot(q, v);
        double vn = norm(v);
        return vn == 0.0 ? 0.0 : dot / (qNorm * vn);
      case DOT_PRODUCT:
        return dot(q, v);
      case NEGATIVE_L2:
        double d2 = 0.0;
        for (int i = 0; i < q.length; i++) {
          double diff = q[i] - v[i];
          d2 += diff * diff;
        }
        return -Math.sqrt(d2);
      default:
        throw new IllegalStateException("Unknown similarity: " + similarity);
    }
  }

  private static double dot(float[] a, float[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++) {
      s += a[i] * b[i];
    }
    return s;
  }

  private static double norm(float[] a) {
    double s = 0.0;
    for (float v : a) {
      s += v * v;
    }
    return Math.sqrt(s);
  }

  private void evictOldest() throws Exception {
    String victim = null;
    long oldest = Long.MAX_VALUE;
    for (Map.Entry<String, VectorEntry> e : state.entries()) {
      ContextItem item = e.getValue().getItem();
      long createdAt =
          item != null && item.getCreatedAt() != null ? item.getCreatedAt() : Long.MAX_VALUE;
      if (createdAt < oldest) {
        oldest = createdAt;
        victim = e.getKey();
      }
    }
    if (victim != null) {
      state.remove(victim);
    }
  }

  static final class Spec implements VectorMemorySpec {
    private static final long serialVersionUID = 1L;
    private final int dimension;
    private final Similarity similarity;
    private final int maxItems;

    Spec(int dimension, Similarity similarity, int maxItems) {
      if (dimension <= 0) throw new IllegalArgumentException("dimension must be positive");
      this.dimension = dimension;
      this.similarity = similarity;
      this.maxItems = maxItems;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public Similarity similarity() {
      return similarity;
    }

    @Override
    public int maxItems() {
      return maxItems;
    }

    @Override
    public VectorMemory bind(RuntimeContext rc) throws Exception {
      MapStateDescriptor<String, VectorEntry> descriptor =
          new MapStateDescriptor<>("vector.entries", String.class, VectorEntry.class);
      return new FlinkStateVectorMemory(
          rc.getMapState(descriptor), dimension, similarity, maxItems);
    }

    @Override
    public String providerName() {
      return "FlinkStateVectorMemory(brute-force, d=" + dimension + ", sim=" + similarity + ")";
    }
  }
}
