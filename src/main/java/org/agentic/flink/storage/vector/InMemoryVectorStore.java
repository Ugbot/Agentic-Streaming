package org.agentic.flink.storage.vector;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.storage.StorageTier;
import org.agentic.flink.storage.VectorStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-JVM {@link VectorStore} backed by a concurrent map with brute-force K-NN search.
 *
 * <p>Zero external infrastructure — the default vector backend for tests, local development, and
 * small RAG corpora. Search is O(n) per query, which is sub-millisecond for a few thousand vectors
 * and fine into the low tens of thousands. For larger corpora use {@link PgVectorStore},
 * {@code QdrantVectorStore}, or {@code MilvusVectorStore}, or the Flink-state-backed
 * {@code FlinkStateVectorMemory} / {@code FlinkStateHnswVectorMemory}.
 *
 * <p>Config keys (all optional): {@code vector.dimension} (validated on store if set),
 * {@code vector.similarity} (one of {@code cosine} (default), {@code euclidean}, {@code dot_product}).
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; provider name {@code "in-memory"}.
 */
public final class InMemoryVectorStore implements VectorStore {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryVectorStore.class);

  private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();
  private int dimension = 0; // 0 = infer from first stored vector
  private String similarity = "cosine";

  public InMemoryVectorStore() {}

  @Override
  public void initialize(Map<String, String> config) {
    if (config != null) {
      String dim = config.get("vector.dimension");
      if (dim != null && !dim.isBlank()) {
        this.dimension = Integer.parseInt(dim.trim());
      }
      String sim = config.get("vector.similarity");
      if (sim != null && !sim.isBlank()) {
        this.similarity = sim.trim().toLowerCase();
      }
    }
    LOG.info("InMemoryVectorStore initialized: dim={} similarity={}", dimension, similarity);
  }

  @Override
  public void storeEmbedding(String id, float[] embedding, Map<String, Object> metadata) {
    if (id == null) throw new IllegalArgumentException("id must not be null");
    if (embedding == null) throw new IllegalArgumentException("embedding must not be null");
    if (dimension == 0) {
      dimension = embedding.length;
    } else if (embedding.length != dimension) {
      throw new IllegalArgumentException(
          "embedding dimension " + embedding.length + " != store dimension " + dimension);
    }
    Map<String, Object> meta =
        metadata == null ? new HashMap<>() : new HashMap<>(metadata);
    store.put(id, new Entry(embedding.clone(), meta));
  }

  @Override
  public void storeEmbeddingsBatch(
      Map<String, float[]> embeddings, Map<String, Map<String, Object>> metadata)
      throws Exception {
    if (embeddings == null) return;
    for (Map.Entry<String, float[]> e : embeddings.entrySet()) {
      Map<String, Object> meta =
          metadata == null ? null : metadata.get(e.getKey());
      storeEmbedding(e.getKey(), e.getValue(), meta);
    }
  }

  @Override
  public List<VectorSearchResult> searchSimilar(float[] queryEmbedding, int topK) {
    return searchSimilarWithFilter(queryEmbedding, topK, null);
  }

  @Override
  public List<VectorSearchResult> searchSimilarWithFilter(
      float[] queryEmbedding, int topK, Map<String, Object> metadataFilter) {
    if (queryEmbedding == null) throw new IllegalArgumentException("queryEmbedding must not be null");
    if (topK <= 0) return new ArrayList<>();

    List<VectorSearchResult> scored = new ArrayList<>();
    for (Map.Entry<String, Entry> e : store.entrySet()) {
      Entry entry = e.getValue();
      if (!matchesFilter(entry.metadata, metadataFilter)) continue;
      float score = score(queryEmbedding, entry.vector);
      scored.add(new VectorSearchResult(e.getKey(), score, new HashMap<>(entry.metadata)));
    }
    scored.sort(Comparator.comparingDouble(VectorSearchResult::getScore).reversed());
    return scored.size() > topK ? new ArrayList<>(scored.subList(0, topK)) : scored;
  }

  @Override
  public void storeContextItem(String flowId, ContextItem item) {
    throw new UnsupportedOperationException(
        "InMemoryVectorStore.storeContextItem requires an external embedder; use storeEmbedding");
  }

  @Override
  public List<ContextItem> searchContextItems(String queryText, int topK) {
    throw new UnsupportedOperationException(
        "InMemoryVectorStore.searchContextItems requires an external embedder; use searchSimilar");
  }

  @Override
  public float[] getEmbedding(String id) {
    Entry e = store.get(id);
    return e == null ? null : e.vector.clone();
  }

  @Override
  public Map<String, Object> getMetadata(String id) {
    Entry e = store.get(id);
    return e == null ? null : new HashMap<>(e.metadata);
  }

  @Override
  public void deleteEmbedding(String id) {
    store.remove(id);
  }

  @Override
  public void deleteByFlowId(String flowId) {
    store.values().removeIf(e -> flowId != null && flowId.equals(e.metadata.get("flowId")));
    // ConcurrentHashMap.values().removeIf is supported; entries keyed by id are pruned in place.
  }

  @Override
  public int getEmbeddingDimension() {
    return dimension;
  }

  @Override
  public String getSimilarityMetric() {
    return similarity;
  }

  @Override
  public Map<String, Object> getStatistics() {
    Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("total_vectors", store.size());
    stats.put("dimension", dimension);
    stats.put("similarity", similarity);
    return stats;
  }

  @Override
  public void createCollection(String collectionName, int dimension, Map<String, Object> config) {
    // Single flat namespace; nothing to create.
  }

  @Override
  public String getProviderName() {
    return "in-memory";
  }

  // ---------- StorageProvider plumbing ----------

  @Override
  public void put(String key, float[] value) {
    storeEmbedding(key, value, Map.of());
  }

  @Override
  public Optional<float[]> get(String key) {
    return Optional.ofNullable(getEmbedding(key));
  }

  @Override
  public void delete(String key) {
    deleteEmbedding(key);
  }

  @Override
  public boolean exists(String key) {
    return store.containsKey(key);
  }

  @Override
  public void close() {
    store.clear();
  }

  @Override
  public StorageTier getTier() {
    return StorageTier.VECTOR;
  }

  @Override
  public long getExpectedLatencyMs() {
    return 1; // in-JVM brute force
  }

  // ---------- helpers ----------

  private static boolean matchesFilter(Map<String, Object> metadata, Map<String, Object> filter) {
    if (filter == null || filter.isEmpty()) return true;
    for (Map.Entry<String, Object> f : filter.entrySet()) {
      Object actual = metadata.get(f.getKey());
      if (actual == null ? f.getValue() != null : !actual.equals(f.getValue())) {
        return false;
      }
    }
    return true;
  }

  private float score(float[] q, float[] v) {
    switch (similarity) {
      case "euclidean":
      case "l2":
        return (float) (1.0 / (1.0 + Math.sqrt(squaredDistance(q, v))));
      case "dot_product":
        return dot(q, v);
      case "cosine":
      default:
        return cosine(q, v);
    }
  }

  private static float cosine(float[] a, float[] b) {
    double dot = 0, na = 0, nb = 0;
    int n = Math.min(a.length, b.length);
    for (int i = 0; i < n; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    if (na == 0 || nb == 0) return 0f;
    return (float) (dot / (Math.sqrt(na) * Math.sqrt(nb)));
  }

  private static float dot(float[] a, float[] b) {
    double dot = 0;
    int n = Math.min(a.length, b.length);
    for (int i = 0; i < n; i++) dot += a[i] * b[i];
    return (float) dot;
  }

  private static double squaredDistance(float[] a, float[] b) {
    double sum = 0;
    int n = Math.min(a.length, b.length);
    for (int i = 0; i < n; i++) {
      double d = a[i] - b[i];
      sum += d * d;
    }
    return sum;
  }

  private static final class Entry {
    final float[] vector;
    final Map<String, Object> metadata;

    Entry(float[] vector, Map<String, Object> metadata) {
      this.vector = vector;
      this.metadata = metadata;
    }
  }
}
