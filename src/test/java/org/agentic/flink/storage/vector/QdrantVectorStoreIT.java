package org.agentic.flink.storage.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.storage.StorageTier;
import org.agentic.flink.storage.VectorStore.VectorSearchResult;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link QdrantVectorStore} against a real Qdrant ({@code qdrant/qdrant} through Testcontainers on
 * Podman) over its native gRPC client.
 *
 * <p>The test is deterministic because of two Qdrant properties: below the default indexing
 * threshold (20000 points) a collection is searched exhaustively, so results are exact rather than
 * approximate; and with the cosine distance Qdrant normalizes every vector on write, so the one-hot
 * basis vectors used here are stored unchanged and a query that copies one of them scores exactly 1
 * against the copy and exactly 0 against every orthogonal neighbour. Vectors written with a
 * non-unit norm are read back normalized, so the read-back checks use unit vectors.
 */
@Tag("integration")
class QdrantVectorStoreIT {

  private static QdrantTestServer server;

  private QdrantVectorStore store;
  private int dimension;
  private String collection;

  @BeforeAll
  static void startServer() {
    server = QdrantTestServer.start();
  }

  @AfterAll
  static void stopServer() {
    if (server != null) {
      server.close();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    dimension = 4 + ThreadLocalRandom.current().nextInt(4);
    collection = "vectors_" + UUID.randomUUID().toString().replace('-', '_');
    store = new QdrantVectorStore();
    store.initialize(config());
  }

  @AfterEach
  void tearDown() {
    store.close();
  }

  private Map<String, String> config() {
    Map<String, String> config = new HashMap<>(server.storeConfig());
    config.put("qdrant.collection", collection);
    config.put("vector.dimension", Integer.toString(dimension));
    return config;
  }

  private float[] basis(int axis) {
    float[] v = new float[dimension];
    v[axis] = 1f;
    return v;
  }

  private float[] randomUnitVector() {
    float[] v = new float[dimension];
    double norm = 0;
    for (int i = 0; i < dimension; i++) {
      v[i] = ThreadLocalRandom.current().nextFloat() * 2f - 1f;
      norm += v[i] * v[i];
    }
    float scale = (float) (1.0 / Math.sqrt(norm));
    for (int i = 0; i < dimension; i++) {
      v[i] *= scale;
    }
    return v;
  }

  private static List<String> ids(List<VectorSearchResult> results) {
    return results.stream().map(VectorSearchResult::getId).toList();
  }

  @Test
  @DisplayName("reports the qdrant provider, vector tier, configured dimension and metric")
  void metadata() throws Exception {
    assertEquals("qdrant", store.getProviderName());
    assertEquals(StorageTier.VECTOR, store.getTier());
    assertEquals(dimension, store.getEmbeddingDimension());
    assertEquals("cosine", store.getSimilarityMetric());
    Map<String, Object> stats = store.getStatistics();
    assertEquals(0L, stats.get("total_vectors"));
    assertEquals(collection, stats.get("collection"));
  }

  @Test
  @DisplayName("store, read back, upsert and delete a single embedding with its payload")
  void singleEmbeddingLifecycle() throws Exception {
    String id = "emb-" + UUID.randomUUID();
    float[] first = randomUnitVector();
    String flow = "flow-" + UUID.randomUUID();
    long n = ThreadLocalRandom.current().nextLong(1, 1_000_000);
    store.storeEmbedding(id, first, Map.of("flowId", flow, "n", n));

    assertTrue(store.exists(id));
    assertArrayEquals(first, store.getEmbedding(id), 1e-5f);
    Map<String, Object> metadata = store.getMetadata(id);
    assertEquals(id, metadata.get("_id"));
    assertEquals(flow, metadata.get("flowId"));
    assertEquals(n, metadata.get("n"));

    float[] second = randomUnitVector();
    store.storeEmbedding(id, second, Map.of("n", n + 1));
    assertArrayEquals(second, store.getEmbedding(id), 1e-5f);
    assertEquals(n + 1, store.getMetadata(id).get("n"));
    assertEquals(1L, store.getStatistics().get("total_vectors"));

    store.deleteEmbedding(id);
    assertFalse(store.exists(id));
    assertNull(store.getEmbedding(id));
    assertNull(store.getMetadata(id));
    assertEquals(0L, store.getStatistics().get("total_vectors"));
  }

  @Test
  @DisplayName("an embedding of the wrong dimension is rejected before it reaches the server")
  void wrongDimensionRejected() throws Exception {
    float[] wrong = new float[dimension + 1 + ThreadLocalRandom.current().nextInt(3)];
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> store.storeEmbedding("bad-" + UUID.randomUUID(), wrong, Map.of()));
    assertTrue(e.getMessage().contains(Integer.toString(dimension)), e.getMessage());
    assertEquals(0L, store.getStatistics().get("total_vectors"));
  }

  @Test
  @DisplayName("cosine search returns the exact match first, then the orthogonal neighbours")
  void cosineOrdering() throws Exception {
    Map<String, float[]> embeddings = new HashMap<>();
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    String[] ids = new String[dimension];
    for (int axis = 0; axis < dimension; axis++) {
      ids[axis] = "axis-" + axis + "-" + UUID.randomUUID();
      embeddings.put(ids[axis], basis(axis));
      metadata.put(ids[axis], Map.of("axis", axis));
    }
    store.storeEmbeddingsBatch(embeddings, metadata);
    assertEquals((long) dimension, store.getStatistics().get("total_vectors"));

    int target = ThreadLocalRandom.current().nextInt(dimension);
    List<VectorSearchResult> results = store.searchSimilar(basis(target), dimension);

    assertEquals(dimension, results.size());
    assertEquals(ids[target], results.get(0).getId());
    assertEquals(1f, results.get(0).getScore(), 1e-6f);
    assertEquals((long) target, results.get(0).getMetadata().get("axis"));
    for (VectorSearchResult r : results.subList(1, results.size())) {
      assertEquals(0f, r.getScore(), 1e-6f, r.getId());
    }

    assertEquals(List.of(ids[target]), ids(store.searchSimilar(basis(target), 1)));
    assertTrue(store.searchSimilar(basis(target), 0).isEmpty());
  }

  @Test
  @DisplayName("payload filter narrows the candidates to matching points")
  void filteredSearch() throws Exception {
    String wanted = "tenant-" + UUID.randomUUID();
    String other = "tenant-" + UUID.randomUUID();
    String wantedId = "w-" + UUID.randomUUID();
    String otherId = "o-" + UUID.randomUUID();
    int wantedAxis = ThreadLocalRandom.current().nextInt(dimension);
    int otherAxis = (wantedAxis + 1) % dimension;
    store.storeEmbedding(otherId, basis(otherAxis), Map.of("tenant", other));
    store.storeEmbedding(wantedId, basis(wantedAxis), Map.of("tenant", wanted));

    List<VectorSearchResult> filtered =
        store.searchSimilarWithFilter(basis(otherAxis), 10, Map.of("tenant", wanted));
    assertEquals(List.of(wantedId), ids(filtered));
    assertEquals(0f, filtered.get(0).getScore(), 1e-6f);
    assertEquals(wanted, filtered.get(0).getMetadata().get("tenant"));

    List<VectorSearchResult> unfiltered =
        store.searchSimilarWithFilter(basis(otherAxis), 10, Map.of());
    assertEquals(List.of(otherId, wantedId), ids(unfiltered));

    assertTrue(
        store
            .searchSimilarWithFilter(
                basis(otherAxis), 10, Map.of("tenant", "nobody-" + UUID.randomUUID()))
            .isEmpty());
  }

  @Test
  @DisplayName("deleteByFlowId removes every point whose flowId payload matches and nothing else")
  void deleteByFlow() throws Exception {
    String flow = "flow-" + UUID.randomUUID();
    String otherFlow = "flow-" + UUID.randomUUID();
    int inFlow = 2 + ThreadLocalRandom.current().nextInt(4);
    for (int i = 0; i < inFlow; i++) {
      store.storeEmbedding("f-" + UUID.randomUUID(), randomUnitVector(), Map.of("flowId", flow));
    }
    String survivor = "s-" + UUID.randomUUID();
    store.storeEmbedding(survivor, randomUnitVector(), Map.of("flowId", otherFlow));
    assertEquals((long) inFlow + 1, store.getStatistics().get("total_vectors"));

    store.deleteByFlowId(flow);

    assertEquals(1L, store.getStatistics().get("total_vectors"));
    assertTrue(store.exists(survivor));
  }

  @Test
  @DisplayName("store serialized with Flink's InstantiationUtil reconnects to the same collection")
  void usableAfterFlinkSerialization() throws Exception {
    String id = "emb-" + UUID.randomUUID();
    float[] vector = randomUnitVector();
    store.storeEmbedding(id, vector, Map.of());

    byte[] bytes = InstantiationUtil.serializeObject(store);
    QdrantVectorStore copy =
        InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
    try {
      assertArrayEquals(vector, copy.getEmbedding(id), 1e-5f);
      String fromCopy = "copy-" + UUID.randomUUID();
      copy.put(fromCopy, randomUnitVector());
      assertTrue(store.exists(fromCopy));
      assertTrue(copy.get(fromCopy).isPresent());
      copy.delete(fromCopy);
      assertFalse(store.exists(fromCopy));
    } finally {
      copy.close();
    }
  }
}
