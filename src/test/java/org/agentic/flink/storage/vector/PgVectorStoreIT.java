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
import org.agentic.flink.storage.VectorStore.VectorSearchResult;
import org.agentic.flink.storage.postgres.PostgresTestDatabase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link PgVectorStore} against a real PostgreSQL with the pgvector extension ({@code
 * pgvector/pgvector:pg16} through Testcontainers on Podman). Covers the {@code vector(dim)} schema,
 * {@code ON CONFLICT} upserts, cosine ordering through the {@code <=>} operator, {@code jsonb}
 * containment filters and use of the store after Flink serialization.
 *
 * <p>Ordering assertions use one-hot basis vectors plus a query that is an exact copy of one of
 * them, so cosine distances are exactly 0 (the copy) and 1 (every orthogonal neighbour) and the
 * expected order does not depend on floating point rounding.
 */
@Tag("integration")
class PgVectorStoreIT {

  private static PostgresTestDatabase database;

  private PgVectorStore store;
  private int dimension;
  private String table;

  @BeforeAll
  static void startDatabase() {
    database = PostgresTestDatabase.startWithPgVector();
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) {
      database.close();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    dimension = 4 + ThreadLocalRandom.current().nextInt(4);
    table = "vectors_" + UUID.randomUUID().toString().replace('-', '_');
    store = new PgVectorStore();
    store.initialize(config());
  }

  @AfterEach
  void tearDown() {
    store.close();
  }

  private Map<String, String> config() {
    Map<String, String> config = new HashMap<>(database.storeConfig());
    config.put("postgres.dimension", Integer.toString(dimension));
    config.put("pgvector.table", table);
    return config;
  }

  private float[] basis(int axis) {
    float[] v = new float[dimension];
    v[axis] = 1f;
    return v;
  }

  private float[] randomVector() {
    float[] v = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      v[i] = ThreadLocalRandom.current().nextFloat() * 2f - 1f;
    }
    return v;
  }

  @Test
  @DisplayName("reports pgvector provider, configured dimension and metric")
  void metadata() throws Exception {
    assertEquals("pgvector", store.getProviderName());
    assertEquals(dimension, store.getEmbeddingDimension());
    assertEquals("cosine", store.getSimilarityMetric());
    assertEquals(0, store.getStatistics().get("total_embeddings"));
  }

  @Test
  @DisplayName("store, read back, upsert and delete a single embedding")
  void singleEmbeddingLifecycle() throws Exception {
    String id = "emb-" + UUID.randomUUID();
    float[] first = randomVector();
    String flow = "flow-" + UUID.randomUUID();
    store.storeEmbedding(id, first, Map.of("flow_id", flow, "n", 1));

    assertTrue(store.exists(id));
    assertArrayEquals(first, store.getEmbedding(id), 1e-6f);
    assertEquals(Map.of("flow_id", flow, "n", 1), store.getMetadata(id));

    float[] second = randomVector();
    store.storeEmbedding(id, second, Map.of("n", 2));
    assertArrayEquals(second, store.getEmbedding(id), 1e-6f);
    assertEquals(Map.of("n", 2), store.getMetadata(id));
    assertEquals(1, store.getStatistics().get("total_embeddings"));

    store.deleteEmbedding(id);
    assertFalse(store.exists(id));
    assertNull(store.getEmbedding(id));
    assertEquals(Map.of(), store.getMetadata(id));
  }

  @Test
  @DisplayName("an embedding of the wrong dimension is rejected before it reaches the database")
  void wrongDimensionRejected() throws Exception {
    float[] wrong = new float[dimension + 1 + ThreadLocalRandom.current().nextInt(3)];
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> store.storeEmbedding("bad-" + UUID.randomUUID(), wrong, Map.of()));
    assertTrue(e.getMessage().contains(Integer.toString(dimension)), e.getMessage());
    assertEquals(0, store.getStatistics().get("total_embeddings"));
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
    assertEquals(dimension, store.getStatistics().get("total_embeddings"));

    int target = ThreadLocalRandom.current().nextInt(dimension);
    List<VectorSearchResult> results = store.searchSimilar(basis(target), dimension);

    assertEquals(dimension, results.size());
    assertEquals(ids[target], results.get(0).getId());
    assertEquals(1f, results.get(0).getScore(), 1e-6f);
    assertEquals(Map.of("axis", target), results.get(0).getMetadata());
    for (VectorSearchResult r : results.subList(1, results.size())) {
      assertEquals(0f, r.getScore(), 1e-6f, r.getId());
    }

    List<VectorSearchResult> topOne = store.searchSimilar(basis(target), 1);
    assertEquals(List.of(ids[target]), topOne.stream().map(VectorSearchResult::getId).toList());
  }

  @Test
  @DisplayName("metadata filter narrows the candidates with jsonb containment")
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
    assertEquals(List.of(wantedId), filtered.stream().map(VectorSearchResult::getId).toList());
    assertEquals(0f, filtered.get(0).getScore(), 1e-6f);

    List<VectorSearchResult> unfiltered =
        store.searchSimilarWithFilter(basis(otherAxis), 10, Map.of());
    assertEquals(
        List.of(otherId, wantedId), unfiltered.stream().map(VectorSearchResult::getId).toList());

    assertTrue(
        store
            .searchSimilarWithFilter(
                basis(otherAxis), 10, Map.of("tenant", "nobody-" + UUID.randomUUID()))
            .isEmpty());
  }

  @Test
  @DisplayName("deleteByFlowId removes every embedding tagged with that flow and nothing else")
  void deleteByFlow() throws Exception {
    String flow = "flow-" + UUID.randomUUID();
    String otherFlow = "flow-" + UUID.randomUUID();
    int inFlow = 2 + ThreadLocalRandom.current().nextInt(4);
    for (int i = 0; i < inFlow; i++) {
      store.storeEmbedding("f-" + UUID.randomUUID(), randomVector(), Map.of("flow_id", flow));
    }
    String survivor = "s-" + UUID.randomUUID();
    store.storeEmbedding(survivor, randomVector(), Map.of("flow_id", otherFlow));
    assertEquals(inFlow + 1, store.getStatistics().get("total_embeddings"));

    store.deleteByFlowId(flow);

    assertEquals(1, store.getStatistics().get("total_embeddings"));
    assertTrue(store.exists(survivor));
  }

  @Test
  @DisplayName(
      "store serialized with Flink's InstantiationUtil reopens its pool and sees the same table")
  void usableAfterFlinkSerialization() throws Exception {
    String id = "emb-" + UUID.randomUUID();
    float[] vector = randomVector();
    store.storeEmbedding(id, vector, Map.of());

    byte[] bytes = InstantiationUtil.serializeObject(store);
    PgVectorStore copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
    try {
      assertArrayEquals(vector, copy.getEmbedding(id), 1e-6f);
      String fromCopy = "copy-" + UUID.randomUUID();
      copy.put(fromCopy, randomVector());
      assertTrue(store.exists(fromCopy));
      assertTrue(copy.get(fromCopy).isPresent());
      copy.delete(fromCopy);
      assertFalse(store.exists(fromCopy));
    } finally {
      copy.close();
    }
  }
}
