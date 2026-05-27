package org.agentic.flink.storage.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.VectorStore.VectorSearchResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/** Randomized unit tests for {@link InMemoryVectorStore}. */
class InMemoryVectorStoreTest {

  private static float[] randomUnit(int dim) {
    float[] v = new float[dim];
    double norm = 0;
    for (int i = 0; i < dim; i++) {
      v[i] = (float) ThreadLocalRandom.current().nextGaussian();
      norm += v[i] * v[i];
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < dim; i++) v[i] /= (float) norm;
    return v;
  }

  @Test
  void plantedNearestNeighbourRecoveredAtRankOne() throws Exception {
    int dim = ThreadLocalRandom.current().nextInt(16, 128);
    int n = ThreadLocalRandom.current().nextInt(200, 800);
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(Map.of("vector.similarity", "cosine"));

    for (int i = 0; i < n; i++) {
      store.storeEmbedding("doc-" + i, randomUnit(dim), Map.of("flowId", "f" + (i % 7)));
    }

    // Plant a near-duplicate of a query.
    float[] query = randomUnit(dim);
    float[] planted = query.clone();
    for (int i = 0; i < dim; i++) planted[i] += 0.01f * (float) ThreadLocalRandom.current().nextGaussian();
    store.storeEmbedding("planted", planted, Map.of("flowId", "planted"));

    List<VectorSearchResult> top = store.searchSimilar(query, 5);
    assertEquals("planted", top.get(0).getId(), "planted near-duplicate should rank first");
    assertTrue(top.get(0).getScore() > 0.9f, "cosine score should be high for near-duplicate");
    assertEquals(5, top.size());
    // Scores must be in descending order.
    for (int i = 1; i < top.size(); i++) {
      assertTrue(top.get(i - 1).getScore() >= top.get(i).getScore());
    }
  }

  @Test
  void metadataFilterRestrictsResults() throws Exception {
    int dim = 32;
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(new HashMap<>());
    for (int i = 0; i < 100; i++) {
      Map<String, Object> meta = Map.of("flowId", i % 2 == 0 ? "even" : "odd");
      store.storeEmbedding("d" + i, randomUnit(dim), meta);
    }
    List<VectorSearchResult> results =
        store.searchSimilarWithFilter(randomUnit(dim), 100, Map.of("flowId", "even"));
    assertEquals(50, results.size());
    for (VectorSearchResult r : results) {
      assertEquals("even", r.getMetadata().get("flowId"));
    }
  }

  @Test
  void batchStoreThenRetrieveAndDelete() throws Exception {
    int dim = 24;
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(new HashMap<>());

    Map<String, float[]> embeddings = new HashMap<>();
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    int n = ThreadLocalRandom.current().nextInt(10, 50);
    for (int i = 0; i < n; i++) {
      String id = "b" + i;
      embeddings.put(id, randomUnit(dim));
      metadata.put(id, Map.of("idx", i));
    }
    store.storeEmbeddingsBatch(embeddings, metadata);

    assertEquals(n, ((Number) store.getStatistics().get("total_vectors")).intValue());
    String someId = "b" + ThreadLocalRandom.current().nextInt(n);
    assertTrue(store.exists(someId));
    assertEquals(dim, store.getEmbedding(someId).length);

    store.deleteEmbedding(someId);
    assertNull(store.getEmbedding(someId));
    assertEquals(n - 1, ((Number) store.getStatistics().get("total_vectors")).intValue());
  }

  @Test
  void deleteByFlowIdRemovesMatchingOnly() throws Exception {
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(new HashMap<>());
    store.storeEmbedding("a", randomUnit(8), Map.of("flowId", "keep"));
    store.storeEmbedding("b", randomUnit(8), Map.of("flowId", "drop"));
    store.storeEmbedding("c", randomUnit(8), Map.of("flowId", "drop"));
    store.deleteByFlowId("drop");
    assertTrue(store.exists("a"));
    assertNull(store.getEmbedding("b"));
    assertNull(store.getEmbedding("c"));
  }

  @Test
  void dimensionMismatchRejected() {
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(Map.of("vector.dimension", "16"));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.storeEmbedding("x", randomUnit(32), Map.of()));
  }

  @Test
  void discoverableAsVectorStoreSpi() {
    boolean found = false;
    for (VectorStore vs : java.util.ServiceLoader.load(VectorStore.class)) {
      if ("in-memory".equals(vs.getProviderName())) found = true;
    }
    assertTrue(found, "InMemoryVectorStore should be ServiceLoader-discoverable");
  }
}
