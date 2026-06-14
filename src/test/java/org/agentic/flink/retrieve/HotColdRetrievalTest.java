package org.agentic.flink.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.memory.vector.ScoredItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Deterministic, cluster-free tests for the live-RAG hot tier ({@link InMemoryHotVectorIndex}) and
 * the {@link TwoTierRetriever} hot+cold merge — KNN correctness, LRU eviction, shared-window
 * visibility across instances, dedup-by-id, and graceful degradation when a tier fails.
 */
class HotColdRetrievalTest {

  private static float[] randomVec(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) ThreadLocalRandom.current().nextGaussian();
    }
    return v;
  }

  @Test
  @DisplayName("hot index returns the exact nearest neighbor by cosine")
  void hotKnnFindsNearest() {
    InMemoryHotVectorIndex hot = new InMemoryHotVectorIndex("knn-" + UUID.randomUUID());
    int dim = 16;
    // A known target vector, plus noise documents.
    float[] target = randomVec(dim);
    String targetId = "target";
    hot.upsert(targetId, target, "the answer", null);
    for (int i = 0; i < 50; i++) {
      hot.upsert("noise-" + i, randomVec(dim), "noise " + i, null);
    }
    // Query with the target itself → it must be rank 1 with score ~1.0.
    List<ScoredItem> hits = hot.search(target, 5);
    assertEquals(5, hits.size());
    assertEquals(targetId, hits.get(0).getId());
    assertTrue(hits.get(0).getScore() > 0.99, "self-cosine should be ~1, was " + hits.get(0).getScore());
    // Scores must be in descending order.
    for (int i = 1; i < hits.size(); i++) {
      assertTrue(hits.get(i - 1).getScore() >= hits.get(i).getScore(), "scores not descending");
    }
  }

  @Test
  @DisplayName("hot index evicts the oldest beyond maxEntries (LRU window)")
  void hotEvictsOldest() {
    InMemoryHotVectorIndex hot = new InMemoryHotVectorIndex("evict-" + UUID.randomUUID(), 10);
    float[] v = randomVec(8);
    for (int i = 0; i < 25; i++) {
      hot.upsert("doc-" + i, v.clone(), "d" + i, null);
    }
    assertEquals(10, hot.size(), "window must be bounded to maxEntries");
    // The earliest ids must have been evicted; the most-recent retained.
    List<ScoredItem> hits = hot.search(v, 10);
    List<String> ids = new ArrayList<>();
    for (ScoredItem s : hits) {
      ids.add(s.getId());
    }
    assertTrue(ids.contains("doc-24"), "newest retained");
    assertFalse(ids.contains("doc-0"), "oldest evicted");
  }

  @Test
  @DisplayName("a second index instance with the same name sees the same shared window")
  void sharedWindowAcrossInstances() {
    String name = "shared-" + UUID.randomUUID();
    InMemoryHotVectorIndex writer = new InMemoryHotVectorIndex(name);
    float[] v = randomVec(8);
    writer.upsert("x", v, "hello", null);
    // A separate instance (as the query operator would hold) addresses the same window.
    InMemoryHotVectorIndex reader = new InMemoryHotVectorIndex(name);
    assertEquals(1, reader.size());
    assertEquals("x", reader.search(v, 1).get(0).getId());
  }

  @Test
  @DisplayName("two-tier retrieve merges hot+cold, de-dups by id keeping the higher score")
  void twoTierMergesAndDedups() {
    String name = "merge-" + UUID.randomUUID();
    InMemoryHotVectorIndex hot = new InMemoryHotVectorIndex(name);
    int dim = 12;
    float[] q = randomVec(dim);

    // Hot has a fresh doc "h1" and a shared doc "shared".
    hot.upsert("h1", q.clone(), "fresh hot doc", null); // ~1.0 vs query
    hot.upsert("shared", scale(q, 0.5f), "shared doc (hot copy)", null);

    // Cold (a lambda) returns a stale "c1" and the same "shared" id with a LOWER score.
    TwoTierRetriever.ColdSearch cold =
        (query, k) -> {
          List<ScoredItem> out = new ArrayList<>();
          out.add(new ScoredItem("c1", 0.42, item("cold doc")));
          out.add(new ScoredItem("shared", 0.10, item("shared doc (cold copy)"))); // lower than hot
          return out;
        };

    TwoTierRetriever retriever = new TwoTierRetriever(hot, cold, 5, 5);
    List<ScoredItem> merged = retriever.retrieve(q, 10);

    List<String> ids = new ArrayList<>();
    for (ScoredItem s : merged) {
      ids.add(s.getId());
    }
    // All three distinct ids present, "shared" only once.
    assertTrue(ids.contains("h1") && ids.contains("c1") && ids.contains("shared"));
    assertEquals(1, ids.stream().filter("shared"::equals).count(), "shared must be de-duplicated");
    // The retained "shared" is the hot copy (higher cosine score than the cold 0.10).
    ScoredItem shared = merged.stream().filter(s -> s.getId().equals("shared")).findFirst().orElseThrow();
    assertTrue(shared.getScore() > 0.10, "hot copy (higher score) must win the dedup");
    assertEquals("h1", merged.get(0).getId(), "the freshest exact match ranks first");
  }

  @Test
  @DisplayName("two-tier degrades gracefully when one tier throws")
  void twoTierToleratesTierFailure() {
    String name = "degrade-" + UUID.randomUUID();
    InMemoryHotVectorIndex hot = new InMemoryHotVectorIndex(name);
    float[] q = randomVec(8);
    hot.upsert("h1", q.clone(), "hot", null);

    TwoTierRetriever.ColdSearch boom =
        (query, k) -> {
          throw new RuntimeException("cold store down");
        };
    TwoTierRetriever retriever = new TwoTierRetriever(hot, boom, 5, 5);
    List<ScoredItem> merged = retriever.retrieve(q, 5);
    assertEquals(1, merged.size(), "hot results survive a cold-tier failure");
    assertEquals("h1", merged.get(0).getId());
  }

  private static ContextItem item(String text) {
    return new ContextItem(text, ContextPriority.SHOULD, MemoryType.SHORT_TERM);
  }

  private static float[] scale(float[] v, float f) {
    float[] out = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      out[i] = v[i] * f;
    }
    return out;
  }
}
