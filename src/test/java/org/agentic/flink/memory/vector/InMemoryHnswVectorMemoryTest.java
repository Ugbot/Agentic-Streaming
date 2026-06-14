package org.agentic.flink.memory.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Exercises the Flink-free HNSW graph via {@link InMemoryHnswVectorMemory} on randomized vectors. */
class InMemoryHnswVectorMemoryTest {

  private static ContextItem item(String s) {
    return new ContextItem(s, ContextPriority.SHOULD, MemoryType.LONG_TERM);
  }

  @Test
  @DisplayName("exact query returns its own vector top-1 with ~1.0 cosine")
  void exactMatchTop1() {
    InMemoryHnswVectorMemory mem = InMemoryHnswVectorMemory.of(8);
    Random r = new Random(7);
    float[] target = null;
    String targetId = "v50";
    for (int i = 0; i < 100; i++) {
      float[] v = randomVec(8, r);
      if (i == 50) {
        target = v;
      }
      mem.put(new VectorEntry("v" + i, v, item("doc " + i)));
    }
    List<ScoredItem> hits = mem.search(target, 1);
    assertEquals(1, hits.size());
    assertEquals(targetId, hits.get(0).getId());
    assertTrue(hits.get(0).getScore() > 0.99, "self-similarity should be ~1.0");
    assertEquals(100, mem.size());
  }

  @Test
  @DisplayName("recall@1 vs brute-force cosine is high on random vectors")
  void recallVsBruteForce() {
    int dim = 64;
    int n = 300;
    int queries = 60;
    Random r = new Random(42);

    InMemoryHnswVectorMemory mem = InMemoryHnswVectorMemory.of(dim);
    float[][] all = new float[n][];
    for (int i = 0; i < n; i++) {
      all[i] = randomVec(dim, r);
      mem.put(new VectorEntry("id" + i, all[i], item("d" + i)));
    }

    int correct = 0;
    for (int q = 0; q < queries; q++) {
      float[] query = randomVec(dim, r);
      // brute-force nearest by cosine
      int best = -1;
      double bestScore = Double.NEGATIVE_INFINITY;
      for (int i = 0; i < n; i++) {
        double s = cosine(query, all[i]);
        if (s > bestScore) {
          bestScore = s;
          best = i;
        }
      }
      List<ScoredItem> hits = mem.search(query, 1);
      if (!hits.isEmpty() && hits.get(0).getId().equals("id" + best)) {
        correct++;
      }
    }
    double recall = correct / (double) queries;
    assertTrue(recall >= 0.8, "recall@1 too low: " + recall);
  }

  @Test
  @DisplayName("remove tombstones a vector; clear empties the index")
  void removeAndClear() {
    InMemoryHnswVectorMemory mem = InMemoryHnswVectorMemory.of(4);
    Random r = new Random(1);
    for (int i = 0; i < 10; i++) {
      mem.put(new VectorEntry("k" + i, randomVec(4, r), item("d" + i)));
    }
    assertEquals(10, mem.size());
    mem.remove("k3");
    assertEquals(9, mem.size());
    assertTrue(mem.search(randomVec(4, r), 5).stream().noneMatch(s -> s.getId().equals("k3")));
    mem.clear();
    assertEquals(0, mem.size());
    assertTrue(mem.search(randomVec(4, r), 3).isEmpty());
  }

  private static float[] randomVec(int dim, Random r) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) r.nextGaussian();
    }
    return v;
  }

  private static double cosine(float[] a, float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return (na == 0 || nb == 0) ? 0 : dot / (Math.sqrt(na) * Math.sqrt(nb));
  }
}
