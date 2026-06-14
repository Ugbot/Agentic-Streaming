package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

/** Embeddable in-process vector store — the hand-rolled HNSW index: recall vs brute force
 * and exact top-1 for a planted query. Randomized vectors, seeded for reproducibility. */
class HnswTest {

  private static float[] randVec(Random rng, int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) rng.nextGaussian();
    }
    return v;
  }

  private static List<String> bruteTopK(Map<String, float[]> vecs, float[] q, int k) {
    List<Retrieval.Scored> all = new ArrayList<>();
    for (var e : vecs.entrySet()) {
      all.add(new Retrieval.Scored(e.getKey(), Retrieval.cosine(q, e.getValue()), ""));
    }
    all.sort((a, b) -> Double.compare(b.score(), a.score()));
    List<String> out = new ArrayList<>();
    for (int i = 0; i < Math.min(k, all.size()); i++) {
      out.add(all.get(i).id());
    }
    return out;
  }

  @Test
  void recallMatchesBruteForce() {
    Random rng = new Random(7);
    int dim = 48, n = 400, k = 10;
    Map<String, float[]> vecs = new HashMap<>();
    HnswIndex index = new HnswIndex(16, 200, 64, 42L);
    for (int i = 0; i < n; i++) {
      String id = "d" + i;
      float[] v = randVec(rng, dim);
      vecs.put(id, v);
      index.add(id, v, id);
    }
    assertEquals(n, index.size());

    int hits = 0, total = 0;
    for (int q = 0; q < 30; q++) {
      float[] query = randVec(rng, dim);
      Set<String> truth = new HashSet<>(bruteTopK(vecs, query, k));
      for (Retrieval.Scored s : index.search(query, k)) {
        if (truth.contains(s.id())) {
          hits++;
        }
      }
      total += k;
    }
    double recall = (double) hits / total;
    assertTrue(recall >= 0.85, "recall@" + k + " = " + recall);
  }

  @Test
  void top1ExactForPlantedQuery() {
    Random rng = new Random(11);
    int dim = 64;
    HnswVectorStore store = new HnswVectorStore(16, 200, 50, 1L);
    float[] planted = randVec(rng, dim);
    store.upsert("target", planted, "the answer");
    for (int i = 0; i < 200; i++) {
      store.upsert("noise" + i, randVec(rng, dim), "noise");
    }
    float[] perturbed = planted.clone();
    for (int i = 0; i < dim; i++) {
      perturbed[i] += (float) (rng.nextGaussian() * 0.01);
    }
    List<Retrieval.Scored> top = store.search(perturbed, 1);
    assertEquals("target", top.get(0).id());
    assertTrue(top.get(0).score() > 0.99, "score=" + top.get(0).score());
  }

  @Test
  void agreesWithInMemoryOnSmallSet() {
    Random rng = new Random(3);
    int dim = 32;
    InMemoryVectorStore mem = new InMemoryVectorStore();
    HnswVectorStore hnsw = new HnswVectorStore(16, 200, 50, 5L);
    for (int i = 0; i < 20; i++) {
      String id = "k" + i;
      float[] v = randVec(rng, dim);
      mem.upsert(id, v, id);
      hnsw.upsert(id, v, id);
    }
    float[] q = randVec(rng, dim);
    assertEquals(mem.search(q, 1).get(0).id(), hnsw.search(q, 1).get(0).id());
  }
}
