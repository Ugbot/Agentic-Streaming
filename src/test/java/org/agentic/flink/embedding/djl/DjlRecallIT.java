package org.agentic.flink.embedding.djl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.memory.vector.ScoredItem;
import org.agentic.flink.retrieve.InMemoryHotVectorIndex;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Live DJL embedding test + micro-benchmark. Loads a real sentence-transformer
 * ({@code all-MiniLM-L6-v2}) through DJL/PyTorch, embeds a small corpus into the RAG hot index, and
 * asserts <b>semantic recall</b> — a paraphrased query retrieves the on-topic passage as top-1.
 *
 * <p>Tagged {@code djl}; runs only under the {@code djl-native} profile (which bundles the CPU
 * PyTorch native and selects this group):
 *
 * <pre>
 *   mvn test -P djl-native -Dtest=DjlRecallIT
 * </pre>
 *
 * First run downloads the model (~90 MB) + the PyTorch native; subsequent runs use the DJL cache.
 */
@Tag("djl")
class DjlRecallIT {

  private static final String MODEL_URI =
      "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2";
  private static final int DIM = 384;

  private static EmbeddingClient client;
  private static EmbeddingSetup setup;

  @BeforeAll
  static void load() throws Exception {
    client = DjlEmbeddingConnection.of(MODEL_URI).bind(null);
    setup = EmbeddingSetup.of("all-MiniLM-L6-v2", DIM, true); // normalized for cosine
  }

  @AfterAll
  static void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @Test
  @DisplayName("MiniLM embeddings give correct semantic recall through the hot index")
  void semanticRecall() {
    InMemoryHotVectorIndex index = new InMemoryHotVectorIndex("djl-" + UUID.randomUUID());

    record Doc(String id, String text) {}
    List<Doc> corpus =
        List.of(
            new Doc("france", "The capital of France is Paris, a city on the Seine."),
            new Doc("biology", "Photosynthesis converts sunlight into chemical energy in plants."),
            new Doc("markets", "The stock market fell sharply on Monday amid rate fears."),
            new Doc("cooking", "To make a roux, whisk equal parts butter and flour over heat."));

    for (Doc d : corpus) {
      float[] v = client.embed(d.text(), setup);
      assertEquals(DIM, v.length, "MiniLM produces 384-dim vectors");
      index.upsert(d.id(), v, d.text(), null);
    }

    // A paraphrase that shares almost no words with the target passage.
    float[] q = client.embed("Which European city is France's capital?", setup);
    List<ScoredItem> hits = index.search(q, 4);
    assertEquals("france", hits.get(0).getId(), "the Paris passage must rank first by meaning");
    assertTrue(hits.get(0).getScore() > 0.4, "top cosine should be clearly positive: " + hits.get(0).getScore());

    // A different topic resolves to its own passage.
    float[] q2 = client.embed("How do plants turn light into energy?", setup);
    assertEquals("biology", index.search(q2, 4).get(0).getId());
  }

  @Test
  @DisplayName("micro-benchmark: report mean embed latency (no hard threshold)")
  void benchmark() {
    String[] samples = {
      "agentic flink streaming retrieval",
      "the quick brown fox jumps over the lazy dog",
      "customer wants to dispute a credit card charge",
      "what is the weather forecast for tomorrow"
    };
    // Warm up (model + JIT).
    for (String s : samples) {
      client.embed(s, setup);
    }
    int iters = 40;
    long start = System.nanoTime();
    for (int i = 0; i < iters; i++) {
      client.embed(samples[i % samples.length], setup);
    }
    double meanMs = (System.nanoTime() - start) / 1_000_000.0 / iters;
    System.out.printf("DJL all-MiniLM-L6-v2 mean embed latency: %.2f ms/doc over %d iters%n", meanMs, iters);
    assertTrue(meanMs > 0, "benchmark ran");
  }
}
