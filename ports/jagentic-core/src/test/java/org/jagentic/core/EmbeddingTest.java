package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.embedding.Embedders;
import org.jagentic.core.embedding.HashingEmbedder;

/** Embedder SPI tests — hashing default offline; Ollama live (opt-in, skips). */
class EmbeddingTest {

  @Test
  void hashingEmbedderDefaultDeterministic() {
    Embedder e = Embedders.make(Map.of("kind", "hashing", "dim", 256));
    assertTrue(e instanceof HashingEmbedder);
    assertEquals(256, e.dim());
    float[] v1 = e.embed("crypto cash-back");
    float[] v2 = e.embed("crypto cash-back");
    assertEquals(256, v1.length);
    assertTrue(Retrieval.cosine(v1, v2) > 0.999);
    double near = Retrieval.cosine(e.embed("crypto cash-back redemption"), v1);
    double far = Retrieval.cosine(e.embed("the weather is sunny today"), v1);
    assertTrue(near > far, "related text should be nearer");
  }

  @Test
  void ollamaEmbedderIfAvailable() {
    Embedder e;
    float[] v;
    try {
      e = Embedders.make(Map.of("kind", "ollama", "model", "nomic-embed-text"));
      v = e.embed("hello world");
    } catch (Throwable t) {
      Assumptions.abort("Ollama not reachable / model not pulled: " + t.getMessage());
      return;
    }
    assertTrue(v.length > 0);
    assertEquals(v.length, e.dim());
    assertTrue(Retrieval.cosine(v, e.embed("hello world")) > 0.99);
  }
}
