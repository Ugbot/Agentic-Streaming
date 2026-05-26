package org.agentic.flink.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.embedding.EmbeddingSetup;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Sanity test of the stub used by guardrail / tool-adapter tests in later phases. */
class EchoInferenceConnectionTest {

  @Test
  @DisplayName("Echo exposes every task surface with deterministic outputs")
  void allTaskSurfacesWork() throws Exception {
    EchoInferenceConnection conn =
        new EchoInferenceConnection("unsafe", 0.97, Map.of("safe", 0.03, "unsafe", 0.97), 0.42, 16);
    InferenceClient client = conn.bind(null);

    assertTrue(client.supports(InferenceClient.TaskKind.CLASSIFIER));
    assertTrue(client.supports(InferenceClient.TaskKind.SCORER));
    assertTrue(client.supports(InferenceClient.TaskKind.EMBEDDER));
    assertTrue(client.supports(InferenceClient.TaskKind.GENERIC));

    InferenceSetup setup =
        InferenceSetup.builder().withModelName("m").withModelUri("u").build();

    ClassificationResult cls = client.asClassifier().classify("anything", setup);
    assertEquals("unsafe", cls.getLabel());
    assertEquals(0.97, cls.getScore(), 1e-9);
    assertEquals(0.97, cls.getProbabilities().get("unsafe"), 1e-9);

    double score = client.asScorer().score("anything", setup);
    assertEquals(0.42, score, 1e-9);

    float[] emb = client.asEmbedder().embed("xyz", EmbeddingSetup.of("echo", 16));
    assertEquals(16, emb.length);

    Map<String, Object> generic =
        client.asGeneric().infer(Map.of("k", "v"), setup);
    assertEquals(Boolean.TRUE, generic.get("_echo"));
    assertEquals("v", generic.get("k"));

    assertEquals("echo", client.providerName());
  }

  @Test
  @DisplayName("Batch classify uses the default loop and preserves order")
  void batchClassifyDefaults() throws Exception {
    EchoInferenceConnection conn = EchoInferenceConnection.withLabel("ok");
    InferenceClient client = conn.bind(null);
    InferenceSetup setup =
        InferenceSetup.builder().withModelName("m").withModelUri("u").build();
    List<ClassificationResult> results =
        client.asClassifier().classifyBatch(List.of("a", "b", "c"), setup);
    assertEquals(3, results.size());
    for (ClassificationResult r : results) {
      assertEquals("ok", r.getLabel());
    }
    assertNotNull(client);
  }
}
