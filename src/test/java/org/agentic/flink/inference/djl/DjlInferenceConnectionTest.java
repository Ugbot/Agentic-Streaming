package org.agentic.flink.inference.djl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.embedding.djl.DjlEmbeddingConnection;
import org.agentic.flink.inference.InferenceClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies the DJL connection's serializable surface without loading a real model. Actual
 * model loading is exercised by {@code DjlInferenceConnectionIT} under the integration
 * profile, which requires a network connection to fetch model weights.
 */
class DjlInferenceConnectionTest {

  @Test
  @DisplayName("Classification connection round-trips with model URI")
  void classificationRoundTrips() throws Exception {
    String uri = "djl://ai.djl.huggingface.pytorch/" + UUID.randomUUID();
    DjlInferenceConnection original = DjlInferenceConnection.classification(uri);
    Object restored = roundTrip(original);
    assertInstanceOf(DjlInferenceConnection.class, restored);
    DjlInferenceConnection r = (DjlInferenceConnection) restored;
    assertEquals(DjlInferenceConnection.Task.CLASSIFICATION, r.getTask());
    assertEquals(uri, r.getDefaultModelUri());
    assertEquals("PyTorch", r.getEngine());
    assertTrue(r.providerName().contains("classification"));
  }

  @Test
  @DisplayName("Embedding connection round-trips with model URI")
  void embeddingRoundTrips() throws Exception {
    String uri = "djl://huggingface/sentence-transformers/" + UUID.randomUUID();
    DjlInferenceConnection original = DjlInferenceConnection.embedding(uri);
    Object restored = roundTrip(original);
    DjlInferenceConnection r = (DjlInferenceConnection) restored;
    assertEquals(DjlInferenceConnection.Task.EMBEDDING, r.getTask());
    assertEquals(uri, r.getDefaultModelUri());
    assertTrue(r.providerName().contains("embedding"));
  }

  @Test
  @DisplayName("supports() reports the right task surfaces without touching weights")
  void supportsContract() {
    InferenceClient classifyClient =
        DjlInferenceConnection.classification("any-uri").bind(null);
    assertTrue(classifyClient.supports(InferenceClient.TaskKind.CLASSIFIER));
    assertTrue(classifyClient.supports(InferenceClient.TaskKind.SCORER));
    assertFalse(classifyClient.supports(InferenceClient.TaskKind.EMBEDDER));
    assertFalse(classifyClient.supports(InferenceClient.TaskKind.GENERIC));
    assertThrows(UnsupportedOperationException.class, classifyClient::asEmbedder);
    assertThrows(UnsupportedOperationException.class, classifyClient::asGeneric);

    InferenceClient embedClient = DjlInferenceConnection.embedding("any-uri").bind(null);
    assertFalse(embedClient.supports(InferenceClient.TaskKind.CLASSIFIER));
    assertFalse(embedClient.supports(InferenceClient.TaskKind.SCORER));
    assertTrue(embedClient.supports(InferenceClient.TaskKind.EMBEDDER));
    assertFalse(embedClient.supports(InferenceClient.TaskKind.GENERIC));
    assertThrows(UnsupportedOperationException.class, embedClient::asClassifier);
  }

  @Test
  @DisplayName("DjlEmbeddingConnection round-trips and exposes provider name")
  void djlEmbeddingConnectionRoundTrips() throws Exception {
    String uri = "djl://huggingface/sentence-transformers/all-MiniLM-L6-v2";
    DjlEmbeddingConnection original = DjlEmbeddingConnection.of(uri);
    Object restored = roundTrip(original);
    assertInstanceOf(DjlEmbeddingConnection.class, restored);
    DjlEmbeddingConnection r = (DjlEmbeddingConnection) restored;
    assertEquals(uri, r.getModelUri());
    assertEquals("djl:embedding", r.providerName());
    assertNotNull(r);
  }

  private static Object roundTrip(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }
}
