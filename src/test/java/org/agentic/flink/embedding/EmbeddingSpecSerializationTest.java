package org.agentic.flink.embedding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EmbeddingSpecSerializationTest {

  @Test
  @DisplayName("EmbeddingSetup round-trips with randomized fields")
  void embeddingSetupRoundTrips() throws Exception {
    String model = "embed-" + UUID.randomUUID();
    int dim = ThreadLocalRandom.current().nextInt(64, 4096);
    boolean normalize = ThreadLocalRandom.current().nextBoolean();

    EmbeddingSetup original = EmbeddingSetup.of(model, dim, normalize);
    Object restored = roundTrip(original);

    assertInstanceOf(EmbeddingSetup.class, restored);
    EmbeddingSetup r = (EmbeddingSetup) restored;
    assertEquals(model, r.getModelName());
    assertEquals(dim, r.getDimension());
    assertEquals(normalize, r.shouldNormalize());
  }

  @Test
  @DisplayName("EmbeddingSetup rejects non-positive dimension")
  void rejectsBadDimension() {
    assertThrows(IllegalArgumentException.class, () -> EmbeddingSetup.of("foo", 0));
    assertThrows(IllegalArgumentException.class, () -> EmbeddingSetup.of("foo", -1));
  }

  @Test
  @DisplayName("OllamaEmbeddingConnection round-trips")
  void ollamaConnectionRoundTrips() throws Exception {
    String baseUrl = "http://" + UUID.randomUUID() + ":11434";
    OllamaEmbeddingConnection original =
        new OllamaEmbeddingConnection(baseUrl, Duration.ofSeconds(45));

    Object restored = roundTrip(original);
    assertInstanceOf(OllamaEmbeddingConnection.class, restored);
    OllamaEmbeddingConnection r = (OllamaEmbeddingConnection) restored;
    assertEquals(baseUrl, r.getBaseUrl());
    assertEquals(Duration.ofSeconds(45), r.getTimeout());
    assertEquals("ollama", r.providerName());
  }

  @Test
  @DisplayName("Default EmbeddingConnection is discovered via ServiceLoader")
  void serviceLoaderFindsDefault() {
    java.util.ServiceLoader<EmbeddingConnection> loader =
        java.util.ServiceLoader.load(EmbeddingConnection.class);
    EmbeddingConnection first = null;
    for (EmbeddingConnection c : loader) {
      first = c;
      break;
    }
    assertNotNull(first);
    assertInstanceOf(OllamaEmbeddingConnection.class, first);
    assertTrue(first.providerName().contains("ollama"));
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
