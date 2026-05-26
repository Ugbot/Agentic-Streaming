package org.agentic.flink.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.memory.vector.FlinkStateVectorMemory;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Specs must round-trip through Java serialization because Flink ships them in the job graph.
 * If a spec accidentally captures a non-serializable field, the job submits cleanly on a single
 * JVM but blows up when a TaskManager tries to deserialize the graph.
 */
class MemorySpecSerializationTest {

  @Test
  @DisplayName("FlinkStateShortTermMemory.spec() is Java-serializable round-trip")
  void shortTermSpecRoundTrips() throws Exception {
    long ttlSeconds = ThreadLocalRandom.current().nextLong(1, 86_400);
    ShortTermMemorySpec original = FlinkStateShortTermMemory.spec(Duration.ofSeconds(ttlSeconds));

    Object restored = roundTrip(original);

    assertInstanceOf(ShortTermMemorySpec.class, restored);
    ShortTermMemorySpec restoredSpec = (ShortTermMemorySpec) restored;
    assertEquals(original.ttl(), restoredSpec.ttl());
    assertEquals(original.providerName(), restoredSpec.providerName());
  }

  @Test
  @DisplayName("FlinkStateShortTermMemory.spec() with zero TTL means no TTL")
  void shortTermSpecZeroTtl() {
    assertEquals(Duration.ZERO, FlinkStateShortTermMemory.spec().ttl());
    assertEquals(Duration.ZERO, FlinkStateShortTermMemory.spec(null).ttl());
  }

  @Test
  @DisplayName("FlinkStateVectorMemory.spec() is Java-serializable round-trip")
  void vectorSpecRoundTrips() throws Exception {
    int dimension = ThreadLocalRandom.current().nextInt(8, 2048);
    int maxItems = ThreadLocalRandom.current().nextInt(100, 100_000);
    VectorMemorySpec original =
        FlinkStateVectorMemory.spec(dimension, VectorMemorySpec.Similarity.DOT_PRODUCT, maxItems);

    Object restored = roundTrip(original);

    assertInstanceOf(VectorMemorySpec.class, restored);
    VectorMemorySpec restoredSpec = (VectorMemorySpec) restored;
    assertEquals(dimension, restoredSpec.dimension());
    assertEquals(maxItems, restoredSpec.maxItems());
    assertEquals(VectorMemorySpec.Similarity.DOT_PRODUCT, restoredSpec.similarity());
    assertNotNull(restoredSpec.providerName());
    assertTrue(restoredSpec.providerName().contains("brute-force"));
  }

  @Test
  @DisplayName("Vector spec rejects non-positive dimension")
  void vectorSpecRejectsZeroDimension() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> FlinkStateVectorMemory.spec(0));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> FlinkStateVectorMemory.spec(-1));
  }

  private static Object roundTrip(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }
}
