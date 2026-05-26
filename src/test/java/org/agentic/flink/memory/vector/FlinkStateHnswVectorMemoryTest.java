package org.agentic.flink.memory.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlinkStateHnswVectorMemoryTest {

  @Test
  @DisplayName("HnswBuildConfig validates required positive fields")
  void configValidatesPositiveFields() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HnswBuildConfig(0, 10, 10, 1.2f, VectorMemorySpec.Similarity.COSINE));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HnswBuildConfig(10, 0, 10, 1.2f, VectorMemorySpec.Similarity.COSINE));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HnswBuildConfig(10, 10, 0, 1.2f, VectorMemorySpec.Similarity.COSINE));
  }

  @Test
  @DisplayName("HnswBuildConfig.defaults() returns sane values")
  void defaultsAreSane() {
    HnswBuildConfig d = HnswBuildConfig.defaults();
    assertEquals(16, d.getM());
    assertEquals(100, d.getBeamWidth());
    assertEquals(50, d.getSearchBeam());
    assertEquals(VectorMemorySpec.Similarity.COSINE, d.getSimilarity());
  }

  @Test
  @DisplayName("FlinkStateHnswVectorMemory.spec(dim) is Java-serializable round-trip")
  void specRoundTrips() throws Exception {
    int dim = ThreadLocalRandom.current().nextInt(64, 1024);
    VectorMemorySpec original = FlinkStateHnswVectorMemory.spec(dim);
    Object restored = roundTrip(original);
    assertInstanceOf(VectorMemorySpec.class, restored);
    VectorMemorySpec r = (VectorMemorySpec) restored;
    assertEquals(dim, r.dimension());
    assertEquals(VectorMemorySpec.Similarity.COSINE, r.similarity());
    assertNotNull(r.providerName());
    assertTrue(r.providerName().contains("Hnsw"));
  }

  @Test
  @DisplayName("Spec with custom config round-trips and preserves all parameters")
  void customConfigRoundTrips() throws Exception {
    HnswBuildConfig cfg =
        new HnswBuildConfig(32, 200, 80, 1.4f, VectorMemorySpec.Similarity.DOT_PRODUCT);
    VectorMemorySpec original = FlinkStateHnswVectorMemory.spec(512, cfg);
    Object restored = roundTrip(original);
    VectorMemorySpec r = (VectorMemorySpec) restored;
    assertEquals(512, r.dimension());
    assertEquals(VectorMemorySpec.Similarity.DOT_PRODUCT, r.similarity());
    assertTrue(r.providerName().contains("M=32"));
    assertTrue(r.providerName().contains("beam=200"));
  }

  @Test
  @DisplayName("Spec rejects non-positive dimension")
  void rejectsBadDimension() {
    assertThrows(
        IllegalArgumentException.class, () -> FlinkStateHnswVectorMemory.spec(0));
    assertThrows(
        IllegalArgumentException.class, () -> FlinkStateHnswVectorMemory.spec(-128));
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
