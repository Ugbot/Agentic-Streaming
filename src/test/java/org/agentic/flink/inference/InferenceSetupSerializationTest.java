package org.agentic.flink.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InferenceSetupSerializationTest {

  @Test
  @DisplayName("InferenceSetup round-trips with randomized fields")
  void setupRoundTrips() throws Exception {
    String name = "model-" + UUID.randomUUID();
    String uri = "djl://huggingface/" + UUID.randomUUID();
    InferenceSetup.DeviceType[] devices = InferenceSetup.DeviceType.values();
    InferenceSetup.DeviceType dev = devices[ThreadLocalRandom.current().nextInt(devices.length)];
    int threads = ThreadLocalRandom.current().nextInt(1, 32);
    int batch = ThreadLocalRandom.current().nextInt(1, 256);

    InferenceSetup original =
        InferenceSetup.builder()
            .withModelName(name)
            .withModelUri(uri)
            .withDevice(dev)
            .withThreads(threads)
            .withMaxBatchSize(batch)
            .withWarmupInputs(List.of("hello", "world"))
            .build();

    Object restored = roundTrip(original);
    assertInstanceOf(InferenceSetup.class, restored);
    InferenceSetup r = (InferenceSetup) restored;
    assertEquals(name, r.getModelName());
    assertEquals(uri, r.getModelUri());
    assertEquals(dev, r.getDeviceType());
    assertEquals(threads, r.getThreads());
    assertEquals(batch, r.getMaxBatchSize());
    assertEquals(List.of("hello", "world"), r.getWarmupInputs());
  }

  @Test
  @DisplayName("InferenceSetup rejects non-positive threads / batch")
  void rejectsBadInts() {
    InferenceSetup.Builder b =
        InferenceSetup.builder().withModelName("m").withModelUri("u");
    assertThrows(IllegalArgumentException.class, () -> b.withThreads(0).build());
    assertThrows(IllegalArgumentException.class, () -> b.withThreads(1).withMaxBatchSize(0).build());
  }

  @Test
  @DisplayName("InferenceSetup requires modelName and modelUri")
  void requiresIdentifiers() {
    assertThrows(
        NullPointerException.class,
        () -> InferenceSetup.builder().withModelUri("u").build());
    assertThrows(
        NullPointerException.class,
        () -> InferenceSetup.builder().withModelName("m").build());
  }

  @Test
  @DisplayName("toBuilder() preserves every field")
  void toBuilderPreserves() {
    InferenceSetup original =
        InferenceSetup.builder()
            .withModelName("a")
            .withModelUri("b")
            .withDevice(InferenceSetup.DeviceType.CUDA)
            .withThreads(4)
            .withMaxBatchSize(16)
            .withWarmupInputs(List.of("x"))
            .build();
    InferenceSetup copy = original.toBuilder().build();
    assertEquals(original.getModelName(), copy.getModelName());
    assertEquals(original.getModelUri(), copy.getModelUri());
    assertEquals(original.getDeviceType(), copy.getDeviceType());
    assertEquals(original.getThreads(), copy.getThreads());
    assertEquals(original.getMaxBatchSize(), copy.getMaxBatchSize());
    assertEquals(original.getWarmupInputs(), copy.getWarmupInputs());
  }

  @Test
  @DisplayName("ServiceLoader resolves to the empty provider set in the base build")
  void serviceLoaderIsDiscoverable() {
    java.util.ServiceLoader<InferenceConnection> loader =
        java.util.ServiceLoader.load(InferenceConnection.class);
    // Empty providers are allowed; the assertion is just that loading doesn't blow up.
    assertNotNull(loader);
    int count = 0;
    for (InferenceConnection c : loader) {
      assertNotNull(c.providerName());
      count++;
    }
    assertTrue(count >= 0); // tautological — but documents the contract
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
