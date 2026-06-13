package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Verifies {@link A2AToolExecutor} maps tool params to A2A messages and artifacts back to results. */
class A2AToolExecutorTest {

  private final Random random = new Random();

  /** Serializable factory (lambda) producing a fake that completes after one poll. */
  private static final A2AClientFactory FAKE = spec -> new FakeA2AClient(spec, 1, false);

  private RemoteAgentSpec spec(String name) {
    return RemoteAgentSpec.builder()
        .withName(name)
        .withEndpointUrl("https://peer/a2a")
        .withPollInterval(Duration.ofMillis(1))
        .withRequestTimeout(Duration.ofSeconds(5))
        .build();
  }

  @RepeatedTest(10)
  @DisplayName("execute() with text input returns the peer's echoed artifact text")
  void executeTextInput() throws Exception {
    A2AToolExecutor tool = new A2AToolExecutor(spec("peer-" + UUID.randomUUID()), FAKE);
    String prompt = "summarize-" + random.nextInt();
    Object result = tool.execute(Map.of("input", prompt)).get();
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) result;
    assertEquals("completed", map.get("state"));
    assertEquals(Boolean.TRUE, map.get("success"));
    assertTrue(((String) map.get("text")).contains(prompt));
    assertNotNull(map.get("taskId"));
    assertEquals(1, ((java.util.List<?>) map.get("artifacts")).size());
  }

  @Test
  @DisplayName("execute() forwards 'data' map and whole-payload fallback")
  void executeDataAndFallback() throws Exception {
    A2AToolExecutor tool = new A2AToolExecutor(spec("p"), FAKE);
    // data map present -> succeeds
    Object r1 = tool.execute(Map.of("data", Map.of("x", 1, "y", 2))).get();
    assertEquals("completed", ((Map<?, ?>) r1).get("state"));
    // no recognized field -> whole payload forwarded, still succeeds
    Object r2 = tool.execute(Map.of("foo", "bar", "n", 3)).get();
    assertEquals("completed", ((Map<?, ?>) r2).get("state"));
  }

  @Test
  @DisplayName("execute() surfaces a failed terminal state as success=false")
  void executeFailure() throws Exception {
    A2AClientFactory failing = spec -> new FakeA2AClient(spec, 1, true);
    A2AToolExecutor tool = new A2AToolExecutor(spec("p"), failing);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) tool.execute(Map.of("input", "x")).get();
    assertEquals("failed", map.get("state"));
    assertEquals(Boolean.FALSE, map.get("success"));
  }

  @Test
  @DisplayName("toolId is a2a:<name> and description is non-empty")
  void identity() {
    A2AToolExecutor tool = new A2AToolExecutor(spec("planner"), FAKE);
    assertEquals("a2a:planner", tool.getToolId());
    assertFalse(tool.getDescription().isEmpty());
  }

  @Test
  @DisplayName("executor is Java-serializable (ships in the Flink job graph)")
  void serializable() throws Exception {
    A2AToolExecutor tool = new A2AToolExecutor(spec("planner"), FAKE);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(tool);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      A2AToolExecutor restored = (A2AToolExecutor) ois.readObject();
      assertEquals("a2a:planner", restored.getToolId());
      // The restored executor still works (client/pool rebuilt lazily, transient).
      assertEquals(
          "completed",
          ((Map<?, ?>) restored.execute(Map.of("input", "hi")).get()).get("state"));
    }
  }
}
