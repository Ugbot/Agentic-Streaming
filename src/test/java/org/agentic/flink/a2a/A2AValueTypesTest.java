package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Round-trips the A2A value model through Jackson and Java serialization with randomized data. */
class A2AValueTypesTest {

  private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER = A2AJson.mapper();
  private final Random random = new Random();

  @RepeatedTest(20)
  @DisplayName("A2ATask round-trips through Jackson with random parts/artifacts")
  void taskJacksonRoundTrip() throws Exception {
    A2ATask original = randomTask();
    String json = MAPPER.writeValueAsString(original);
    A2ATask restored = MAPPER.readValue(json, A2ATask.class);
    assertEquals(original, restored);
  }

  @RepeatedTest(20)
  @DisplayName("A2ATask round-trips through Java serialization")
  void taskJavaRoundTrip() throws Exception {
    A2ATask original = randomTask();
    assertEquals(original, javaRoundTrip(original));
  }

  @Test
  @DisplayName("A2APart factory variants preserve kind + payload")
  void partVariants() {
    A2APart text = A2APart.text("hello");
    assertEquals(A2APart.Kind.TEXT, text.getKind());
    assertEquals("hello", text.getText());

    A2APart data = A2APart.data(Map.of("k", 1));
    assertEquals(A2APart.Kind.DATA, data.getKind());
    assertEquals(1, data.getData().get("k"));

    A2APart fileUri = A2APart.fileUri("https://x/y.png", "image/png", "y.png");
    assertEquals(A2APart.Kind.FILE, fileUri.getKind());
    assertEquals("https://x/y.png", fileUri.getFileUri());
    assertEquals("image/png", fileUri.getMimeType());
  }

  @Test
  @DisplayName("A2ATaskState wire mapping is bidirectional and tolerant")
  void taskStateWire() {
    for (A2ATaskState state : A2ATaskState.values()) {
      assertEquals(state, A2ATaskState.fromWire(state.wire()));
    }
    assertEquals(A2ATaskState.INPUT_REQUIRED, A2ATaskState.fromWire("input-required"));
    assertEquals(A2ATaskState.INPUT_REQUIRED, A2ATaskState.fromWire("TASK_STATE_INPUT_REQUIRED"));
    assertEquals(A2ATaskState.AUTH_REQUIRED, A2ATaskState.fromWire("AUTH-REQUIRED"));
    assertEquals(A2ATaskState.UNKNOWN, A2ATaskState.fromWire("not-a-state"));
    assertEquals(A2ATaskState.UNKNOWN, A2ATaskState.fromWire(null));

    assertTrue(A2ATaskState.COMPLETED.isTerminal());
    assertTrue(A2ATaskState.COMPLETED.isFinal());
    assertFalse(A2ATaskState.INPUT_REQUIRED.isTerminal());
    assertTrue(A2ATaskState.INPUT_REQUIRED.isInterrupted());
    assertTrue(A2ATaskState.INPUT_REQUIRED.isFinal());
    assertFalse(A2ATaskState.WORKING.isFinal());
  }

  @Test
  @DisplayName("AgentCard endpointFor resolves preferred + additional interfaces")
  void agentCardEndpointResolution() {
    A2AAgentCard card =
        A2AAgentCard.builder()
            .name("peer")
            .url("https://peer/a2a")
            .preferredTransport(A2ATransport.JSONRPC)
            .addInterface("https://peer/grpc", A2ATransport.GRPC)
            .addInterface("https://peer/rest", A2ATransport.HTTP_JSON)
            .build();
    assertEquals("https://peer/a2a", card.endpointFor(A2ATransport.JSONRPC).orElseThrow());
    assertEquals("https://peer/grpc", card.endpointFor(A2ATransport.GRPC).orElseThrow());
    assertEquals("https://peer/rest", card.endpointFor(A2ATransport.HTTP_JSON).orElseThrow());
  }

  @Test
  @DisplayName("A2AAgentCard round-trips through Jackson with skills + interfaces")
  void agentCardJacksonRoundTrip() throws Exception {
    A2AAgentCard card =
        A2AAgentCard.builder()
            .name("planner-" + UUID.randomUUID())
            .description("plans things")
            .url("https://peer/a2a")
            .version("1.2.3")
            .capabilities(true, true, false)
            .addInterface("https://peer/grpc", A2ATransport.GRPC)
            .addSkill(
                new A2AAgentSkill(
                    "route", "Router", "routes", List.of("maps"), List.of("ex"), null, null))
            .build();
    String json = MAPPER.writeValueAsString(card);
    A2AAgentCard restored = MAPPER.readValue(json, A2AAgentCard.class);
    assertEquals(card.getName(), restored.getName());
    assertEquals(card.getVersion(), restored.getVersion());
    assertEquals(1, restored.getSkills().size());
    assertEquals("route", restored.getSkills().get(0).getId());
    assertTrue(restored.getCapabilities().isStreaming());
    assertEquals(
        "https://peer/grpc", restored.endpointFor(A2ATransport.GRPC).orElseThrow());
  }

  private A2ATask randomTask() {
    String id = UUID.randomUUID().toString();
    String ctx = UUID.randomUUID().toString();
    long now = Math.abs(random.nextLong() % 1_000_000_000L);

    List<A2APart> parts = new ArrayList<>();
    parts.add(A2APart.text("msg-" + random.nextInt()));
    if (random.nextBoolean()) {
      parts.add(A2APart.data(Map.of("n", random.nextInt(1000), "flag", random.nextBoolean())));
    }
    A2AMessage msg = A2AMessage.user(UUID.randomUUID().toString(), parts);

    A2ATaskState[] states = A2ATaskState.values();
    A2ATaskState state = states[random.nextInt(states.length)];

    A2ATask task = A2ATask.submitted(id, ctx, msg, now).withState(state, "status-" + random.nextInt(), now);
    int artifactCount = random.nextInt(3);
    for (int i = 0; i < artifactCount; i++) {
      task =
          task.withArtifact(
              A2AArtifact.text(UUID.randomUUID().toString(), "a" + i, "content-" + random.nextInt()),
              now + i);
    }
    return task;
  }

  @SuppressWarnings("unchecked")
  private static <T> T javaRoundTrip(T obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return (T) ois.readObject();
    }
  }
}
