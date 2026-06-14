package org.agentic.flink.a2a.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;
import org.agentic.flink.a2a.A2ATaskState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Bridge envelopes must survive both JSON (Redis/Kafka) and Java serialization (ZeroMQ/in-JVM). */
class A2ABridgeEnvelopeTest {

  private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
      org.agentic.flink.a2a.A2AJson.mapper();
  private final Random random = new Random();

  @RepeatedTest(15)
  @DisplayName("A2ARequest round-trips through Jackson and Java serialization")
  void requestRoundTrips() throws Exception {
    List<A2APart> parts = new ArrayList<>();
    parts.add(A2APart.text("do-" + random.nextInt()));
    if (random.nextBoolean()) {
      parts.add(A2APart.data(Map.of("k", random.nextInt())));
    }
    A2ARequest req =
        new A2ARequest(
            UUID.randomUUID().toString(),
            random.nextBoolean() ? UUID.randomUUID().toString() : null,
            "agent-" + random.nextInt(5),
            A2AMessage.user(UUID.randomUUID().toString(), parts),
            random.nextBoolean(),
            "resp-channel",
            Map.of("sub", "user-" + random.nextInt()));

    A2ARequest json = MAPPER.readValue(MAPPER.writeValueAsString(req), A2ARequest.class);
    assertEquals(req, json);
    assertEquals(req, javaRoundTrip(req));
    assertEquals(req.key(), json.key());
  }

  @RepeatedTest(15)
  @DisplayName("A2AResponse round-trips through Jackson and Java serialization")
  void responseRoundTrips() throws Exception {
    List<A2AArtifact> artifacts = new ArrayList<>();
    int n = random.nextInt(3);
    for (int i = 0; i < n; i++) {
      artifacts.add(A2AArtifact.text(UUID.randomUUID().toString(), "a" + i, "v" + random.nextInt()));
    }
    A2AResponse resp =
        new A2AResponse(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            A2ATaskState.values()[random.nextInt(A2ATaskState.values().length)],
            "status",
            artifacts,
            random.nextBoolean(),
            random.nextBoolean() ? "err" : null);
    assertEquals(resp, MAPPER.readValue(MAPPER.writeValueAsString(resp), A2AResponse.class));
    assertEquals(resp, javaRoundTrip(resp));
  }

  @Test
  @DisplayName("factory helpers set the expected terminal/working states")
  void factoryHelpers() {
    assertEquals(A2ATaskState.COMPLETED, A2AResponse.completed("t", "c", List.of()).getState());
    assertTrue(A2AResponse.completed("t", "c", List.of()).isFinal());
    assertEquals(A2ATaskState.FAILED, A2AResponse.failed("t", "c", "boom").getState());
    assertEquals("boom", A2AResponse.failed("t", "c", "boom").getErrorMessage());
    assertEquals(A2ATaskState.WORKING, A2AResponse.working("t", "c", "in progress").getState());
    assertTrue(!A2AResponse.working("t", "c", "in progress").isFinal());
  }

  @Test
  @DisplayName("envelopes are usable as Flink stream element types")
  void flinkTypeInformation() {
    TypeInformation<A2ARequest> reqType = TypeExtractor.getForClass(A2ARequest.class);
    TypeInformation<A2AResponse> respType = TypeExtractor.getForClass(A2AResponse.class);
    assertTrue(reqType.getTypeClass().equals(A2ARequest.class));
    assertTrue(respType.getTypeClass().equals(A2AResponse.class));
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
