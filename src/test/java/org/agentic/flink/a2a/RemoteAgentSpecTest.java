package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RemoteAgentSpecTest {

  @Test
  @DisplayName("card() spec round-trips and uses discovery")
  void cardSpecRoundTrips() throws Exception {
    String name = "planner-" + UUID.randomUUID();
    RemoteAgentSpec original =
        RemoteAgentSpec.builder()
            .withName(name)
            .withAgentCardUrl("https://peer.example.com/.well-known/agent-card.json")
            .withSkillId("route-optimizer")
            .withAuth(AuthSpec.bearer("tok-" + UUID.randomUUID()))
            .withStreaming(true)
            .withRequestTimeout(Duration.ofSeconds(90))
            .build();
    RemoteAgentSpec r = (RemoteAgentSpec) javaRoundTrip(original);
    assertEquals(name, r.name());
    assertEquals("a2a:" + name, r.toolId());
    assertTrue(r.usesCardDiscovery());
    assertTrue(r.streaming());
    assertEquals("route-optimizer", r.skillId());
    assertEquals(Duration.ofSeconds(90).toMillis(), r.requestTimeoutMs());
    assertEquals(AuthSpec.Scheme.BEARER, r.auth().getScheme());
  }

  @Test
  @DisplayName("endpoint() spec pins transport and skips discovery")
  void endpointSpec() throws Exception {
    RemoteAgentSpec original =
        RemoteAgentSpec.endpoint("svc", "https://peer/a2a/grpc", A2ATransport.GRPC);
    RemoteAgentSpec r = (RemoteAgentSpec) javaRoundTrip(original);
    assertEquals(A2ATransport.GRPC, r.transport());
    assertEquals("https://peer/a2a/grpc", r.endpointUrl());
    assertTrue(!r.usesCardDiscovery());
  }

  @Test
  @DisplayName("spec without card URL or endpoint is rejected")
  void requiresTarget() {
    assertThrows(
        IllegalArgumentException.class, () -> RemoteAgentSpec.builder().withName("x").build());
  }

  @Test
  @DisplayName("AuthSpec renders correct headers and never leaks credential in toString")
  void authHeaders() {
    assertEquals(
        Map.of("Authorization", "Bearer secret"), AuthSpec.bearer("secret").toHeaders());
    assertEquals(Map.of("X-API-Key", "k"), AuthSpec.apiKey("X-API-Key", "k").toHeaders());
    assertTrue(AuthSpec.none().toHeaders().isEmpty());
    assertTrue(!AuthSpec.bearer("topsecret").toString().contains("topsecret"));
    assertThrows(IllegalArgumentException.class, () -> AuthSpec.apiKey(null, "k"));
    assertThrows(IllegalArgumentException.class, () -> AuthSpec.bearer(null));
  }

  private static Object javaRoundTrip(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }
}
