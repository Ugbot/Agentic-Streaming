package org.agentic.flink.a2a.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.a2a.bridge.InProcA2ABridge;
import org.agentic.flink.a2a.storage.InMemoryA2ATaskStore;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The served Agent Card advertises exactly the transports the gateway implements (JSON-RPC, with SSE
 * as a capability of it), and configuration for transports it does not implement is refused.
 */
class AgentCardTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static JsonNode card(Map<String, String> props) throws Exception {
    GatewayConfig cfg = new GatewayConfig(AgenticFlinkConfig.fromMap(props));
    InProcA2ABridge bridge = new InProcA2ABridge(rnd("req"), rnd("resp"));
    A2AResource r = A2AResourceLifecycleTest.resource(bridge.openGateway(), new InMemoryA2ATaskStore(), cfg);
    return JSON.readTree(r.agentCard());
  }

  /** Every transport named anywhere on the card: {@code preferredTransport} plus {@code additionalInterfaces}. */
  private static Set<String> transports(JsonNode card) {
    Set<String> t = new HashSet<>();
    t.add(card.get("preferredTransport").asText());
    for (JsonNode iface : card.get("additionalInterfaces")) {
      t.add(iface.get("transport").asText());
    }
    return t;
  }

  @Test
  @DisplayName("card lists exactly the JSONRPC transport at the public URL, with streaming as its capability")
  void cardListsOnlyImplementedTransports() throws Exception {
    String url = "https://" + rnd("agent") + ".example.com/a2a";
    JsonNode card = card(Map.of(
        ConfigKeys.A2A_GATEWAY_PUBLIC_URL, url,
        "a2a.gateway.streaming.enabled", "true"));

    assertEquals(Set.of(A2AResource.TRANSPORT), transports(card));
    assertEquals("JSONRPC", A2AResource.TRANSPORT);
    assertEquals(url, card.get("url").asText());
    assertEquals(1, card.get("additionalInterfaces").size(), "one interface: the JSON-RPC endpoint itself");
    assertEquals(url, card.get("additionalInterfaces").get(0).get("url").asText());
    assertTrue(card.get("capabilities").get("streaming").asBoolean(), "SSE is a JSON-RPC capability, not a transport");
    assertFalse(card.toString().contains("GRPC"));
    assertFalse(card.toString().contains("HTTP+JSON"));
  }

  @Test
  @DisplayName("card carries the configured skills, or one generic skill when none are configured")
  void cardSkillsComeFromConfiguration() throws Exception {
    JsonNode generic = card(Map.of());
    assertEquals(1, generic.get("skills").size());
    assertEquals("agent", generic.get("skills").get(0).get("id").asText());

    String id1 = rnd("skill");
    String id2 = rnd("skill");
    JsonNode card = card(Map.of("a2a.gateway.agent.skills",
        id1 + ":Balance:Answers balance questions, " + id2 + ":Cards:Explains card products: fees and limits"));
    assertEquals(List.of(id1, id2), List.of(
        card.get("skills").get(0).get("id").asText(), card.get("skills").get(1).get("id").asText()));
    assertEquals("Explains card products: fees and limits", card.get("skills").get(1).get("description").asText());
    assertTrue(card.get("skills").get(1).get("tags").isArray());
  }

  @Test
  @DisplayName("a skill descriptor without id:name:description is rejected")
  void malformedSkillIsRejected() {
    GatewayConfig cfg = new GatewayConfig(AgenticFlinkConfig.fromMap(
        Map.of("a2a.gateway.agent.skills", rnd("skill") + ":only-a-name")));
    IllegalStateException e = assertThrows(IllegalStateException.class, cfg::skills);
    assertTrue(e.getMessage().contains("id:name:description"), e.getMessage());
  }

  @Test
  @DisplayName("gRPC and REST endpoint configuration fails fast instead of being advertised")
  void unsupportedTransportConfigurationFailsFast() {
    for (String key : GatewayConfig.UNSUPPORTED_TRANSPORT_KEYS) {
      String value = "http://" + rnd("host") + ":" + ThreadLocalRandom.current().nextInt(1024, 65535);
      IllegalStateException e = assertThrows(IllegalStateException.class,
          () -> new GatewayConfig(AgenticFlinkConfig.fromMap(Map.of(key, value))), key);
      assertTrue(e.getMessage().contains(key), e.getMessage());
      assertTrue(e.getMessage().contains("JSON-RPC"), e.getMessage());
    }
    assertEquals(List.of("a2a.gateway.grpc.url", "a2a.gateway.rest.url"), GatewayConfig.UNSUPPORTED_TRANSPORT_KEYS);
  }
}
