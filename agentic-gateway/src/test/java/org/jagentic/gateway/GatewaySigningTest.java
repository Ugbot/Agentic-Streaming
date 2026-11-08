package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.Principal;
import org.jagentic.core.identity.Identity;
import org.jagentic.core.identity.PrincipalSeal;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.gateway.auth.StaticTokens;

/**
 * A runtime with the gateway's public keys believes a principal only with the gateway's signature,
 * so the gateway signs every turn and signal it delivers, in process and on the Kafka input topic.
 */
class GatewaySigningTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String ALICE = "alice-token-0123456789";
  private static final StaticTokens TOKENS = StaticTokens.parse(ALICE + "=alice|acme");

  private final HttpClient http = HttpClient.newHttpClient();
  private Gateway gateway;

  @AfterEach
  void stop() {
    if (gateway != null) {
      gateway.close();
    }
  }

  private static LocalRuntime runtime(KeyPair keys) {
    Map<String, Object> spec = new LinkedHashMap<>(Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
            "paths", Map.of("main", Map.of("brain", "rule")), "verifier", Map.of("kind", "none"))));
    spec.put(Identity.SPEC_KEY, Map.of("keys", new PrincipalSeal.Verifier(List.of(keys.getPublic())).pem()));
    GraphBuilder.Built built = GraphBuilder.build(spec, null);
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new ConversationLog.InMemory());
  }

  @Test
  void aTurnThroughTheGatewayIsSignedAndTheRuntimeBelievesIt() throws Exception {
    KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    gateway = Gateway.start(GatewayOptions.loopback(TOKENS)
        .withSigner(new PrincipalSeal.Signer(keys.getPrivate(), Duration.ofMinutes(5))),
        new RuntimeBackend(runtime(keys)), Map.of());
    HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + gateway.port() + "/agent"))
        .header("Authorization", "Bearer " + ALICE)
        .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(
            Map.of("conversation_id", "c-" + UUID.randomUUID(), "text", "hello"))))
        .build(), HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r.statusCode(), r.body());
    assertEquals("completed", JSON.readTree(r.body()).path("status").asText());
  }

  @Test
  void theProofGoesOnTheInputTopicBesideThePrincipal() throws Exception {
    KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    MockProducer<String, byte[]> producer =
        new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    MockConsumer<String, byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
    TopicPartition results = new TopicPartition("turns.results", 0);
    consumer.assign(List.of(results));
    consumer.updateBeginningOffsets(new HashMap<>(Map.of(results, 0L)));
    try (KafkaBackend backend = new KafkaBackend(producer, consumer, "turns")) {
      Event e = new Event("acme/c1", "t1", "alice", "hello", Map.of(), null, Principal.of("alice", "acme"));
      backend.submit(e.withPrincipalProof(
          new PrincipalSeal.Signer(keys.getPrivate(), Duration.ofMinutes(5)).sign(e, Instant.now())));
      JsonNode sent = JSON.readTree(new String(producer.history().get(0).value(), StandardCharsets.UTF_8));
      Event decoded = new Event("acme/c1", "t1", "alice", "hello", Map.of(), null, Principal.of("alice", "acme"))
          .withPrincipalProof(sent.path("principal_proof").asText(null));
      assertNull(new PrincipalSeal.Verifier(List.of(keys.getPublic())).problem(decoded, Instant.now(), false));
    }
  }
}
