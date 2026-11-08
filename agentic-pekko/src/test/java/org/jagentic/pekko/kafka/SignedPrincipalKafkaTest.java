package org.jagentic.pekko.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.Principal;
import org.jagentic.core.identity.PrincipalSeal;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.runtime.TurnWire;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * Anything that could write the input topic could claim any principal: the Kafka front door believed
 * the {@code principal} on a record. Given the gateway's public keys it believes one only with the
 * gateway's proof for that turn, answers anything else as a turn failed with class policy without
 * asking the entity, and carries on.
 */
class SignedPrincipalKafkaTest {

  private static Map<String, Object> record(String cid, String tid, String proof) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", cid);
    m.put("turn_id", tid);
    m.put("text", "what is my balance?");
    m.put("principal", Map.of("subject", "alice", "tenant", "acme"));
    if (proof != null) {
      m.put("principal_proof", proof);
    }
    return m;
  }

  private static Map<String, Object> answer(PekkoSystem sys, Map<String, Object> rec, PrincipalSeal.Verifier keys)
      throws Exception {
    ConsumerRecord<String, String> in = new ConsumerRecord<>("turns", 0, 1L, (String) rec.get("conversation_id"),
        TurnWire.write(rec));
    return TurnWire.readObject(KafkaStreamApp.answer(sys.system(), in, "out", Duration.ofSeconds(10), keys)
        .toCompletableFuture().get(30, TimeUnit.SECONDS).value());
  }

  @Test
  void aPrincipalTheGatewayDidNotSignIsRefusedAtTheDoor() throws Exception {
    KeyPair gateway = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    PrincipalSeal.Verifier keys = new PrincipalSeal.Verifier(List.of(gateway.getPublic()));
    PrincipalSeal.Signer signer = new PrincipalSeal.Signer(gateway.getPrivate(), Duration.ofMinutes(5));
    CountingGraph graph = new CountingGraph();
    try (PekkoSystem sys = new PekkoSystem(graph.deps())) {
      String cid = "acme/c-" + UUID.randomUUID();
      Map<String, Object> claimed = answer(sys, record(cid, "t1", null), keys);
      assertEquals("failed", claimed.get("status"));
      assertEquals("policy", ((Map<?, ?>) claimed.get("error")).get("class"));
      assertEquals(0, graph.brainCalls.get(), "the entity was never asked");

      Event e = new Event(cid, "t2", "alice", "what is my balance?", Map.of(), null, Principal.of("alice", "acme"));
      Map<String, Object> replayed = answer(sys, record(cid, "t3", signer.sign(e, Instant.now())), keys);
      assertEquals("failed", replayed.get("status"), "a proof for t2 is no proof for t3");
      Map<String, Object> signed = answer(sys, record(cid, "t2", signer.sign(e, Instant.now())), keys);
      assertEquals("completed", signed.get("status"));
      assertEquals(1, graph.brainCalls.get());
    }
  }

  @Test
  void aKafkaFrontDoorMustBeToldWhoMayClaimAPrincipal() {
    assertThrows(IllegalStateException.class, () -> KafkaStreamApp.identity(Map.of(), "turns"));
    assertEquals(null, KafkaStreamApp.identity(Map.of("AGENTIC_IDENTITY", "unauthenticated"), "turns"));
  }
}
