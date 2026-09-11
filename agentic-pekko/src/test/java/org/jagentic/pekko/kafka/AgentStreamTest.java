package org.jagentic.pekko.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import org.jagentic.core.Event;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.runtime.TurnWire;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * The Kafka front door. Broker-free: the ask-the-entity flow keeps stream order and dedupes by
 * {@code turn_id}; the record mapping emits the normalized result document, keyed by conversation,
 * and turns a malformed record into an error document instead of dropping or inventing a turn.
 * Live round trip against a broker runs when {@code AGENTIC_PEKKO_INTEGRATION=true} and fails —
 * never skips — if {@code AGENTIC_KAFKA_BOOTSTRAP} is missing or unreachable.
 */
class AgentStreamTest {

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  void streamsEventsThroughEntitiesPreservingOrderAndDedupingTurnIds() throws Exception {
    CountingGraph graph = new CountingGraph();
    try (PekkoSystem sys = new PekkoSystem(graph.deps())) {
      String c1 = rnd("c");
      String c2 = rnd("c");
      String t1 = rnd("t");
      List<Event> events = List.of(
          Event.turn(c1, t1, "u", "what is my balance?"),
          Event.turn(c2, rnd("t"), "u", "hello there"),
          Event.turn(c1, rnd("t"), "u", "thanks"),
          Event.turn(c1, t1, "u", "what is my balance?"));

      List<TurnReply> replies = Source.from(events)
          .via(AgentStream.flow(sys.system(), 4, Duration.ofSeconds(10)))
          .runWith(Sink.seq(), sys.system())
          .toCompletableFuture().get(30, TimeUnit.SECONDS);

      assertEquals(4, replies.size());
      assertEquals(t1, replies.get(0).turnId());
      assertEquals("payments", replies.get(0).path());
      assertEquals("general", replies.get(1).path());
      assertEquals(2L, replies.get(2).state().get("turn_count"));
      assertEquals(TurnStatus.DUPLICATE, replies.get(3).status());
      assertEquals(replies.get(0).reply(), replies.get(3).reply());
      assertEquals(3, graph.brainCalls.get());
    }
  }

  @Test
  void recordMappingEmitsNormalizedResultsAndFlagsMalformedRecords() throws Exception {
    CountingGraph graph = new CountingGraph();
    try (PekkoSystem sys = new PekkoSystem(graph.deps())) {
      String cid = rnd("c");
      String t1 = rnd("t");
      String out = rnd("out");
      ConsumerRecord<String, String> in = new ConsumerRecord<>("turns", 0, 7L, cid,
          TurnWire.write(Map.of("conversation_id", cid, "turn_id", t1, "user_id", "u", "text", "what is my balance?")));

      ProducerRecord<String, String> result = KafkaStreamApp.answer(sys.system(), in, out, Duration.ofSeconds(10))
          .toCompletableFuture().get(30, TimeUnit.SECONDS);
      assertEquals(out, result.topic());
      assertEquals(cid, result.key(), "results are keyed by conversation");
      Map<String, Object> doc = TurnWire.readObject(result.value());
      assertEquals(t1, doc.get("turn_id"));
      assertEquals("completed", doc.get("status"));
      assertEquals("payments", doc.get("path"));

      Map<String, Object> dup = TurnWire.readObject(KafkaStreamApp.answer(sys.system(), in, out, Duration.ofSeconds(10))
          .toCompletableFuture().get(30, TimeUnit.SECONDS).value());
      assertEquals("duplicate", dup.get("status"));
      assertEquals(1, graph.brainCalls.get());

      ConsumerRecord<String, String> bad = new ConsumerRecord<>("turns", 2, 41L, "k", "{\"text\":\"no id\"}");
      ProducerRecord<String, String> err = KafkaStreamApp.answer(sys.system(), bad, out, Duration.ofSeconds(10))
          .toCompletableFuture().get(30, TimeUnit.SECONDS);
      Map<String, Object> errDoc = TurnWire.readObject(err.value());
      assertEquals("k", err.key());
      assertEquals(2, errDoc.get("partition"));
      assertEquals(41, errDoc.get("offset"));
      @SuppressWarnings("unchecked")
      Map<String, Object> error = (Map<String, Object>) errDoc.get("error");
      assertEquals("malformed_record", error.get("error_class"));
      assertTrue(String.valueOf(error.get("message")).contains("conversation_id"));
      assertEquals(1, graph.brainCalls.get());
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "AGENTIC_PEKKO_INTEGRATION", matches = "true")
  void liveKafkaRoundTripProducesNormalizedResults() throws Exception {
    String bootstrap = System.getenv("AGENTIC_KAFKA_BOOTSTRAP");
    if (bootstrap == null || bootstrap.isBlank()) {
      fail("AGENTIC_PEKKO_INTEGRATION=true but AGENTIC_KAFKA_BOOTSTRAP is not set: a Kafka broker is required");
    }
    String in = rnd("turns");
    String out = rnd("results");
    String cid = rnd("c");
    String t1 = rnd("t");
    CountingGraph graph = new CountingGraph();
    try (PekkoSystem sys = new PekkoSystem(graph.deps())) {
      Consumer.DrainingControl<?> control = KafkaStreamApp.run(sys.system(), bootstrap, in, out, rnd("group"),
          ThreadLocalRandom.current().nextInt(1, 5), Duration.ofSeconds(20));
      Properties pp = new Properties();
      pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      try (KafkaProducer<String, String> producer = new KafkaProducer<>(pp, new StringSerializer(), new StringSerializer())) {
        String body = TurnWire.write(Map.of("conversation_id", cid, "turn_id", t1, "text", "what is my balance?"));
        producer.send(new ProducerRecord<>(in, cid, body)).get(30, TimeUnit.SECONDS);
        producer.send(new ProducerRecord<>(in, cid, body)).get(30, TimeUnit.SECONDS);
      }
      Properties cp = new Properties();
      cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      cp.put(ConsumerConfig.GROUP_ID_CONFIG, rnd("verify"));
      cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      List<String> statuses = new ArrayList<>();
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp, new StringDeserializer(), new StringDeserializer())) {
        consumer.subscribe(List.of(out));
        long deadline = System.currentTimeMillis() + 60_000L;
        while (statuses.size() < 2 && System.currentTimeMillis() < deadline) {
          ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(1));
          polled.forEach(r -> {
            assertEquals(cid, r.key());
            statuses.add(String.valueOf(TurnWire.readObject(r.value()).get("status")));
          });
        }
      }
      control.drainAndShutdown(sys.system().executionContext()).toCompletableFuture().get(30, TimeUnit.SECONDS);
      assertEquals(List.of("completed", "duplicate"), statuses);
      assertEquals(1, graph.brainCalls.get());
    }
  }
}
