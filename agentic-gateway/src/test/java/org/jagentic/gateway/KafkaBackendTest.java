package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.Principal;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;

/** What goes on the input topic, and how an answer finds its caller. */
class KafkaBackendTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TopicPartition RESULTS = new TopicPartition("turns.results", 0);

  private static Map<String, Object> result(String key, String turn, long sequence) {
    return Map.of("conversation_id", key, "turn_id", turn, "status", "completed", "reply", "hi",
        "events", List.of(Map.of("type", "turn_received", "sequence", sequence, "payload", Map.of())));
  }

  @Test
  void theVerifiedPrincipalGoesOnTheTopicAndTheResultComesBack() throws Exception {
    MockProducer<String, byte[]> producer =
        new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    MockConsumer<String, byte[]> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());
    consumer.assign(List.of(RESULTS));
    consumer.updateBeginningOffsets(new HashMap<>(Map.of(RESULTS, 0L)));
    try (KafkaBackend backend = new KafkaBackend(producer, consumer, "turns")) {
      Event event = new Event("acme/c1", "t1", "ignored", "hello", Map.of(), null, Principal.of("alice", "acme"));
      CompletableFuture<TurnResult> answer = backend.submit(event).toCompletableFuture();
      assertFalse(answer.isDone());

      assertEquals("acme/c1", producer.history().get(0).key());
      JsonNode sent = JSON.readTree(new String(producer.history().get(0).value(), StandardCharsets.UTF_8));
      assertEquals("alice", sent.path("user_id").asText());
      assertEquals("alice", sent.path("principal").path("subject").asText());
      assertEquals("acme", sent.path("principal").path("tenant").asText());

      byte[] body = JSON.writeValueAsBytes(result("acme/c1", "t1", 0));
      consumer.schedulePollTask(() -> {
        consumer.addRecord(new ConsumerRecord<>("turns.results", 0, 0L, "acme/c1", "not json".getBytes(StandardCharsets.UTF_8)));
        consumer.addRecord(new ConsumerRecord<>("turns.results", 0, 1L, "acme/c1", body));
        consumer.addRecord(new ConsumerRecord<>("turns.results", 0, 2L, "acme/c1", body));
      });
      assertEquals(TurnStatus.COMPLETED, answer.get(5, TimeUnit.SECONDS).status);
      List<LogEvent> log = backend.conversation("acme/c1").toCompletableFuture().get();
      assertEquals(1, log.size(), "a redelivered result changes nothing");
      assertTrue(backend.conversation("beta/c1").toCompletableFuture().get().isEmpty());
    }
  }
}
