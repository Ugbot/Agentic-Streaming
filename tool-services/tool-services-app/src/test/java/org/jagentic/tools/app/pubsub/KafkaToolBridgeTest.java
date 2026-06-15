package org.jagentic.tools.app.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import jakarta.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.jagentic.core.ToolRegistry;

/**
 * Round-trip test for the Kafka pub-sub contract against a live broker. The production
 * {@link KafkaToolBridge} is build-gated OFF by default, so this test runs the same
 * {@link ToolPubSub} handler over a plain Kafka producer/consumer — proving the
 * {@code {id,tool,args}} -&gt; {@code {id,ok,result}} envelope behaves as the bridge would.
 *
 * <p><b>Skips (never fails) when no broker is reachable on localhost:9092.</b> Bring one up with
 * a redpanda/Kafka container via podman to exercise it for real.
 */
@QuarkusTest
class KafkaToolBridgeTest {

  private static final String BOOTSTRAP = "localhost:9092";
  private static final String REQUESTS = "tool-requests-test";
  private static final String REPLIES = "tool-results-test";

  @Inject
  ToolRegistry registry;

  @Inject
  ObjectMapper mapper;

  private static boolean kafkaReachable() {
    String[] hp = BOOTSTRAP.split(":");
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 500);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  void kafkaRoundTrip() throws Exception {
    assumeTrue(kafkaReachable(), "no Kafka broker on " + BOOTSTRAP + " — skipping");

    ToolPubSub pubsub = new ToolPubSub(registry, mapper);
    String id = UUID.randomUUID().toString();

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Properties reqConsumerProps = consumerProps("bridge-" + id);
    Properties replyConsumerProps = consumerProps("client-" + id);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
         KafkaConsumer<String, String> requestConsumer = new KafkaConsumer<>(reqConsumerProps);
         KafkaConsumer<String, String> replyConsumer = new KafkaConsumer<>(replyConsumerProps)) {

      requestConsumer.subscribe(List.of(REQUESTS));
      replyConsumer.subscribe(List.of(REPLIES));
      // Force assignment before producing so we don't miss the record.
      requestConsumer.poll(Duration.ofMillis(500));
      replyConsumer.poll(Duration.ofMillis(500));

      // Client publishes the request.
      String request = mapper.writeValueAsString(Map.of(
          "id", id, "tool", "util_add", "args", Map.of("a", 40, "b", 2)));
      producer.send(new ProducerRecord<>(REQUESTS, id, request));
      producer.flush();

      // Bridge role: consume the request, run the tool, publish the reply.
      String replyJson = null;
      long deadline = System.currentTimeMillis() + 15_000;
      boolean handled = false;
      while (System.currentTimeMillis() < deadline && replyJson == null) {
        if (!handled) {
          ConsumerRecords<String, String> reqs = requestConsumer.poll(Duration.ofMillis(500));
          for (ConsumerRecord<String, String> r : reqs) {
            producer.send(new ProducerRecord<>(REPLIES, id, pubsub.handle(r.value())));
            producer.flush();
            handled = true;
          }
        }
        ConsumerRecords<String, String> replies = replyConsumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : replies) {
          replyJson = r.value();
        }
      }

      assertNotNull(replyJson, "no reply observed on " + REPLIES + " within timeout");
      Map<String, Object> result =
          mapper.readValue(replyJson, new TypeReference<Map<String, Object>>() {});
      assertEquals(id, result.get("id"), "reply must correlate by id");
      assertEquals(Boolean.TRUE, result.get("ok"), "tool call should succeed: " + replyJson);
      assertEquals(42.0, ((Number) result.get("result")).doubleValue(), 1e-9);
    }
  }

  private static Properties consumerProps(String group) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    return p;
  }
}
