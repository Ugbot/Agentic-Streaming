package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

/** F7: malformed Kafka records go to the dead-letter handler and a counter, and never crash the source. */
class KafkaChannelDeadLetterTest {

  public record Order(String id, int qty) {}

  private static DeserializationSchema.InitializationContext context(MetricGroup group) {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return group;
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        throw new UnsupportedOperationException("not needed by JsonSchema");
      }
    };
  }

  private static Collector<Order> into(List<Order> out) {
    return new Collector<>() {
      @Override
      public void collect(Order record) {
        out.add(record);
      }

      @Override
      public void close() {}
    };
  }

  @Test
  void malformedRecordsAreDeadLetteredAndCountedWhileValidOnesFlow() throws Exception {
    DeadLetterHandler.Collecting dlq = new DeadLetterHandler.Collecting();
    String topic = "orders-" + UUID.randomUUID();
    KafkaChannel.JsonSchema<Order> schema =
        new KafkaChannel.JsonSchema<>(Order.class, TypeInformation.of(Order.class), topic, dlq);
    schema.open(context(new UnregisteredMetricsGroup()));

    int bad = ThreadLocalRandom.current().nextInt(1, 6);
    int good = ThreadLocalRandom.current().nextInt(1, 6);
    List<Order> out = new ArrayList<>();
    List<String> badPayloads = new ArrayList<>();
    for (int i = 0; i < bad; i++) {
      String payload = i % 2 == 0 ? "{not json " + UUID.randomUUID() : "{\"id\": \"x\", \"qty\": \"nope\"}";
      badPayloads.add(payload);
      schema.deserialize(payload.getBytes(StandardCharsets.UTF_8), into(out));
      String id = "o" + i;
      int qty = ThreadLocalRandom.current().nextInt(1, 100);
      if (i < good) {
        schema.deserialize(
            ("{\"id\": \"" + id + "\", \"qty\": " + qty + ", \"extra\": 1}").getBytes(StandardCharsets.UTF_8),
            into(out));
        assertEquals(new Order(id, qty), out.get(out.size() - 1));
      }
    }
    for (int i = bad; i < good; i++) {
      schema.deserialize(("{\"id\": \"o" + i + "\", \"qty\": 1}").getBytes(StandardCharsets.UTF_8), into(out));
    }

    assertEquals(good, out.size(), "valid records after malformed ones are still emitted");
    assertEquals(bad, schema.failureCount());
    assertEquals(bad, dlq.records().size());
    for (int i = 0; i < bad; i++) {
      assertEquals(badPayloads.get(i), dlq.records().get(i).getKey(), "raw payload forwarded verbatim");
      assertTrue(!dlq.records().get(i).getValue().isBlank(), "error context forwarded");
    }
  }

  @Test
  void loggingDefaultAndTopicHandlerAreConfigurableAndSerializable() throws Exception {
    KafkaChannel<Order> channel = new KafkaChannel<>("localhost:9092", "t", "g", Order.class);
    assertInstanceOf(DeadLetterHandler.Logging.class, channel.getDeadLetterHandler());

    String dlt = "dead-" + UUID.randomUUID();
    KafkaChannel<Order> withTopic = channel.withDeadLetterTopic(dlt);
    DeadLetterHandler.KafkaTopic handler =
        assertInstanceOf(DeadLetterHandler.KafkaTopic.class, withTopic.getDeadLetterHandler());
    assertEquals(dlt, handler.getDeadLetterTopic());

    DeadLetterHandler.Collecting dlq = new DeadLetterHandler.Collecting();
    KafkaChannel.JsonSchema<Order> schema =
        new KafkaChannel.JsonSchema<>(Order.class, TypeInformation.of(Order.class), "t", dlq);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(schema);
      oos.writeObject(withTopic.getDeadLetterHandler());
    }
    KafkaChannel.JsonSchema<?> restored;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      restored = (KafkaChannel.JsonSchema<?>) ois.readObject();
      assertInstanceOf(DeadLetterHandler.KafkaTopic.class, ois.readObject());
    }
    restored.open(context(new UnregisteredMetricsGroup()));
    List<Object> out = new ArrayList<>();
    @SuppressWarnings("unchecked")
    KafkaChannel.JsonSchema<Object> typed = (KafkaChannel.JsonSchema<Object>) restored;
    typed.deserialize("garbage".getBytes(StandardCharsets.UTF_8), new Collector<>() {
      @Override
      public void collect(Object record) {
        out.add(record);
      }

      @Override
      public void close() {}
    });
    assertEquals(1, restored.failureCount(), "counter re-registered after deserialization");
    assertTrue(out.isEmpty());
  }
}
