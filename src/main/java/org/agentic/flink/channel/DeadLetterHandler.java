package org.agentic.flink.channel;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives records a {@link KafkaChannel.JsonSchema} could not deserialize. Handlers are shipped
 * with the source, so they must be {@link Serializable} and create any connection lazily.
 */
public interface DeadLetterHandler extends Serializable {

  /** Header carrying the deserialization error message on dead-lettered records. */
  String ERROR_HEADER = "agentic.deserialization.error";
  /** Header carrying the source topic on dead-lettered records. */
  String SOURCE_TOPIC_HEADER = "agentic.source.topic";

  /**
   * Handles one malformed record. Implementations must not throw for ordinary delivery
   * problems; a throwing handler fails the source.
   */
  void handle(String sourceTopic, byte[] payload, Exception error);

  /** Releases resources held by the handler. */
  default void close() {}

  /** Logs the failure and drops the record. */
  static DeadLetterHandler logging() {
    return new Logging();
  }

  /** Republishes the raw record to {@code deadLetterTopic} on the same cluster. */
  static DeadLetterHandler kafkaTopic(String bootstrapServers, String deadLetterTopic) {
    return new KafkaTopic(bootstrapServers, deadLetterTopic);
  }

  final class Logging implements DeadLetterHandler {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Logging.class);

    @Override
    public void handle(String sourceTopic, byte[] payload, Exception error) {
      LOG.warn("Dropping malformed record from {} ({} bytes): {}", sourceTopic,
          payload == null ? 0 : payload.length, error.getMessage());
    }
  }

  final class KafkaTopic implements DeadLetterHandler {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopic.class);

    private final String bootstrapServers;
    private final String deadLetterTopic;
    private transient Producer<byte[], byte[]> producer;

    KafkaTopic(String bootstrapServers, String deadLetterTopic) {
      this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
      this.deadLetterTopic = Objects.requireNonNull(deadLetterTopic, "deadLetterTopic");
    }

    public String getDeadLetterTopic() {
      return deadLetterTopic;
    }

    private Producer<byte[], byte[]> producer() {
      if (producer == null) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("acks", "all");
        producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
      }
      return producer;
    }

    @Override
    public void handle(String sourceTopic, byte[] payload, Exception error) {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(deadLetterTopic, payload);
      record.headers().add(new RecordHeader(SOURCE_TOPIC_HEADER,
          sourceTopic.getBytes(StandardCharsets.UTF_8)));
      record.headers().add(new RecordHeader(ERROR_HEADER,
          String.valueOf(error.getMessage()).getBytes(StandardCharsets.UTF_8)));
      try {
        producer().send(record).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while dead-lettering record from {}", sourceTopic);
      } catch (ExecutionException e) {
        LOG.error("Failed to dead-letter record from {} to {}: {}", sourceTopic, deadLetterTopic,
            e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
      }
    }

    @Override
    public void close() {
      if (producer != null) {
        producer.close();
        producer = null;
      }
    }

    @Override
    public String toString() {
      return "KafkaTopic(" + deadLetterTopic + ")";
    }
  }

  /** Test and diagnostics handler that keeps the dead-lettered payloads in memory. */
  final class Collecting implements DeadLetterHandler {
    private static final long serialVersionUID = 1L;
    private final java.util.List<Map.Entry<String, String>> records =
        new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override
    public void handle(String sourceTopic, byte[] payload, Exception error) {
      records.add(Map.entry(new String(payload, StandardCharsets.UTF_8),
          String.valueOf(error.getMessage())));
    }

    /** Pairs of (raw payload, error message) in arrival order. */
    public java.util.List<Map.Entry<String, String>> records() {
      return records;
    }
  }
}
