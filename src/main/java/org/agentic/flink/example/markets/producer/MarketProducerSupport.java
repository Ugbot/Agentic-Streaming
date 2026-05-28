package org.agentic.flink.example.markets.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tiny support layer for the Java market-producers: a {@link KafkaProducer} with sensible defaults
 * for local development, and a Jackson {@link ObjectMapper} configured for Java 17 records (via
 * {@link ParameterNamesModule}) so {@code mapper.writeValueAsString(record)} emits JSON whose
 * keys are exactly the record component names. The Flink job's
 * {@code KafkaChannel.JsonSchema} reads the same shape on the other end.
 */
public final class MarketProducerSupport implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MarketProducerSupport.class);

  private final ObjectMapper mapper;
  private final KafkaProducer<String, String> producer;
  private long sent;

  public MarketProducerSupport(String bootstrap, String clientId) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.ACKS_CONFIG, "1");
    p.put(ProducerConfig.LINGER_MS_CONFIG, 10);
    p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    this.producer = new KafkaProducer<>(p);
    this.mapper = new ObjectMapper().registerModule(new ParameterNamesModule());
  }

  /** Bootstrap server, falling back to the env-var or localhost:9092. */
  public static String defaultBootstrap() {
    String fromEnv = System.getenv("KAFKA_BOOTSTRAP");
    return fromEnv == null || fromEnv.isBlank() ? "localhost:9092" : fromEnv;
  }

  /** Serialize {@code value} as JSON and asynchronously send it to {@code topic}. */
  public void send(String topic, Object value) {
    try {
      String json = mapper.writeValueAsString(value);
      producer.send(new ProducerRecord<>(topic, json));
      if (++sent % 1000 == 0) {
        LOG.info("sent {} records to {}", sent, topic);
      }
    } catch (Exception e) {
      throw new RuntimeException("send to " + topic + " failed: " + e.getMessage(), e);
    }
  }

  public long totalSent() {
    return sent;
  }

  @Override
  public void close() {
    producer.flush();
    producer.close();
  }
}
