package org.agentic.flink.channel;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Kafka-backed {@link Channel} of JSON-encoded {@code T} values.
 *
 * <p>Use this when you want to wire any payload type through Kafka — for the framework-built-in
 * {@link KeyedContextItem} feed see {@link KafkaContextChannel}. {@code T} must be deserializable
 * by Jackson from the raw bytes; supply a custom {@link DeserializationSchema} if you need
 * non-JSON wire formats.
 *
 * <p>Records that fail to deserialize never fail the source: they are counted on the source's
 * metric group ({@link JsonSchema#DESERIALIZATION_FAILURES_METRIC}) and handed to the channel's
 * {@link DeadLetterHandler} (by default logged and dropped; {@link #withDeadLetterTopic} republishes
 * them to a Kafka topic with the error in a header).
 */
public final class KafkaChannel<T> implements Channel<T> {
  private static final long serialVersionUID = 2L;

  private final String bootstrapServers;
  private final String topic;
  private final String groupId;
  private final Class<T> type;
  private final TypeInformation<T> typeInfo;
  private final DeadLetterHandler deadLetterHandler;

  public KafkaChannel(String bootstrapServers, String topic, String groupId, Class<T> type) {
    this(bootstrapServers, topic, groupId, type, TypeInformation.of(type));
  }

  public KafkaChannel(
      String bootstrapServers,
      String topic,
      String groupId,
      Class<T> type,
      TypeInformation<T> typeInfo) {
    this(bootstrapServers, topic, groupId, type, typeInfo, DeadLetterHandler.logging());
  }

  public KafkaChannel(
      String bootstrapServers,
      String topic,
      String groupId,
      Class<T> type,
      TypeInformation<T> typeInfo,
      DeadLetterHandler deadLetterHandler) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.type = Objects.requireNonNull(type, "type");
    this.typeInfo = Objects.requireNonNull(typeInfo, "typeInfo");
    this.deadLetterHandler = Objects.requireNonNull(deadLetterHandler, "deadLetterHandler");
  }

  /** Same channel, republishing malformed records to {@code deadLetterTopic} on this cluster. */
  public KafkaChannel<T> withDeadLetterTopic(String deadLetterTopic) {
    return withDeadLetterHandler(DeadLetterHandler.kafkaTopic(bootstrapServers, deadLetterTopic));
  }

  /** Same channel with a custom {@link DeadLetterHandler}. */
  public KafkaChannel<T> withDeadLetterHandler(DeadLetterHandler handler) {
    return new KafkaChannel<>(bootstrapServers, topic, groupId, type, typeInfo, handler);
  }

  public DeadLetterHandler getDeadLetterHandler() {
    return deadLetterHandler;
  }

  @Override
  public DataStream<T> open(StreamExecutionEnvironment env) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", bootstrapServers);
    props.setProperty("group.id", groupId);

    KafkaSource<T> source =
        KafkaSource.<T>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new JsonSchema<>(type, typeInfo, topic, deadLetterHandler))
            .setProperties(props)
            .build();

    return env.fromSource(
        source, WatermarkStrategy.noWatermarks(), "kafka[" + topic + "]");
  }

  @Override
  public TypeInformation<T> elementType() {
    return typeInfo;
  }

  @Override
  public String providerName() {
    return "kafka";
  }

  public String getTopic() {
    return topic;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * JSON-from-bytes deserializer driven by a Class&lt;T&gt;. Public so other Flink jobs can reuse it.
   *
   * <p>A record Jackson cannot map to {@code T} is not an error of the source: the failure is
   * counted, the raw bytes go to the {@link DeadLetterHandler}, and nothing is emitted for it.
   */
  public static final class JsonSchema<T> implements DeserializationSchema<T> {
    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchema.class);

    public static final String DESERIALIZATION_FAILURES_METRIC = "deserialization_failures";

    private final Class<T> type;
    private final TypeInformation<T> typeInfo;
    private final String sourceTopic;
    private final DeadLetterHandler deadLetterHandler;
    private transient ObjectMapper mapper;
    private transient Counter failures;

    public JsonSchema(Class<T> type, TypeInformation<T> typeInfo) {
      this(type, typeInfo, "", DeadLetterHandler.logging());
    }

    public JsonSchema(
        Class<T> type,
        TypeInformation<T> typeInfo,
        String sourceTopic,
        DeadLetterHandler deadLetterHandler) {
      this.type = Objects.requireNonNull(type, "type");
      this.typeInfo = Objects.requireNonNull(typeInfo, "typeInfo");
      this.sourceTopic = sourceTopic == null ? "" : sourceTopic;
      this.deadLetterHandler = Objects.requireNonNull(deadLetterHandler, "deadLetterHandler");
    }

    @Override
    public void open(InitializationContext context) {
      failures = context.getMetricGroup().counter(DESERIALIZATION_FAILURES_METRIC);
    }

    /** Deserialization failures seen by this instance since {@link #open}. */
    public long failureCount() {
      return failures == null ? 0L : failures.getCount();
    }

    private ObjectMapper mapper() {
      if (mapper == null) {
        mapper =
            new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                // Be lenient about producer-side extras (e.g. a Python producer carrying fields
                // the consuming Java record doesn't model) — don't fail the whole job over a
                // stray field.
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      }
      return mapper;
    }

    /**
     * Strict variant: throws on malformed input. Flink's source calls
     * {@link #deserialize(byte[], Collector)}, which routes failures to the dead-letter handler.
     */
    @Override
    public T deserialize(byte[] message) throws IOException {
      return mapper().readValue(message, type);
    }

    @Override
    public void deserialize(byte[] message, Collector<T> out) {
      T value;
      try {
        value = deserialize(message);
      } catch (IOException | RuntimeException e) {
        if (failures != null) {
          failures.inc();
        } else {
          LOG.warn("JsonSchema used before open(); failure not counted");
        }
        deadLetterHandler.handle(sourceTopic, message, e);
        return;
      }
      if (value != null) {
        out.collect(value);
      }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
      return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
      return typeInfo;
    }
  }
}
