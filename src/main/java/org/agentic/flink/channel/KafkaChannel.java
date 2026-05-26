package org.agentic.flink.channel;

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

/**
 * Generic Kafka-backed {@link Channel} of JSON-encoded {@code T} values.
 *
 * <p>Use this when you want to wire any payload type through Kafka — for the framework-built-in
 * {@link KeyedContextItem} feed see {@link KafkaContextChannel}. {@code T} must be deserializable
 * by Jackson from the raw bytes; supply a custom {@link DeserializationSchema} if you need
 * non-JSON wire formats.
 */
public final class KafkaChannel<T> implements Channel<T> {
  private static final long serialVersionUID = 1L;

  private final String bootstrapServers;
  private final String topic;
  private final String groupId;
  private final Class<T> type;
  private final TypeInformation<T> typeInfo;

  public KafkaChannel(String bootstrapServers, String topic, String groupId, Class<T> type) {
    this(bootstrapServers, topic, groupId, type, TypeInformation.of(type));
  }

  public KafkaChannel(
      String bootstrapServers,
      String topic,
      String groupId,
      Class<T> type,
      TypeInformation<T> typeInfo) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.type = Objects.requireNonNull(type, "type");
    this.typeInfo = Objects.requireNonNull(typeInfo, "typeInfo");
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
            .setValueOnlyDeserializer(new JsonSchema<>(type, typeInfo))
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

  /** JSON-from-bytes deserializer driven by a Class&lt;T&gt;. */
  static final class JsonSchema<T> implements DeserializationSchema<T> {
    private static final long serialVersionUID = 1L;

    private final Class<T> type;
    private final TypeInformation<T> typeInfo;
    private transient ObjectMapper mapper;

    JsonSchema(Class<T> type, TypeInformation<T> typeInfo) {
      this.type = type;
      this.typeInfo = typeInfo;
    }

    private ObjectMapper mapper() {
      if (mapper == null) {
        mapper = new ObjectMapper().registerModule(new ParameterNamesModule());
      }
      return mapper;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
      return mapper().readValue(message, type);
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
