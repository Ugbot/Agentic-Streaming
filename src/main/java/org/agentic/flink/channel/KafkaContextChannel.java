package org.agentic.flink.channel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.agentic.flink.context.core.ContextItem;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Channel that consumes a Kafka topic of JSON-encoded {@link KeyedContextItem} records.
 *
 * <p>Wire format is a JSON object with two top-level fields, {@code flowId} (string) and {@code
 * item} (a {@link ContextItem} object). Using JSON rather than a binary schema keeps the channel
 * usable from any language that can write to Kafka.
 *
 * <p>Requires {@code flink-connector-kafka} on the runtime classpath. The dependency is marked
 * {@code optional} in the project's pom so users not running a Kafka channel don't pull it
 * transitively.
 *
 * <p>Migrated from the prior {@code KafkaMemoryFeed}; the wire format is unchanged.
 */
public final class KafkaContextChannel implements Channel<KeyedContextItem> {
  private static final long serialVersionUID = 1L;

  private final String bootstrapServers;
  private final String topic;
  private final String groupId;

  public KafkaContextChannel(String bootstrapServers, String topic, String groupId) {
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
    this.groupId = groupId;
  }

  @Override
  public DataStream<KeyedContextItem> open(StreamExecutionEnvironment env) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", bootstrapServers);
    props.setProperty("group.id", groupId);

    KafkaSource<KeyedContextItem> source =
        KafkaSource.<KeyedContextItem>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new JsonKeyedContextItemSchema())
            .setProperties(props)
            .build();

    return env.fromSource(
        source, WatermarkStrategy.noWatermarks(), "kafka-context-channel[" + topic + "]");
  }

  @Override
  public TypeInformation<KeyedContextItem> elementType() {
    return TypeInformation.of(new TypeHint<KeyedContextItem>() {});
  }

  @Override
  public String providerName() {
    return "kafka-context";
  }

  /** Deserializes the JSON byte payload into a {@link KeyedContextItem}. */
  static final class JsonKeyedContextItemSchema implements DeserializationSchema<KeyedContextItem> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    private ObjectMapper mapper() {
      if (mapper == null) {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new ParameterNamesModule());
        mapper.setVisibility(
            mapper
                .getSerializationConfig()
                .getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
                .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
                .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
      }
      return mapper;
    }

    @Override
    public KeyedContextItem deserialize(byte[] message) throws IOException {
      String json = new String(message, StandardCharsets.UTF_8);
      return mapper().readValue(json, KeyedContextItem.class);
    }

    @Override
    public boolean isEndOfStream(KeyedContextItem nextElement) {
      return false;
    }

    @Override
    public TypeInformation<KeyedContextItem> getProducedType() {
      return TypeInformation.of(new TypeHint<KeyedContextItem>() {});
    }
  }
}
