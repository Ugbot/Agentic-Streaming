package org.agentic.flink.a2a.bridge;

import java.io.IOException;
import org.agentic.flink.a2a.A2AJson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Flink JSON (de)serialization schemas for A2A bridge envelopes, backed by the canonical {@link
 * A2AJson} mapper.
 *
 * <p>The built-in {@code ZeroMqChannel}/{@code ZeroMqSink}/{@code KafkaChannel} JSON schemas use a
 * vanilla {@code ObjectMapper}, which cannot bind our immutable constructors and renames boolean
 * getters — so all bridge transports must use these schemas (and the gateway connector the same
 * {@code A2AJson} mapper) to round-trip {@link A2ARequest}/{@link A2AResponse} consistently.
 */
public final class A2AWireSerde {

  private A2AWireSerde() {}

  public static <T> DeserializationSchema<T> deserializer(Class<T> type) {
    return new JsonDeserializationSchema<>(type);
  }

  public static <T> SerializationSchema<T> serializer() {
    return new JsonSerializationSchema<>();
  }

  static final class JsonDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final Class<T> type;

    JsonDeserializationSchema(Class<T> type) {
      this.type = type;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
      return A2AJson.mapper().readValue(message, type);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
      return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
      return TypeInformation.of(type);
    }
  }

  static final class JsonSerializationSchema<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(T element) {
      try {
        return A2AJson.mapper().writeValueAsBytes(element);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize A2A bridge envelope", e);
      }
    }
  }
}
