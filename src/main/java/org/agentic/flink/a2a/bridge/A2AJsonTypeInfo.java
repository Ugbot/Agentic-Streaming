package org.agentic.flink.a2a.bridge;

import java.io.IOException;
import java.util.Objects;
import org.agentic.flink.a2a.A2AJson;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Flink {@link TypeInformation} for A2A envelopes that serializes via the {@link A2AJson} JSON
 * mapper instead of Flink's POJO/Kryo machinery.
 *
 * <p>{@link A2ARequest}/{@link A2AResponse} are immutable value types (no no-arg constructor,
 * {@code unmodifiable} collections), which Flink can neither treat as POJOs nor reliably Kryo-copy
 * between operators. Routing them through the same JSON codec used on the wire makes them first-class
 * stream element types — used by the bridge {@link org.agentic.flink.channel.Channel} sources and
 * any operator that emits them.
 */
public final class A2AJsonTypeInfo<T> extends TypeInformation<T> {
  private static final long serialVersionUID = 1L;

  private final Class<T> type;

  private A2AJsonTypeInfo(Class<T> type) {
    this.type = type;
  }

  public static <T> A2AJsonTypeInfo<T> of(Class<T> type) {
    return new A2AJsonTypeInfo<>(type);
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<T> getTypeClass() {
    return type;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<T> createSerializer(ExecutionConfig config) {
    return new A2AJsonSerializer<>(type);
  }

  @Override
  public String toString() {
    return "A2AJsonTypeInfo<" + type.getSimpleName() + ">";
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof A2AJsonTypeInfo && ((A2AJsonTypeInfo<?>) o).type.equals(type);
  }

  @Override
  public int hashCode() {
    return type.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof A2AJsonTypeInfo;
  }

  /** JSON-backed {@link TypeSerializer}. */
  public static final class A2AJsonSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1L;
    private final Class<T> type;

    public A2AJsonSerializer(Class<T> type) {
      this.type = type;
    }

    @Override
    public boolean isImmutableType() {
      return true;
    }

    @Override
    public TypeSerializer<T> duplicate() {
      return new A2AJsonSerializer<>(type);
    }

    @Override
    public T createInstance() {
      return null;
    }

    @Override
    public T copy(T from) {
      return from; // immutable
    }

    @Override
    public T copy(T from, T reuse) {
      return from;
    }

    @Override
    public int getLength() {
      return -1; // variable length
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
      byte[] bytes = A2AJson.mapper().writeValueAsBytes(record);
      target.writeInt(bytes.length);
      target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
      int len = source.readInt();
      byte[] bytes = new byte[len];
      source.readFully(bytes);
      return A2AJson.mapper().readValue(bytes, type);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
      return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
      int len = source.readInt();
      byte[] bytes = new byte[len];
      source.readFully(bytes);
      target.writeInt(len);
      target.write(bytes);
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof A2AJsonSerializer && ((A2AJsonSerializer<?>) obj).type.equals(type);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type);
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
      return new A2AJsonSerializerSnapshot<>(type);
    }
  }

  /** State-compatibility snapshot; carries the element class so the serializer can be restored. */
  public static final class A2AJsonSerializerSnapshot<T>
      extends SimpleTypeSerializerSnapshot<T> {
    public A2AJsonSerializerSnapshot() {
      // Required public no-arg constructor for restore; type is read from the snapshot.
      super(() -> new A2AJsonSerializer<>(null));
    }

    A2AJsonSerializerSnapshot(Class<T> type) {
      super(() -> new A2AJsonSerializer<>(type));
    }
  }
}
