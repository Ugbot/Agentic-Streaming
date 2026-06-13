package org.agentic.flink.typeinfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A reusable Flink {@link TypeInformation} that serializes a value type via the shared {@link
 * FlinkJson} JSON mapper instead of Flink's POJO machinery or the Kryo generic fallback.
 *
 * <p>Use it for types Flink would otherwise Kryo — immutable value types (no no-arg ctor / {@code
 * final} fields) or POJOs carrying {@code Map<String,Object>} — by attaching it with {@link
 * org.apache.flink.api.common.typeinfo.TypeInfo @TypeInfo} + a {@link JsonTypeInfoFactory} subclass,
 * so Flink's {@code TypeExtractor} picks it up automatically for both stream elements and keyed
 * state (no per-call-site {@code .returns(...)}).
 *
 * <p>The {@code mutable} flag controls value-copy semantics: immutable types are copied by reference
 * (cheap, safe); mutable types (e.g. an event whose fields operators rewrite) are deep-copied via a
 * JSON round-trip so Flink's object reuse never aliases live state.
 *
 * @param <T> the value type
 */
public final class JsonTypeInfo<T> extends TypeInformation<T> {
  private static final long serialVersionUID = 1L;

  private final Class<T> type;
  private final boolean mutable;

  private JsonTypeInfo(Class<T> type, boolean mutable) {
    this.type = type;
    this.mutable = mutable;
  }

  /** Immutable value type (copied by reference). */
  public static <T> JsonTypeInfo<T> of(Class<T> type) {
    return new JsonTypeInfo<>(type, false);
  }

  /** {@code mutable=true} deep-copies on {@link TypeSerializer#copy} via a JSON round-trip. */
  public static <T> JsonTypeInfo<T> of(Class<T> type, boolean mutable) {
    return new JsonTypeInfo<>(type, mutable);
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
    return new JsonSerializer<>(type, mutable);
  }

  @Override
  public String toString() {
    return "JsonTypeInfo<" + type.getSimpleName() + (mutable ? ",mutable>" : ">");
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof JsonTypeInfo
        && ((JsonTypeInfo<?>) o).type.equals(type)
        && ((JsonTypeInfo<?>) o).mutable == mutable;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, mutable);
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof JsonTypeInfo;
  }

  /** JSON-backed {@link TypeSerializer}; length-prefixed bytes from {@link FlinkJson}. */
  public static final class JsonSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1L;
    private final Class<T> type;
    private final boolean mutable;

    public JsonSerializer(Class<T> type, boolean mutable) {
      this.type = type;
      this.mutable = mutable;
    }

    private static ObjectMapper json() {
      return FlinkJson.mapper();
    }

    @Override
    public boolean isImmutableType() {
      return !mutable;
    }

    @Override
    public TypeSerializer<T> duplicate() {
      return new JsonSerializer<>(type, mutable);
    }

    @Override
    public T createInstance() {
      return null;
    }

    @Override
    public T copy(T from) {
      if (!mutable || from == null) {
        return from;
      }
      try {
        return json().readValue(json().writeValueAsBytes(from), type);
      } catch (IOException e) {
        throw new RuntimeException("JSON deep-copy failed for " + type.getName(), e);
      }
    }

    @Override
    public T copy(T from, T reuse) {
      return copy(from);
    }

    @Override
    public int getLength() {
      return -1; // variable length
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
      byte[] bytes = json().writeValueAsBytes(record);
      target.writeInt(bytes.length);
      target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
      int len = source.readInt();
      byte[] bytes = new byte[len];
      source.readFully(bytes);
      return json().readValue(bytes, type);
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

    Class<T> typeClass() {
      return type;
    }

    boolean mutable() {
      return mutable;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof JsonSerializer
          && ((JsonSerializer<?>) obj).type.equals(type)
          && ((JsonSerializer<?>) obj).mutable == mutable;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, mutable);
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
      return new JsonSerializerSnapshot<>(type, mutable);
    }
  }

  /**
   * State-compatibility snapshot — persists the element class + mutability so a checkpoint restores
   * the serializer correctly (the keyed-state use needs this; a class-less snapshot would restore a
   * null-typed serializer).
   */
  public static final class JsonSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {
    private Class<T> type;
    private boolean mutable;

    /** Public no-arg constructor required for restore. */
    public JsonSerializerSnapshot() {}

    JsonSerializerSnapshot(Class<T> type, boolean mutable) {
      this.type = type;
      this.mutable = mutable;
    }

    @Override
    public int getCurrentVersion() {
      return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
      out.writeUTF(type.getName());
      out.writeBoolean(mutable);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
        throws IOException {
      try {
        this.type = (Class<T>) Class.forName(in.readUTF(), false, userCodeClassLoader);
      } catch (ClassNotFoundException e) {
        throw new IOException("Cannot restore JsonTypeInfo serializer; class missing", e);
      }
      this.mutable = in.readBoolean();
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
      return new JsonSerializer<>(type, mutable);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
        TypeSerializerSnapshot<T> oldSerializerSnapshot) {
      if (oldSerializerSnapshot instanceof JsonSerializerSnapshot
          && Objects.equals(((JsonSerializerSnapshot<?>) oldSerializerSnapshot).type, type)) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      }
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}
