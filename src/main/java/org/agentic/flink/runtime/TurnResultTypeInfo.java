package org.agentic.flink.runtime;

import java.io.IOException;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.jagentic.core.TurnResult;

/**
 * Flink {@link TypeInformation} for the core {@link TurnResult}: elements travel between operators
 * and through checkpoints as their normalized {@code result.schema.json} bytes ({@link
 * TurnResultCodec}), never via Kryo. {@link TurnResult} is immutable in its spec fields, so values
 * are copied by reference.
 */
public final class TurnResultTypeInfo extends TypeInformation<TurnResult> {
  private static final long serialVersionUID = 1L;
  public static final TurnResultTypeInfo INSTANCE = new TurnResultTypeInfo();

  private TurnResultTypeInfo() {}

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
  public Class<TurnResult> getTypeClass() {
    return TurnResult.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<TurnResult> createSerializer(SerializerConfig config) {
    return TurnResultSerializer.INSTANCE;
  }

  @Override
  public String toString() {
    return "TurnResultTypeInfo";
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TurnResultTypeInfo;
  }

  @Override
  public int hashCode() {
    return TurnResultTypeInfo.class.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof TurnResultTypeInfo;
  }

  /** Length-prefixed normalized-result JSON. */
  public static final class TurnResultSerializer extends TypeSerializer<TurnResult> {
    private static final long serialVersionUID = 1L;
    public static final TurnResultSerializer INSTANCE = new TurnResultSerializer();

    @Override
    public boolean isImmutableType() {
      return true;
    }

    @Override
    public TypeSerializer<TurnResult> duplicate() {
      return this;
    }

    @Override
    public TurnResult createInstance() {
      return null;
    }

    @Override
    public TurnResult copy(TurnResult from) {
      return from;
    }

    @Override
    public TurnResult copy(TurnResult from, TurnResult reuse) {
      return from;
    }

    @Override
    public int getLength() {
      return -1;
    }

    @Override
    public void serialize(TurnResult record, DataOutputView target) throws IOException {
      byte[] bytes = TurnResultCodec.encode(record);
      target.writeInt(bytes.length);
      target.write(bytes);
    }

    @Override
    public TurnResult deserialize(DataInputView source) throws IOException {
      int len = source.readInt();
      byte[] bytes = new byte[len];
      source.readFully(bytes);
      return TurnResultCodec.decode(bytes);
    }

    @Override
    public TurnResult deserialize(TurnResult reuse, DataInputView source) throws IOException {
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
      return obj instanceof TurnResultSerializer;
    }

    @Override
    public int hashCode() {
      return TurnResultSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<TurnResult> snapshotConfiguration() {
      return new Snapshot();
    }
  }

  /** Stateless snapshot: the wire form is the spec's result document, versioned by the spec. */
  public static final class Snapshot extends SimpleTypeSerializerSnapshot<TurnResult> {
    public Snapshot() {
      super(() -> TurnResultSerializer.INSTANCE);
    }
  }
}
