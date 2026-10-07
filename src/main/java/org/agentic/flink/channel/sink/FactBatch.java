package org.agentic.flink.channel.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/**
 * The committable of {@link PostgresTwoPhaseFactSink}: the facts one writer buffered between two
 * checkpoints plus the commit id that makes committing them idempotent.
 */
public final class FactBatch {

  /** One {@code agent_facts} row. */
  public record Fact(String flowId, String factId, String factJson, long createdAtMillis) {
    public Fact {
      Objects.requireNonNull(flowId, "flowId");
      Objects.requireNonNull(factId, "factId");
      Objects.requireNonNull(factJson, "factJson");
    }
  }

  static final SimpleVersionedSerializer<FactBatch> SERIALIZER = new Serializer();
  static final int SERIALIZER_VERSION = 1;

  private final String commitId;
  private final List<Fact> facts;

  public FactBatch(String commitId, List<Fact> facts) {
    this.commitId = Objects.requireNonNull(commitId, "commitId");
    this.facts = Collections.unmodifiableList(new ArrayList<>(facts));
  }

  public String commitId() {
    return commitId;
  }

  public List<Fact> facts() {
    return facts;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FactBatch other
        && commitId.equals(other.commitId)
        && facts.equals(other.facts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitId, facts);
  }

  @Override
  public String toString() {
    return "FactBatch{" + commitId + ", " + facts.size() + " facts}";
  }

  private static final class Serializer implements SimpleVersionedSerializer<FactBatch> {
    @Override
    public int getVersion() {
      return SERIALIZER_VERSION;
    }

    @Override
    public byte[] serialize(FactBatch batch) throws IOException {
      DataOutputSerializer out = new DataOutputSerializer(256);
      out.writeUTF(batch.commitId);
      out.writeInt(batch.facts.size());
      for (Fact fact : batch.facts) {
        out.writeUTF(fact.flowId());
        out.writeUTF(fact.factId());
        byte[] json = fact.factJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(json.length);
        out.write(json);
        out.writeLong(fact.createdAtMillis());
      }
      return out.getCopyOfBuffer();
    }

    @Override
    public FactBatch deserialize(int version, byte[] serialized) throws IOException {
      if (version != SERIALIZER_VERSION) {
        throw new IOException("unknown FactBatch serializer version " + version);
      }
      DataInputDeserializer in = new DataInputDeserializer(serialized);
      String commitId = in.readUTF();
      int count = in.readInt();
      List<Fact> facts = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        String flowId = in.readUTF();
        String factId = in.readUTF();
        byte[] json = new byte[in.readInt()];
        in.readFully(json);
        long createdAt = in.readLong();
        facts.add(
            new Fact(flowId, factId, new String(json, java.nio.charset.StandardCharsets.UTF_8), createdAt));
      }
      return new FactBatch(commitId, facts);
    }
  }
}
