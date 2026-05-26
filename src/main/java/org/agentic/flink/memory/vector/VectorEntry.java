package org.agentic.flink.memory.vector;

import org.agentic.flink.context.core.ContextItem;
import java.io.Serializable;
import java.util.Objects;

/**
 * A single vectorized entry: an embedding plus the originating context item.
 *
 * <p>Stored in Flink {@code MapState<String, VectorEntry>} so it round-trips through checkpoints
 * the same way ordinary short-term memory items do. The {@link #embedding} array is kept directly
 * (not as some shaded {@code VectorFloat<T>}) so the type works under both HashMap and RocksDB
 * state backends without needing custom serializers.
 */
public final class VectorEntry implements Serializable {
  private static final long serialVersionUID = 1L;

  private String id;
  private float[] embedding;
  private ContextItem item;

  public VectorEntry() {}

  public VectorEntry(String id, float[] embedding, ContextItem item) {
    this.id = Objects.requireNonNull(id, "id");
    this.embedding = Objects.requireNonNull(embedding, "embedding");
    this.item = item;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public float[] getEmbedding() {
    return embedding;
  }

  public void setEmbedding(float[] embedding) {
    this.embedding = embedding;
  }

  public ContextItem getItem() {
    return item;
  }

  public void setItem(ContextItem item) {
    this.item = item;
  }

  public int dimension() {
    return embedding == null ? 0 : embedding.length;
  }
}
