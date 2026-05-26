package org.agentic.flink.ingest;

import java.io.Serializable;
import java.util.Objects;

/** A piece of source text suitable for embedding. */
public final class Chunk implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final String text;
  private final String sourceId;
  private final int position;
  private final int tokenCount;

  public Chunk(String id, String text, String sourceId, int position, int tokenCount) {
    this.id = Objects.requireNonNull(id, "id");
    this.text = text == null ? "" : text;
    this.sourceId = sourceId == null ? "unknown" : sourceId;
    this.position = position;
    this.tokenCount = tokenCount;
  }

  public String getId() {
    return id;
  }

  public String getText() {
    return text;
  }

  public String getSourceId() {
    return sourceId;
  }

  public int getPosition() {
    return position;
  }

  public int getTokenCount() {
    return tokenCount;
  }
}
