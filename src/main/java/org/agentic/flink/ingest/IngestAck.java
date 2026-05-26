package org.agentic.flink.ingest;

import java.io.Serializable;

/** Emitted by an {@link IngestionPipeline} per successfully indexed chunk. */
public final class IngestAck implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String chunkId;
  private final String sourceId;
  private final String corpusName;
  private final long indexedAt;

  public IngestAck(String chunkId, String sourceId, String corpusName, long indexedAt) {
    this.chunkId = chunkId;
    this.sourceId = sourceId;
    this.corpusName = corpusName;
    this.indexedAt = indexedAt;
  }

  public String getChunkId() {
    return chunkId;
  }

  public String getSourceId() {
    return sourceId;
  }

  public String getCorpusName() {
    return corpusName;
  }

  public long getIndexedAt() {
    return indexedAt;
  }

  @Override
  public String toString() {
    return "IngestAck[" + chunkId + " → " + corpusName + "]";
  }
}
