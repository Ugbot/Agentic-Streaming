package org.jagentic.tools.ingest;

import java.io.Serializable;
import java.util.List;

/**
 * Splits a source document into a list of {@link Chunk}s ready for embedding.
 *
 * <p>Implementations must be serializable because they ship in the Flink job graph. They must
 * also be idempotent: calling {@code chunk} twice on the same source must produce the same
 * chunk ids and ordering — downstream stages rely on this to avoid duplicates after restart.
 */
public interface Chunker extends Serializable {

  List<Chunk> chunk(String sourceId, String text);
}
