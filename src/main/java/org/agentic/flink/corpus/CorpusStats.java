package org.agentic.flink.corpus;

import java.io.Serializable;
import org.agentic.flink.annotation.Experimental;

/** Lightweight snapshot of corpus health. */
@Experimental
public final class CorpusStats implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final String providerName;
  private final int size;
  private final int dimension;

  public CorpusStats(String name, String providerName, int size, int dimension) {
    this.name = name;
    this.providerName = providerName;
    this.size = size;
    this.dimension = dimension;
  }

  public String getName() {
    return name;
  }

  public String getProviderName() {
    return providerName;
  }

  public int getSize() {
    return size;
  }

  public int getDimension() {
    return dimension;
  }

  @Override
  public String toString() {
    return "Corpus[" + name + "/" + providerName + ", size=" + size + ", dim=" + dimension + "]";
  }
}
