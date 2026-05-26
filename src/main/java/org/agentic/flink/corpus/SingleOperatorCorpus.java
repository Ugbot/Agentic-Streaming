package org.agentic.flink.corpus;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.memory.vector.ScoredItem;
import org.agentic.flink.memory.vector.VectorMemory;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Corpus flavour where both reads and writes happen on the same Flink operator.
 *
 * <p>Wraps any {@link VectorMemory} (typically {@link
 * org.agentic.flink.memory.vector.FlinkStateVectorMemory} or {@link
 * org.agentic.flink.memory.vector.FlinkStateHnswVectorMemory}). The operator should be a
 * {@code KeyedProcessFunction} or {@code KeyedCoProcessFunction} that binds the corpus in
 * {@code open()} and uses it from {@code processElement}/{@code processElement1}/
 * {@code processElement2}.
 */
public final class SingleOperatorCorpus implements Corpus {

  private final String name;
  private final VectorMemory memory;
  private final int dimension;
  private final String providerName;

  SingleOperatorCorpus(String name, VectorMemory memory, int dimension, String providerName) {
    this.name = name;
    this.memory = memory;
    this.dimension = dimension;
    this.providerName = providerName;
  }

  /** Build a spec backed by the given {@link VectorMemorySpec}. */
  public static CorpusSpec spec(String name, VectorMemorySpec vectorSpec) {
    return new Spec(name, vectorSpec);
  }

  @Override
  public CompletableFuture<Void> upsert(String id, float[] embedding, ContextItem item) {
    try {
      memory.put(id, embedding, item);
      return CompletableFuture.completedFuture(null);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<List<ScoredItem>> search(float[] query, int k) {
    try {
      return CompletableFuture.completedFuture(memory.search(query, k));
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public CorpusStats stats() {
    int size;
    try {
      size = memory.size();
    } catch (Exception e) {
      size = -1;
    }
    return new CorpusStats(name, providerName, size, dimension);
  }

  static final class Spec implements CorpusSpec {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final VectorMemorySpec vectorSpec;

    Spec(String name, VectorMemorySpec vectorSpec) {
      this.name = Objects.requireNonNull(name, "name");
      this.vectorSpec = Objects.requireNonNull(vectorSpec, "vectorSpec");
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Corpus bind(RuntimeContext rc) throws Exception {
      VectorMemory memory = vectorSpec.bind(rc);
      return new SingleOperatorCorpus(name, memory, vectorSpec.dimension(), vectorSpec.providerName());
    }

    @Override
    public String providerName() {
      return "SingleOperatorCorpus(" + vectorSpec.providerName() + ")";
    }
  }
}
