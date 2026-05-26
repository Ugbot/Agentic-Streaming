package org.agentic.flink.corpus;

import org.agentic.flink.memory.vector.VectorMemorySpec;
import java.util.Objects;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Corpus flavour where ingest happens in a single operator and reads happen in any number of
 * read operators, each holding a per-subtask replica.
 *
 * <p>The framework gives you the per-operator vector memory (rebuilt from broadcast updates);
 * the job-graph wiring — turning the ingest output into a {@code BroadcastStream} and
 * connecting it into the read operators' {@code BroadcastProcessFunction} — lives in the user's
 * pipeline. See {@code docs/corpus.md} for the canonical wiring snippet.
 *
 * <p>Under the hood each replica is just a {@link SingleOperatorCorpus} backed by the same
 * {@link VectorMemorySpec}. Updates arrive through the broadcast stream as
 * {@code (id, embedding, item)} triples; the operator calls {@link Corpus#upsert} on each
 * replica's view. There is no shared mutable state between subtasks beyond what Flink broadcast
 * state replicates for you.
 */
public final class BroadcastCorpus {

  private BroadcastCorpus() {}

  /** Build a spec. The flavour-specific wiring is the user's job. */
  public static CorpusSpec spec(String name, VectorMemorySpec vectorSpec) {
    return new Spec(name, vectorSpec);
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
      // Per-replica view is just a SingleOperatorCorpus; broadcast plumbing is upstream of bind.
      return SingleOperatorCorpus.spec(name, vectorSpec).bind(rc);
    }

    @Override
    public String providerName() {
      return "BroadcastCorpus(" + vectorSpec.providerName() + ")";
    }
  }
}
