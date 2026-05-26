package org.agentic.flink.memory.vector;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;

/** Serializable factory for {@link VectorMemory}. Built per-task in {@code open()}. */
public interface VectorMemorySpec extends Serializable {

  /** Construct the operator-scoped vector memory for the running task. */
  VectorMemory bind(RuntimeContext runtimeContext) throws Exception;

  /** Vector dimension; required so descriptors can validate inserts cheaply. */
  int dimension();

  /** Soft cap on entries per scope; implementations may evict by recency once exceeded. */
  default int maxItems() {
    return 10_000;
  }

  /** Similarity function — defaults to cosine. */
  default Similarity similarity() {
    return Similarity.COSINE;
  }

  /** Scope for the underlying state. */
  default Scope scope() {
    return Scope.PER_KEY;
  }

  /** Human-readable provider name for logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }

  /** Where the vector state lives. */
  enum Scope {
    /** One vector graph per Flink key (per conversation). State is keyed state. */
    PER_KEY,
    /**
     * One shared vector graph per task slot. State is operator state (broadcast or union). Useful
     * for cross-conversation fact retrieval; updates must be broadcast into the operator.
     */
    PER_OPERATOR
  }

  /** Similarity function. */
  enum Similarity {
    COSINE,
    DOT_PRODUCT,
    NEGATIVE_L2
  }
}
