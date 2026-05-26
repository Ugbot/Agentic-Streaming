package org.agentic.flink.corpus;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Serializable factory for a {@link Corpus}. Built per-task in {@code RichFunction.open()}.
 *
 * <p>Mirrors the Connection/Client/Setup pattern used elsewhere in the framework: the spec is
 * what travels in the job graph; the live runtime view is constructed in {@code bind}.
 */
public interface CorpusSpec extends Serializable {

  /** Logical corpus name (used in logs + the stats snapshot). */
  String name();

  /** Build the operator-scoped {@link Corpus}. */
  Corpus bind(RuntimeContext runtimeContext) throws Exception;

  /** Human-readable provider name for logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }
}
