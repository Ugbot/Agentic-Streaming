package org.agentic.flink.inference;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Serializable spec for an inference-model transport.
 *
 * <p>Mirrors {@link org.agentic.flink.llm.ChatConnection} exactly: ships in the Flink
 * job graph, is discovered via {@link java.util.ServiceLoader}, and produces a live
 * {@link InferenceClient} when {@link #bind(RuntimeContext)} runs inside a task's
 * {@code open()}. Backends choose which {@link InferenceClient.TaskKind}s they support; the
 * client signals that per-call via {@link InferenceClient#supports}.
 */
public interface InferenceConnection extends Serializable {

  /** Construct the operator-scoped client. Called once per task in {@code RichFunction.open()}. */
  InferenceClient bind(RuntimeContext runtimeContext) throws Exception;

  /** Human-readable provider name for logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }
}
