package org.agentic.flink.listener;

import java.io.Serializable;
import java.util.List;

/**
 * Lifecycle hook interface for observability over agent operations.
 *
 * <p>Every method has a no-op default so implementations can override just the slices they care
 * about. Discovered via {@link java.util.ServiceLoader}; users may also register listeners
 * programmatically via {@code AgentBuilder.withListener(...)}.
 *
 * <p>Implementations must be {@link Serializable} — they ride along with the agent operator in
 * the Flink job graph.
 */
public interface AgentEventListener extends Serializable {

  /** Called once per task when the agent operator starts. */
  default void onAgentStart(String agentId) {}

  /** Called before each chat request to the LLM. */
  default void onChatRequest(String agentId, String modelName, int messageCount) {}

  /** Called after a chat response is received. */
  default void onChatResponse(
      String agentId, String modelName, int responseLength, Long tokensUsed) {}

  /** Called before a tool invocation. */
  default void onToolCallStart(String agentId, String toolName, String toolCallId) {}

  /** Called after a tool invocation completes (success or failure). */
  default void onToolCallEnd(
      String agentId, String toolName, String toolCallId, boolean success, long durationMs) {}

  /** Called after a context compaction round. */
  default void onCompaction(
      String agentId, String flowId, int itemsBefore, int itemsAfter, long durationMs) {}

  /** Called after a write-behind sync to the long-term store. */
  default void onLongTermSync(String agentId, String flowId, int factsWritten) {}

  /** Called when an unhandled error is observed. */
  default void onError(String agentId, String stage, Throwable error) {}

  /** Called after a single inference-model call (classifier, scorer, embedder, generic). */
  default void onInference(String agentId, String modelName, String task, long durationMs) {}

  /** Called when a guardrail blocks an LLM interaction. */
  default void onGuardrailBlock(String agentId, String modelName, String label) {}

  /** Called when a guardrail rewrites an LLM interaction. */
  default void onGuardrailRewrite(String agentId, String modelName, String reason) {}

  /** Human-readable name for logging. */
  default String name() {
    return getClass().getSimpleName();
  }

  /** Convenience: wrap a list of listeners into a single fan-out. */
  static AgentEventListener fanOut(List<AgentEventListener> listeners) {
    return new CompositeListener(listeners);
  }
}
