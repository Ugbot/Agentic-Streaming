package org.agentic.flink.execution;

import java.io.Serializable;
import java.util.Optional;

/**
 * Records completed turn results and individual tool results so that a redelivered turn
 * returns the recorded result instead of re-running LLM and tool side effects.
 *
 * <p>Keys:
 * <ul>
 *   <li>turn results are keyed by {@code turnId} (see {@link AgentExecutor#turnIdOf})</li>
 *   <li>tool results are keyed by {@code (turnId, callIndex)} where {@code callIndex} is the
 *       zero-based position of the tool call in the order the execution requested it</li>
 * </ul>
 *
 * <p>Implementations must be thread safe: the agentic loop runs on worker threads and tool
 * results are recorded from tool completion callbacks.
 *
 * @deprecated Part of the legacy Flink DSL execution path. New code should use the
 *     event-sourced runtime ({@code org.agentic.flink.runtime.WorkflowTurnFunction} with
 *     {@code KeyedConversationLog}), which dedups turns through the keyed conversation log.
 */
@Deprecated
public interface TurnResultStore extends Serializable {

  Optional<ExecutionResult> getTurnResult(String turnId);

  void putTurnResult(String turnId, ExecutionResult result);

  Optional<ToolCallResult> getToolResult(String turnId, int callIndex);

  void putToolResult(String turnId, int callIndex, ToolCallResult result);

  /** Number of recorded turn results currently retained. */
  int size();
}
