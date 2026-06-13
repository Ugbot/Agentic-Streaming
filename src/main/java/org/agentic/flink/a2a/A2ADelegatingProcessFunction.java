package org.agentic.flink.a2a;
import org.apache.flink.api.common.functions.OpenContext;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed operator that delegates each {@link AgentEvent} to a remote A2A agent and emits the
 * enriched event downstream — the runtime behind {@link A2AStep}.
 *
 * <p>Keyed by A2A {@code contextId} (the {@link A2AStep}'s key selector), so a per-conversation
 * {@link ValueState} can carry the remote {@code contextId} across events for continuity: the first
 * call lets the peer assign a context, and subsequent events on the same key reuse it. This is the
 * Flink-state-first analogue of conversation memory — no external store required for the step.
 *
 * <p>The remote call ({@code sendAndAwait}, or {@code stream} when the spec opts in) is synchronous
 * within {@code processElement}; bound it with {@link RemoteAgentSpec#requestTimeoutMs()}. The
 * client and blocking are confined to the task side via a {@code transient} client rebuilt in
 * {@link #open(org.apache.flink.api.common.functions.OpenContext)}.
 */
public final class A2ADelegatingProcessFunction
    extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(A2ADelegatingProcessFunction.class);

  private final A2AStep step;

  private transient A2AClient client;
  private transient ValueState<String> contextIdState;

  public A2ADelegatingProcessFunction(A2AStep step) {
    this.step = java.util.Objects.requireNonNull(step, "step");
  }

  @Override
  public void open(OpenContext openContext) {
    client = step.clientFactory().create(step.spec());
    contextIdState =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("a2a-context-" + step.name(), String.class));
  }

  @Override
  public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out)
      throws Exception {
    String input = A2AStepSupport.resolveInput(step, event);
    String contextId = contextIdState.value();

    try {
      A2ATask task = A2AStepSupport.call(client, step, input, contextId);

      if (task.getContextId() != null && !task.getContextId().equals(contextId)) {
        contextIdState.update(task.getContextId());
      }

      A2AStepSupport.applyResult(step, event, task);

      if (task.getState() != A2ATaskState.COMPLETED && step.failOnError()) {
        emitFailure(event, "A2A step '" + step.name() + "' ended in state " + task.getState().wire(), out);
        return;
      }
      out.collect(event);
    } catch (A2AClientException e) {
      LOG.warn("A2A step '{}' failed for key {}: {}", step.name(), ctx.getCurrentKey(), e.getMessage());
      if (step.failOnError()) {
        emitFailure(event, e.getMessage(), out);
      } else {
        event.putData(step.outputKey() + ".error", e.getMessage());
        out.collect(event);
      }
    }
  }

  private void emitFailure(AgentEvent event, String error, Collector<AgentEvent> out) {
    AgentEvent failed = event.withEventType(AgentEventType.FLOW_FAILED);
    failed.setErrorMessage(error);
    out.collect(failed);
  }
}
