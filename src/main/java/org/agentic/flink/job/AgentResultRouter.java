package org.agentic.flink.job;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.statemachine.AgentState;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Routes legacy pipeline events to the {@link AgentJobGenerator} side output tags.
 *
 * <p>Completed flows go to the main output. Failures are split by the
 * {@code failure_kind} data field written by {@link org.agentic.flink.stream.AgentExecutionFunction}:
 * {@code timeout} goes to {@link AgentJobGenerator#TIMEOUT_TAG}, everything else to
 * {@link AgentJobGenerator#VALIDATION_FAILURES_TAG}. CEP pattern timeouts and compensation
 * requests emitted by {@link AgentExecutionFunction} are forwarded to their tags unchanged.
 *
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated
public class AgentResultRouter extends ProcessFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 1L;

  public static final String FAILURE_KIND = "failure_kind";
  public static final String FAILURE_KIND_TIMEOUT = "timeout";
  public static final String FAILURE_KIND_EXECUTION = "execution";
  public static final String FAILURE_KIND_ERROR = "error";

  @Override
  public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out) {
    AgentEventType type = event.getEventType();
    if (type == null) {
      out.collect(event);
      return;
    }
    switch (type) {
      case FLOW_FAILED:
        if (AgentState.COMPENSATING.name().equals(event.getMetadata("state"))) {
          ctx.output(AgentJobGenerator.COMPENSATION_TAG, event);
        } else if (FAILURE_KIND_TIMEOUT.equals(event.getData(FAILURE_KIND))) {
          ctx.output(AgentJobGenerator.TIMEOUT_TAG, event);
        } else {
          ctx.output(AgentJobGenerator.VALIDATION_FAILURES_TAG, event);
        }
        break;
      case TIMEOUT_OCCURRED:
        ctx.output(AgentJobGenerator.TIMEOUT_TAG, event);
        break;
      case COMPENSATION_REQUESTED:
        ctx.output(AgentJobGenerator.COMPENSATION_TAG, event);
        break;
      default:
        out.collect(event);
    }
  }
}
