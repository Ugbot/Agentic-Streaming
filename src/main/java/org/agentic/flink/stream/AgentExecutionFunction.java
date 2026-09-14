package org.agentic.flink.stream;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.job.AgentResultRouter;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async Flink function for executing agents in a streaming context.
 *
 * <p>This function integrates the AgentExecutor with Flink's async I/O,
 * allowing agents to process events asynchronously without blocking the stream.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>Async agent execution with non-blocking I/O</li>
 *   <li>Real LLM calls via LangChain4J</li>
 *   <li>Real tool execution</li>
 *   <li>Error handling and event enrichment</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * AgentExecutionFunction fn = new AgentExecutionFunction(agent, toolRegistry, llmClient);
 * DataStream<AgentEvent> results = AsyncDataStream.unorderedWait(
 *     inputStream, fn, fn.getTimeout().toMillis(), TimeUnit.MILLISECONDS, capacity);
 * }</pre>
 *
 * <p><b>Timeouts.</b> The wall clock budget for one turn is {@link Agent#getTimeout()} or
 * {@link #DEFAULT_TIMEOUT}. Wire the same value into {@code AsyncDataStream}; when Flink
 * calls {@link #timeout}, the in-flight {@link AgentExecutor} future is cancelled with
 * {@code cancel(true)} so the LLM loop and tool futures stop instead of running orphaned.
 * Timed-out and failed turns are emitted as {@code FLOW_FAILED} with a
 * {@code failure_kind} data field ({@code timeout}, {@code execution} or {@code error}).
 *
 * @author Agentic Flink Team
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated
public class AgentExecutionFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 2L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionFunction.class);

  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  public static final String TIMEOUTS_METRIC = "agent_turn_timeouts";
  public static final String CANCELLED_METRIC = "agent_turns_cancelled";

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;
  private final long timeoutMillis;

  private transient AgentExecutor executor;
  private transient Map<AgentEvent, CompletableFuture<ExecutionResult>> inFlight;
  private transient Counter timeouts;
  private transient Counter cancelled;

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry) {
    this(agent, toolRegistry, null);
  }

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry, LLMClient llmClient) {
    this.agent = agent;
    this.toolRegistry = toolRegistry;
    this.llmClient = llmClient;
    Duration configured = agent.getTimeout();
    this.timeoutMillis = (configured == null ? DEFAULT_TIMEOUT : configured).toMillis();
    if (timeoutMillis <= 0) {
      throw new IllegalArgumentException("agent timeout must be positive, got " + configured);
    }
  }

  /** Per-turn timeout to pass to {@code AsyncDataStream}. */
  public Duration getTimeout() {
    return Duration.ofMillis(timeoutMillis);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    LOG.info("Initializing AgentExecutor for agent: {}", agent.getAgentId());

    // Create agent executor
    executor = AgentExecutor.builder()
        .withAgent(agent)
        .withToolRegistry(toolRegistry)
        .withLlmClient(llmClient)
        .build();
    inFlight = Collections.synchronizedMap(new IdentityHashMap<>());
    timeouts = getRuntimeContext().getMetricGroup().counter(TIMEOUTS_METRIC);
    cancelled = getRuntimeContext().getMetricGroup().counter(CANCELLED_METRIC);

    LOG.info("AgentExecutor initialized successfully");
  }

  @Override
  public void close() throws Exception {
    if (inFlight != null) {
      synchronized (inFlight) {
        inFlight.values().forEach(f -> f.cancel(true));
        inFlight.clear();
      }
    }
    if (executor != null) {
      executor.close();
    }
    super.close();
  }

  @Override
  public void asyncInvoke(AgentEvent inputEvent, ResultFuture<AgentEvent> resultFuture) {
    LOG.debug("Processing event: flow={}, agent={}, type={}",
        inputEvent.getFlowId(), inputEvent.getAgentId(), inputEvent.getEventType());

    // Execute agent asynchronously
    CompletableFuture<ExecutionResult> future = executor.execute(inputEvent);
    inFlight.put(inputEvent, future);

    // Handle completion
    future.whenComplete((result, error) -> {
      inFlight.remove(inputEvent, future);
      if (future.isCancelled()) {
        cancelled.inc();
        LOG.warn("Agent execution cancelled for flow: {}", inputEvent.getFlowId());
        return;
      }
      if (error != null) {
        LOG.error("Agent execution failed for flow: {}", inputEvent.getFlowId(), error);
        resultFuture.complete(java.util.Collections.singleton(
            failure(inputEvent, error.getMessage(), AgentResultRouter.FAILURE_KIND_ERROR)));

      } else if (result.isSuccess()) {
        LOG.info("Agent execution succeeded for flow: {}", inputEvent.getFlowId());

        // Create success event with output
        AgentEvent successEvent = inputEvent.withEventType(AgentEventType.FLOW_COMPLETED);
        successEvent.incrementIteration();
        successEvent.putMetadata("state", AgentState.COMPLETED.name());
        successEvent.putData("agent_id", agent.getAgentId());
        successEvent.putData("result", result.getOutput());
        successEvent.putData("output", result.getOutput());
        successEvent.putData("tool_calls", result.getToolCalls() != null ? result.getToolCalls().size() : 0);
        successEvent.putData("tool_call_count", result.getEvents().size());
        successEvent.putData("events_generated", result.getEvents().size());
        successEvent.putData("completion_timestamp", System.currentTimeMillis());

        resultFuture.complete(java.util.Collections.singleton(successEvent));

      } else {
        LOG.warn("Agent execution completed with failure for flow: {}: {}",
            inputEvent.getFlowId(), result.getErrorMessage());
        resultFuture.complete(java.util.Collections.singleton(
            failure(inputEvent, result.getErrorMessage(), AgentResultRouter.FAILURE_KIND_EXECUTION)));
      }
    });
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.error("Agent execution timed out after {} ms for flow: {}", timeoutMillis, input.getFlowId());
    timeouts.inc();

    CompletableFuture<ExecutionResult> future = inFlight.remove(input);
    if (future != null) {
      future.cancel(true);
    }

    AgentEvent timeoutEvent =
        failure(input, "Agent execution timed out after " + timeoutMillis + " ms",
            AgentResultRouter.FAILURE_KIND_TIMEOUT);
    resultFuture.complete(java.util.Collections.singleton(timeoutEvent));
  }

  private AgentEvent failure(AgentEvent input, String error, String kind) {
    AgentEvent failureEvent = input.withEventType(AgentEventType.FLOW_FAILED);
    failureEvent.incrementIteration();
    failureEvent.putMetadata("state", AgentState.FAILED.name());
    failureEvent.setErrorMessage(error);
    failureEvent.putData("error", error);
    failureEvent.putData("agent_id", agent.getAgentId());
    failureEvent.putData(AgentResultRouter.FAILURE_KIND, kind);
    return failureEvent;
  }
}
