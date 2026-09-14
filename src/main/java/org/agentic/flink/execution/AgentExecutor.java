package org.agentic.flink.execution;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core agent execution engine that orchestrates the agentic loop.
 *
 * <p>The AgentExecutor runs the full agent lifecycle:
 * <ol>
 *   <li><b>Initialization</b> - Sets up agent context with system prompt</li>
 *   <li><b>Validation</b> - Validates input (if enabled)</li>
 *   <li><b>Execution</b> - LLM reasoning + tool calling loop</li>
 *   <li><b>Correction</b> - Fixes validation failures (if enabled)</li>
 *   <li><b>Supervision</b> - Routes to supervisor for review (if configured)</li>
 * </ol>
 *
 * <p>This executor integrates with:
 * <ul>
 *   <li><b>LLMClient</b> - For LangChain4J LLM calls</li>
 *   <li><b>ToolExecutionEngine</b> - For async tool execution</li>
 *   <li><b>ValidationExecutor</b> - For input/output validation</li>
 *   <li><b>CorrectionExecutor</b> - For correction loops</li>
 * </ul>
 *
 * <p><b>Agentic Loop Example:</b>
 * <pre>
 * 1. User: "Analyze sales data for Q3"
 * 2. Agent LLM: "I need to call the database-query tool"
 * 3. Tool Call: database-query(query="SELECT * FROM sales WHERE quarter=3")
 * 4. Tool Result: [sales data...]
 * 5. Agent LLM: "Based on the data, Q3 sales increased 15%..."
 * 6. Completion: Return final response
 * </pre>
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * AgentExecutor executor = AgentExecutor.builder()
 *     .withAgent(myAgent)
 *     .withToolRegistry(toolRegistry)
 *     .withLlmClient(llmClient)
 *     .build();
 *
 * CompletableFuture<ExecutionResult> result = executor.execute(inputEvent);
 * }</pre>
 *
 * <p><b>Cancellation.</b> The returned future supports {@code cancel(true)}: the worker thread
 * is interrupted, in-flight tool futures are cancelled and the loop stops before the next LLM
 * or tool call, so a caller that times out does not leave side effects running.
 *
 * <p><b>Idempotency.</b> Turns are identified by {@link #turnIdOf(AgentEvent)}. A completed turn
 * is recorded in the {@link TurnResultStore}; a redelivered turn returns the recorded result.
 * Tool results are recorded per {@code (turnId, callIndex)} so a retried iteration reuses
 * results instead of re-running side-effecting tools. Retries back off exponentially with
 * full jitter and the last error is carried into the failure result and its events.
 *
 * @author Agentic Flink Team
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@code org.agentic.flink.runtime.WorkflowTurnFunction} with {@code KeyedConversationLog}.
 */
@Deprecated
public class AgentExecutor implements Serializable, AutoCloseable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutor.class);

  public static final Duration DEFAULT_BACKOFF_BASE = Duration.ofMillis(200);
  public static final Duration DEFAULT_BACKOFF_CAP = Duration.ofSeconds(10);

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;
  private final ToolExecutionEngine toolEngine;
  private final ValidationExecutor validationExecutor;
  private final CorrectionExecutor correctionExecutor;
  private final TurnResultStore turnResultStore;
  private final long backoffBaseMillis;
  private final long backoffCapMillis;

  private transient volatile ExecutorService workers;
  private transient volatile ConcurrentHashMap<String, CompletableFuture<ExecutionResult>> inFlightTurns;
  private transient LongUnaryOperator sleeper;

  private AgentExecutor(AgentExecutorBuilder builder) {
    this.agent = builder.agent;
    this.toolRegistry = builder.toolRegistry;
    this.llmClient = builder.llmClient;
    this.toolEngine = new ToolExecutionEngine(toolRegistry, llmClient);
    this.validationExecutor = new ValidationExecutor(llmClient);
    this.correctionExecutor = new CorrectionExecutor(llmClient);
    this.turnResultStore = builder.turnResultStore;
    this.backoffBaseMillis = builder.backoffBase.toMillis();
    this.backoffCapMillis = builder.backoffCap.toMillis();
  }

  /**
   * Identity of a turn for deduplication: {@code turn_id} in event data, then in metadata,
   * then the correlation id, then {@code flowId/iterationNumber}.
   */
  public static String turnIdOf(AgentEvent event) {
    Object fromData = event.getData("turn_id");
    if (fromData != null) {
      return fromData.toString();
    }
    Object fromMetadata = event.getMetadata("turn_id");
    if (fromMetadata != null) {
      return fromMetadata.toString();
    }
    if (event.getCorrelationId() != null) {
      return event.getCorrelationId();
    }
    Integer iteration = event.getIterationNumber();
    return event.getFlowId() + "/" + (iteration == null ? 0 : iteration);
  }

  /**
   * Exponential backoff with full jitter: uniform in {@code [0, min(cap, base * 2^attempt)]}.
   */
  public static long backoffMillis(int attempt, long baseMillis, long capMillis, double unit) {
    if (attempt < 0 || baseMillis <= 0 || capMillis <= 0) {
      return 0L;
    }
    int shift = Math.min(attempt, 30);
    long ceiling = Math.min(capMillis, baseMillis << shift);
    if (ceiling <= 0) {
      ceiling = capMillis;
    }
    return (long) Math.floor(unit * (ceiling + 1));
  }

  public TurnResultStore getTurnResultStore() {
    return turnResultStore;
  }

  private ConcurrentHashMap<String, CompletableFuture<ExecutionResult>> inFlightTurns() {
    ConcurrentHashMap<String, CompletableFuture<ExecutionResult>> m = inFlightTurns;
    if (m == null) {
      synchronized (this) {
        m = inFlightTurns;
        if (m == null) {
          m = new ConcurrentHashMap<>();
          inFlightTurns = m;
        }
      }
    }
    return m;
  }

  private ExecutorService workers() {
    ExecutorService w = workers;
    if (w == null) {
      synchronized (this) {
        w = workers;
        if (w == null) {
          w =
              Executors.newCachedThreadPool(
                  r -> {
                    Thread t = new Thread(r, "agent-executor-" + agent.getAgentId());
                    t.setDaemon(true);
                    return t;
                  });
          workers = w;
        }
      }
    }
    return w;
  }

  private void sleep(long millis) throws InterruptedException {
    if (millis <= 0) {
      return;
    }
    if (sleeper != null) {
      sleeper.applyAsLong(millis);
      return;
    }
    Thread.sleep(millis);
  }

  /** Test hook: replaces {@link Thread#sleep(long)} for backoff waits. */
  void setSleeper(LongUnaryOperator sleeper) {
    this.sleeper = sleeper;
  }

  @Override
  public void close() {
    ExecutorService w = workers;
    if (w != null) {
      w.shutdownNow();
      workers = null;
    }
  }

  /**
   * Executes the agent for a given input event.
   *
   * <p>This is the main entry point that runs the full agentic loop asynchronously.
   *
   * @param inputEvent The input event containing user request
   * @return CompletableFuture with execution result
   */
  public CompletableFuture<ExecutionResult> execute(AgentEvent inputEvent) {
    String turnId = turnIdOf(inputEvent);
    Optional<ExecutionResult> recorded = turnResultStore.getTurnResult(turnId);
    if (recorded.isPresent()) {
      LOG.info("Turn {} already executed for flow: {}, returning recorded result",
          turnId, inputEvent.getFlowId());
      return CompletableFuture.completedFuture(recorded.get());
    }

    ExecutionContext context = new ExecutionContext(inputEvent, agent);
    Execution execution = new Execution(turnId);
    CancellableResult result = new CancellableResult(execution);
    CompletableFuture<ExecutionResult> running = inFlightTurns().putIfAbsent(turnId, result);
    if (running != null) {
      LOG.info("Turn {} already in flight for flow: {}, joining it", turnId, inputEvent.getFlowId());
      return running.thenApply(r -> r);
    }
    result.whenComplete((r, e) -> inFlightTurns().remove(turnId, result));

    LOG.info("Starting agent execution for flow: {}, agent: {}, turn: {}",
        inputEvent.getFlowId(), agent.getAgentId(), turnId);

    execution.worker =
        workers()
            .submit(
                () -> {
                  try {
                    ExecutionResult r = runAgenticLoop(context, execution);
                    if (!execution.cancelled) {
                      turnResultStore.putTurnResult(turnId, r);
                      result.complete(r);
                    }
                  } catch (InterruptedException | CancellationException e) {
                    Thread.currentThread().interrupt();
                    result.cancel(false);
                  } catch (Exception e) {
                    LOG.error("Agent execution failed for flow: {}", inputEvent.getFlowId(), e);
                    result.complete(
                        ExecutionResult.failure(
                            inputEvent.getFlowId(),
                            agent.getAgentId(),
                            describe(e),
                            context.getEvents()));
                  }
                });
    if (execution.cancelled) {
      execution.cancel();
    }
    return result;
  }

  /** Per-execution cancellation state shared between the caller's future and the worker. */
  private static final class Execution {
    final String turnId;
    final Set<CompletableFuture<?>> inFlight = ConcurrentHashMap.newKeySet();
    volatile boolean cancelled;
    volatile java.util.concurrent.Future<?> worker;

    Execution(String turnId) {
      this.turnId = turnId;
    }

    void cancel() {
      cancelled = true;
      for (CompletableFuture<?> f : inFlight) {
        f.cancel(true);
      }
      java.util.concurrent.Future<?> w = worker;
      if (w != null) {
        w.cancel(true);
      }
    }

    void checkCancelled() {
      if (cancelled || Thread.currentThread().isInterrupted()) {
        throw new CancellationException("execution of turn " + turnId + " was cancelled");
      }
    }
  }

  /** Future whose {@code cancel} propagates to the running execution. */
  private static final class CancellableResult extends CompletableFuture<ExecutionResult> {
    private final Execution execution;

    CancellableResult(Execution execution) {
      this.execution = execution;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      execution.cancel();
      return cancelled;
    }
  }

  private static String describe(Throwable t) {
    Throwable root = t;
    while (root instanceof ExecutionException && root.getCause() != null) {
      root = root.getCause();
    }
    return root.getClass().getSimpleName() + ": " + root.getMessage();
  }

  /**
   * Runs the core agentic loop (LLM reasoning + tool calling).
   *
   * <p>Flow:
   * <pre>
   * 1. Initialize agent context with system prompt
   * 2. Add user message from input event
   * 3. Loop (up to maxIterations):
   *    a. Call LLM with context
   *    b. If LLM wants to call tools:
   *       - Execute tools async
   *       - Add tool results to context
   *       - Continue loop
   *    c. If LLM returns final answer:
   *       - Break loop
   * 4. Return execution result
   * </pre>
   */
  private ExecutionResult runAgenticLoop(ExecutionContext context, Execution execution)
      throws InterruptedException {
    LOG.debug("Running agentic loop for flow: {}", context.getFlowId());

    // Build initial prompt with system message
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(createSystemMessage(agent.getSystemPrompt()));

    // Add user message from input
    String userMessage = extractUserMessage(context.getInputEvent());
    messages.add(createUserMessage(userMessage));

    // Track tool call history
    List<ToolCallResult> toolCallHistory = new ArrayList<>();
    AtomicInteger callIndex = new AtomicInteger();
    Throwable lastError = null;
    int consecutiveErrors = 0;

    // Agentic loop - iterate until completion or max iterations
    for (int iteration = 0; iteration < agent.getMaxIterations(); iteration++) {
      LOG.debug("Agentic loop iteration {}/{} for flow: {}",
          iteration + 1, agent.getMaxIterations(), context.getFlowId());

      try {
        execution.checkCancelled();
        // Call LLM with current context
        LLMResponse llmResponse = callLLM(messages, context);

        // Add LLM response to history
        context.addEvent(createLLMEvent(context, llmResponse, iteration));

        // Check if LLM wants to call tools
        if (llmResponse.hasToolCalls()) {
          LOG.info("LLM requested {} tool calls for flow: {}",
              llmResponse.getToolCalls().size(), context.getFlowId());

          // Execute tool calls
          List<ToolCallResult> toolResults = executeToolCalls(
              llmResponse.getToolCalls(), context, execution, callIndex.get());
          callIndex.addAndGet(toolResults.size());
          consecutiveErrors = 0;

          toolCallHistory.addAll(toolResults);

          // Add tool results to conversation
          for (ToolCallResult result : toolResults) {
            messages.add(createToolResultMessage(result));
            context.addEvent(createToolResultEvent(context, result));
          }

          // Continue loop with tool results
          continue;
        }

        // LLM returned final answer - complete successfully
        LOG.info("Agent completed successfully for flow: {} after {} iterations",
            context.getFlowId(), iteration + 1);

        return ExecutionResult.success(
            context.getFlowId(),
            agent.getAgentId(),
            llmResponse.getText(),
            context.getEvents(),
            toolCallHistory);

      } catch (CancellationException e) {
        throw e;
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          throw (InterruptedException) e;
        }
        execution.checkCancelled();
        lastError = e;
        consecutiveErrors++;
        LOG.error("Error in agentic loop iteration {} for flow: {}",
            iteration, context.getFlowId(), e);
        context.addEvent(createErrorEvent(context, e, iteration));

        // If not last iteration, back off and continue
        if (iteration < agent.getMaxIterations() - 1) {
          long backoff =
              backoffMillis(
                  consecutiveErrors - 1,
                  backoffBaseMillis,
                  backoffCapMillis,
                  ThreadLocalRandom.current().nextDouble());
          LOG.warn("Retrying iteration {} after {} ms backoff", iteration, backoff);
          sleep(backoff);
          continue;
        }

        // Last iteration - fail
        return ExecutionResult.failure(
            context.getFlowId(),
            agent.getAgentId(),
            "Max iterations reached with error: " + describe(e),
            context.getEvents());
      }
    }

    // Max iterations reached without completion
    LOG.warn("Agent reached max iterations ({}) for flow: {}",
        agent.getMaxIterations(), context.getFlowId());

    String message =
        lastError == null
            ? "Max iterations reached"
            : "Max iterations reached, last error: " + describe(lastError);
    return ExecutionResult.maxIterations(
        context.getFlowId(),
        agent.getAgentId(),
        message,
        context.getEvents(),
        toolCallHistory);
  }

  /**
   * Calls the LLM with the current conversation context.
   */
  private LLMResponse callLLM(List<Map<String, Object>> messages, ExecutionContext context) {
    LOG.debug("Calling LLM for flow: {} with {} messages",
        context.getFlowId(), messages.size());

    try {
      // Call LLM via LangChain4J
      LLMResponse response = llmClient.chat(messages);

      LOG.info("LLM responded with {} characters", response.getText().length());

      return response;

    } catch (Exception e) {
      LOG.error("LLM call failed for flow: {}", context.getFlowId(), e);
      throw new RuntimeException("LLM call failed: " + e.getMessage(), e);
    }
  }

  /**
   * Executes multiple tool calls concurrently.
   */
  private List<ToolCallResult> executeToolCalls(
      List<ToolCall> toolCalls, ExecutionContext context, Execution execution, int firstIndex)
      throws InterruptedException {

    LOG.info("Executing {} tool calls for flow: {}", toolCalls.size(), context.getFlowId());

    // Execute tools in parallel, reusing results recorded for (turnId, callIndex)
    List<CompletableFuture<ToolCallResult>> futures = new ArrayList<>(toolCalls.size());
    for (int i = 0; i < toolCalls.size(); i++) {
      int index = firstIndex + i;
      ToolCall toolCall = toolCalls.get(i);
      Optional<ToolCallResult> recorded = turnResultStore.getToolResult(execution.turnId, index);
      if (recorded.isPresent()) {
        LOG.info("Reusing recorded result for tool {} (turn {}, call {})",
            toolCall.getToolName(), execution.turnId, index);
        futures.add(CompletableFuture.completedFuture(recorded.get()));
        continue;
      }
      execution.checkCancelled();
      CompletableFuture<ToolCallResult> f = toolEngine.executeTool(toolCall, context);
      execution.inFlight.add(f);
      futures.add(
          f.whenComplete(
              (r, err) -> {
                execution.inFlight.remove(f);
                if (err == null && r != null && r.isSuccess() && !execution.cancelled) {
                  turnResultStore.putToolResult(execution.turnId, index, r);
                }
              }));
    }

    try {
      List<ToolCallResult> results = new ArrayList<>(futures.size());
      for (CompletableFuture<ToolCallResult> f : futures) {
        results.add(f.get());
      }
      return results;
    } catch (InterruptedException e) {
      futures.forEach(f -> f.cancel(true));
      throw e;
    } catch (CancellationException e) {
      throw e;
    } catch (ExecutionException e) {
      LOG.error("Error executing tool calls for flow: {}", context.getFlowId(), e.getCause());
      throw new RuntimeException("Tool execution failed: " + describe(e.getCause()), e.getCause());
    }
  }

  // ==================== Message Creation ====================

  private Map<String, Object> createSystemMessage(String content) {
    Map<String, Object> msg = new HashMap<>();
    msg.put("role", "system");
    msg.put("content", content);
    return msg;
  }

  private Map<String, Object> createUserMessage(String content) {
    Map<String, Object> msg = new HashMap<>();
    msg.put("role", "user");
    msg.put("content", content);
    return msg;
  }

  private Map<String, Object> createToolResultMessage(ToolCallResult result) {
    Map<String, Object> msg = new HashMap<>();
    msg.put("role", "tool");
    msg.put("tool_call_id", result.getToolCallId());
    msg.put("tool_name", result.getToolName());
    msg.put("content", result.getResult());
    return msg;
  }

  // ==================== Event Creation ====================

  private AgentEvent createLLMEvent(ExecutionContext context, LLMResponse response, int iteration) {
    AgentEvent event = context.getInputEvent()
        .withEventType(AgentEventType.LOOP_ITERATION_COMPLETED);
    event.setIterationNumber(iteration);
    event.putMetadata("state", AgentState.EXECUTING.name());
    event.getData().put("llm_response", response.getText());
    event.getData().put("model", response.getModel());
    event.getData().put("tool_calls_requested", response.getToolCalls().size());
    return event;
  }

  private AgentEvent createErrorEvent(ExecutionContext context, Throwable error, int iteration) {
    AgentEvent event = context.getInputEvent().withEventType(AgentEventType.ERROR_OCCURRED);
    event.setIterationNumber(iteration);
    event.putMetadata("state", AgentState.EXECUTING.name());
    event.setErrorMessage(error.getMessage());
    event.setErrorCode(error.getClass().getSimpleName());
    event.getData().put("error", describe(error));
    event.getData().put("iteration", iteration);
    return event;
  }

  private AgentEvent createToolResultEvent(ExecutionContext context, ToolCallResult result) {
    AgentEvent event = context.getInputEvent()
        .withEventType(result.isSuccess()
            ? AgentEventType.TOOL_CALL_COMPLETED
            : AgentEventType.TOOL_CALL_FAILED);
    event.putMetadata("state", AgentState.EXECUTING.name());
    event.getData().put("tool_name", result.getToolName());
    event.getData().put("result", result.getResult());
    event.getData().put("success", result.isSuccess());
    if (!result.isSuccess()) {
      event.setErrorMessage(result.getError());
    }
    return event;
  }

  // ==================== Helpers ====================

  private String extractUserMessage(AgentEvent event) {
    Object userMsg = event.getData("user_message");
    if (userMsg != null) {
      return userMsg.toString();
    }
    Object prompt = event.getData("prompt");
    if (prompt != null) {
      return prompt.toString();
    }
    return "Please process this request: " + event.getData();
  }

  // ==================== Builder ====================

  public static AgentExecutorBuilder builder() {
    return new AgentExecutorBuilder();
  }

  public static class AgentExecutorBuilder {
    private Agent agent;
    private ToolRegistry toolRegistry;
    private LLMClient llmClient;
    private TurnResultStore turnResultStore;
    private Duration backoffBase = DEFAULT_BACKOFF_BASE;
    private Duration backoffCap = DEFAULT_BACKOFF_CAP;

    /** Store used for turn and tool result deduplication; defaults to a bounded in-memory one. */
    public AgentExecutorBuilder withTurnResultStore(TurnResultStore store) {
      this.turnResultStore = Objects.requireNonNull(store, "store");
      return this;
    }

    /** Retry backoff: full-jitter exponential from {@code base} capped at {@code cap}. */
    public AgentExecutorBuilder withRetryBackoff(Duration base, Duration cap) {
      if (base == null || base.isNegative() || cap == null || cap.isNegative()) {
        throw new IllegalArgumentException("backoff base and cap must be non-negative");
      }
      this.backoffBase = base;
      this.backoffCap = cap;
      return this;
    }

    public AgentExecutorBuilder withAgent(Agent agent) {
      this.agent = agent;
      return this;
    }

    public AgentExecutorBuilder withToolRegistry(ToolRegistry toolRegistry) {
      this.toolRegistry = toolRegistry;
      return this;
    }

    public AgentExecutorBuilder withLlmClient(LLMClient llmClient) {
      this.llmClient = llmClient;
      return this;
    }

    public AgentExecutor build() {
      if (agent == null) {
        throw new IllegalStateException("Agent is required");
      }
      if (toolRegistry == null) {
        toolRegistry = ToolRegistry.empty();
      }
      if (llmClient == null) {
        // Create default LLM client
        llmClient = LLMClient.createDefault(agent.getLlmModel(), agent.getTemperature());
      }
      if (turnResultStore == null) {
        turnResultStore = new InMemoryTurnResultStore();
      }
      return new AgentExecutor(this);
    }
  }
}
