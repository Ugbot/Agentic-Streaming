package org.agentic.flink.stream;

import org.agentic.flink.annotation.Internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.agentic.flink.a2a.A2AToolRegistry;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.job.AgentResultRouter;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.listener.CompositeListener;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import org.agentic.flink.tools.mcp.McpServerSpec;
import org.agentic.flink.tools.mcp.McpToolExecutor;
import org.agentic.flink.tools.mcp.McpToolRegistry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async Flink function that executes one agent turn per input event.
 *
 * <p>This operator is the consumer of the legacy {@link Agent} configuration. In {@link #open} it
 * assembles the effective tool registry (job tools, MCP-discovered tools, inference tools and A2A
 * peers, each wrapped with the agent's tool defaults and tool timeout), validates {@link
 * Agent#getRequiredTools()} against it, and builds an {@link LLMClient} carrying the agent's
 * guardrails and listeners. {@link AgentExecutor} then runs the LLM loop; structured tool calls are
 * taken from the provider response and only fall back to Jackson parsing of the text.
 *
 * <p>After every turn the operator appends the exchange to the agent's {@link ConversationStore}
 * (keyed by flow id), archives the outcome as a fact in the {@link LongTermMemoryStore} when one is
 * configured, and, when compensation is enabled, turns a failed turn into a compensation request
 * listing the compensating tool of every completed tool call so {@link CompensationFunction} can
 * roll them back in reverse order.
 *
 * <p><b>Usage:</b>
 *
 * <pre>{@code
 * AgentExecutionFunction fn = new AgentExecutionFunction(agent, toolRegistry);
 * DataStream<AgentEvent> results = AsyncDataStream.unorderedWait(
 *     inputStream, fn, fn.getTimeout().toMillis(), TimeUnit.MILLISECONDS, capacity);
 * }</pre>
 *
 * <p><b>Timeouts.</b> The wall clock budget for one turn is {@link Agent#getTimeout()} or {@link
 * #DEFAULT_TIMEOUT}. Wire the same value into {@code AsyncDataStream}; when Flink calls {@link
 * #timeout}, the in-flight {@link AgentExecutor} future is cancelled with {@code cancel(true)} so
 * the LLM loop and tool futures stop instead of running orphaned. Timed-out and failed turns are
 * emitted as {@code FLOW_FAILED} with a {@code failure_kind} data field ({@code timeout}, {@code
 * execution} or {@code error}).
 *
 * @author Agentic Flink Team
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated(since = "1.0.0")
@Internal
public class AgentExecutionFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final long serialVersionUID = 3L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionFunction.class);

  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  public static final String TIMEOUTS_METRIC = "agent_turn_timeouts";
  public static final String CANCELLED_METRIC = "agent_turns_cancelled";

  /** Metadata key holding the ordered list of compensation actions on a failed turn. */
  public static final String COMPENSATION_ACTIONS_METADATA = "compensation_actions";

  /** Fact id prefix used when archiving a turn outcome in the long-term store. */
  public static final String TURN_FACT_PREFIX = "turn:";

  private final Agent agent;
  private final ToolRegistry toolRegistry;
  private final LLMClient llmClient;
  private final long timeoutMillis;

  private transient AgentExecutor executor;
  private transient ToolRegistry effectiveRegistry;
  private transient AgentEventListener listener;
  private transient Map<AgentEvent, CompletableFuture<ExecutionResult>> inFlight;
  private transient Counter timeouts;
  private transient Counter cancelled;

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry) {
    this(agent, toolRegistry, null);
  }

  public AgentExecutionFunction(Agent agent, ToolRegistry toolRegistry, LLMClient llmClient) {
    this.agent = agent;
    this.toolRegistry = toolRegistry == null ? ToolRegistry.empty() : toolRegistry;
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

  /**
   * The tool registry this operator executes against after {@link #open}: job tools plus MCP,
   * inference and A2A tools, each wrapped with the agent's tool defaults and tool timeout.
   */
  public ToolRegistry getEffectiveToolRegistry() {
    if (effectiveRegistry == null) {
      throw new IllegalStateException("open() has not been called");
    }
    return effectiveRegistry;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    LOG.info("Initializing AgentExecutor for agent: {}", agent.getAgentId());

    listener = new CompositeListener(agent.getListeners());
    effectiveRegistry = assembleRegistry();
    effectiveRegistry.validateRequiredTools(agent.getRequiredTools());

    LLMClient client =
        llmClient != null
            ? llmClient
            : LLMClient.builder()
                .withModel(agent.getLlmModel())
                .withTemperature(agent.getTemperature())
                .withMaxTokens(agent.getMaxResponseTokens())
                .build(agent.getChatConnection());
    client.withGuardrails(agent.getGuardrails(), agent.getAgentId(), listener);

    executor =
        AgentExecutor.builder()
            .withAgent(agent)
            .withToolRegistry(effectiveRegistry)
            .withLlmClient(client)
            .build();
    inFlight = Collections.synchronizedMap(new IdentityHashMap<>());
    timeouts = getRuntimeContext().getMetricGroup().counter(TIMEOUTS_METRIC);
    cancelled = getRuntimeContext().getMetricGroup().counter(CANCELLED_METRIC);
    listener.onAgentStart(agent.getAgentId());

    LOG.info("AgentExecutor initialized with {} tools", effectiveRegistry.getToolNames().size());
  }

  private ToolRegistry assembleRegistry() throws Exception {
    ToolRegistry.ToolRegistryBuilder builder = ToolRegistry.builder();
    for (String name : toolRegistry.getToolNames()) {
      ToolRegistry.ToolDefinition def = toolRegistry.getTool(name).orElseThrow();
      builder.registerTool(name, def);
    }
    for (McpServerSpec spec : agent.getMcpServers()) {
      List<McpToolExecutor> discovered = McpToolRegistry.discover(spec);
      for (McpToolExecutor mcp : discovered) {
        builder.registerTool(mcp.getToolId(), mcp.getDescription(), mcp);
      }
      LOG.info("Registered {} MCP tools from server {}", discovered.size(), spec.getName());
    }
    for (InferenceToolAdapter adapter : agent.getInferenceTools()) {
      builder.registerTool(adapter.getToolId(), adapter.getDescription(), adapter);
    }
    if (agent.hasRemoteAgents()) {
      A2AToolRegistry.registerInto(builder, agent);
    }
    ToolRegistry assembled = builder.build();

    ToolRegistry.ToolRegistryBuilder configured = ToolRegistry.builder();
    for (String name : assembled.getToolNames()) {
      ToolRegistry.ToolDefinition def = assembled.getTool(name).orElseThrow();
      ToolExecutor raw = def.getExecutor();
      ToolExecutor wrapped =
          raw == null
              ? null
              : new ConfiguredToolExecutor(
                  raw,
                  agent.getToolDefaults().getOrDefault(name, Collections.emptyMap()),
                  agent.getToolTimeout());
      configured.registerTool(
          name,
          new ToolRegistry.ToolDefinition(name, def.getDescription(), def.getSchema(), wrapped));
    }
    return configured.build();
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
    LOG.debug(
        "Processing event: flow={}, agent={}, type={}",
        inputEvent.getFlowId(),
        inputEvent.getAgentId(),
        inputEvent.getEventType());

    CompletableFuture<ExecutionResult> future = executor.execute(inputEvent);
    inFlight.put(inputEvent, future);

    future.whenComplete(
        (result, error) -> {
          inFlight.remove(inputEvent, future);
          if (future.isCancelled()) {
            cancelled.inc();
            LOG.warn("Agent execution cancelled for flow: {}", inputEvent.getFlowId());
            return;
          }
          if (error != null) {
            LOG.error("Agent execution failed for flow: {}", inputEvent.getFlowId(), error);
            listener.onError(agent.getAgentId(), "execute", error);
            AgentEvent failed =
                failure(inputEvent, error.getMessage(), AgentResultRouter.FAILURE_KIND_ERROR);
            archiveOutcome(inputEvent, failed, null);
            resultFuture.complete(Collections.singleton(failed));

          } else if (result.isSuccess()) {
            LOG.info("Agent execution succeeded for flow: {}", inputEvent.getFlowId());

            AgentEvent successEvent = inputEvent.withEventType(AgentEventType.FLOW_COMPLETED);
            successEvent.incrementIteration();
            successEvent.putMetadata("state", AgentState.COMPLETED.name());
            successEvent.putData("agent_id", agent.getAgentId());
            successEvent.putData("result", result.getOutput());
            successEvent.putData("output", result.getOutput());
            successEvent.putData(
                "tool_calls", result.getToolCalls() != null ? result.getToolCalls().size() : 0);
            successEvent.putData("tool_call_count", result.getEvents().size());
            successEvent.putData("events_generated", result.getEvents().size());
            successEvent.putData("completion_timestamp", System.currentTimeMillis());

            recordConversation(inputEvent, result.getOutput());
            archiveOutcome(inputEvent, successEvent, result);
            resultFuture.complete(Collections.singleton(successEvent));

          } else {
            LOG.warn(
                "Agent execution completed with failure for flow: {}: {}",
                inputEvent.getFlowId(),
                result.getErrorMessage());
            AgentEvent failed =
                failure(
                    inputEvent, result.getErrorMessage(), AgentResultRouter.FAILURE_KIND_EXECUTION);
            attachCompensation(failed, result);
            archiveOutcome(inputEvent, failed, result);
            resultFuture.complete(Collections.singleton(failed));
          }
        });
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.error(
        "Agent execution timed out after {} ms for flow: {}", timeoutMillis, input.getFlowId());
    timeouts.inc();

    CompletableFuture<ExecutionResult> future = inFlight.remove(input);
    if (future != null) {
      future.cancel(true);
    }

    AgentEvent timeoutEvent =
        failure(
            input,
            "Agent execution timed out after " + timeoutMillis + " ms",
            AgentResultRouter.FAILURE_KIND_TIMEOUT);
    resultFuture.complete(Collections.singleton(timeoutEvent));
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

  /**
   * Turns a failed turn into a compensation request: one action per completed tool call whose tool
   * has a compensating tool registered, in call order. {@link CompensationFunction} executes them
   * in reverse. The event is marked {@code COMPENSATING} so {@link AgentResultRouter} routes it to
   * the compensation side output.
   */
  private void attachCompensation(AgentEvent failed, ExecutionResult result) {
    if (!agent.isCompensationEnabled() || agent.getCompensatingTools().isEmpty()) {
      return;
    }
    List<Map<String, Object>> actions = new ArrayList<>();
    for (AgentEvent evt : result.getEvents()) {
      if (evt.getEventType() != AgentEventType.TOOL_CALL_COMPLETED) {
        continue;
      }
      Object toolName = evt.getData("tool_name");
      String compensating =
          toolName == null ? null : agent.getCompensatingTools().get(toolName.toString());
      if (compensating == null) {
        continue;
      }
      Map<String, Object> parameters = new LinkedHashMap<>();
      parameters.put("original_tool", toolName.toString());
      parameters.put("original_result", evt.getData("result"));
      parameters.put("flow_id", failed.getFlowId());
      parameters.put("agent_id", agent.getAgentId());
      Map<String, Object> action = new LinkedHashMap<>();
      action.put("action_name", "undo-" + toolName + "-" + actions.size());
      action.put("tool_name", compensating);
      action.put("parameters", parameters);
      actions.add(action);
    }
    if (actions.isEmpty()) {
      return;
    }
    failed.putMetadata("state", AgentState.COMPENSATING.name());
    failed.putMetadata("is_compensation", true);
    failed.putMetadata(COMPENSATION_ACTIONS_METADATA, actions);
    failed.setCompensationData(null);
  }

  private void recordConversation(AgentEvent input, String output) {
    ConversationStore store = agent.getConversationStore();
    if (store == null) {
      return;
    }
    try {
      String conversationId = input.getFlowId();
      if (input.getUserId() != null) {
        store.associateUser(conversationId, input.getUserId());
      }
      String userMessage = userMessage(input);
      if (userMessage != null) {
        store.append(conversationId, ChatMessage.user(userMessage));
      }
      store.append(conversationId, ChatMessage.assistant(output == null ? "" : output));
    } catch (RuntimeException e) {
      LOG.warn("Conversation store append failed for flow {}", input.getFlowId(), e);
      listener.onError(agent.getAgentId(), "conversation-store", e);
    }
  }

  private void archiveOutcome(AgentEvent input, AgentEvent outcome, ExecutionResult result) {
    LongTermMemoryStore store = agent.getLongTermStore();
    if (store == null) {
      return;
    }
    String summary =
        outcome.getEventType() == AgentEventType.FLOW_COMPLETED
            ? "completed: " + outcome.getData("output")
            : "failed: " + outcome.getErrorMessage();
    ContextItem fact = new ContextItem(summary, ContextPriority.SHOULD, MemoryType.LONG_TERM);
    fact.addMetadata("agent_id", agent.getAgentId());
    fact.addMetadata("event_type", outcome.getEventType().name());
    fact.addMetadata(
        "tool_calls",
        Integer.toString(
            result == null || result.getToolCalls() == null ? 0 : result.getToolCalls().size()));
    String factId = TURN_FACT_PREFIX + outcome.getTimestamp() + ":" + fact.getItemId();
    try {
      store.addFact(input.getFlowId(), factId, fact);
      listener.onLongTermSync(agent.getAgentId(), input.getFlowId(), 1);
    } catch (Exception e) {
      LOG.warn("Long-term store write failed for flow {}", input.getFlowId(), e);
      listener.onError(agent.getAgentId(), "long-term-store", e);
    }
  }

  private static String userMessage(AgentEvent event) {
    Object msg = event.getData("user_message");
    if (msg == null) {
      msg = event.getData("prompt");
    }
    return msg == null ? null : msg.toString();
  }

  /**
   * Wraps a registered tool with the agent's per-tool defaults and tool timeout: defaults are
   * merged under the model-supplied arguments and the call completes exceptionally with a {@link
   * java.util.concurrent.TimeoutException} once the timeout elapses.
   */
  public static final class ConfiguredToolExecutor implements ToolExecutor {
    private static final long serialVersionUID = 1L;

    private final ToolExecutor delegate;
    private final Map<String, Object> defaults;
    private final long timeoutMillis;

    public ConfiguredToolExecutor(
        ToolExecutor delegate, Map<String, Object> defaults, Duration timeout) {
      this.delegate = delegate;
      this.defaults = defaults == null ? Collections.emptyMap() : new HashMap<>(defaults);
      this.timeoutMillis =
          timeout == null || timeout.isZero() || timeout.isNegative() ? 0L : timeout.toMillis();
    }

    public ToolExecutor getDelegate() {
      return delegate;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      Map<String, Object> merged = new HashMap<>(defaults);
      if (parameters != null) {
        merged.putAll(parameters);
      }
      CompletableFuture<Object> call = delegate.execute(merged);
      if (timeoutMillis <= 0) {
        return call;
      }
      return call.orTimeout(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public String getToolId() {
      return delegate.getToolId();
    }

    @Override
    public String getDescription() {
      return delegate.getDescription();
    }
  }
}
