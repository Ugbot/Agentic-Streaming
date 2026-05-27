package org.agentic.flink.stream;

import org.agentic.flink.core.AgentConfig;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.function.*;
import org.agentic.flink.serde.ToolCallRequest;
import org.agentic.flink.serde.ToolCallResponse;
import org.agentic.flink.tools.ToolExecutorRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentExecutionStream {

  private static final Logger LOG = LoggerFactory.getLogger(AgentExecutionStream.class);

  private final StreamExecutionEnvironment env;
  private final AgentConfig config;
  private final Map<String, ToolDefinition> toolRegistry;
  private final ToolExecutorRegistry executorRegistry;

  public AgentExecutionStream(
      StreamExecutionEnvironment env,
      AgentConfig config,
      Map<String, ToolDefinition> toolRegistry,
      ToolExecutorRegistry executorRegistry) {
    this.env = env;
    this.config = config;
    this.toolRegistry = toolRegistry != null ? toolRegistry : new HashMap<>();
    this.executorRegistry = executorRegistry != null ? executorRegistry : new ToolExecutorRegistry();
  }

  /**
   * Creates the complete agent execution stream with: - Tool call execution - Validation -
   * Correction - Supervisor review - Feedback loop
   */
  public SingleOutputStreamOperator<AgentEvent> createAgentStream(
      DataStream<AgentEvent> inputStream) {

    // Step 1: Route events to tool call requests
    DataStream<ToolCallRequest> toolCallRequests =
        inputStream
            .filter(event -> event.getEventType() == AgentEventType.TOOL_CALL_REQUESTED)
            .map(this::eventToToolCallRequest)
            .name("event-to-tool-call-request");

    // Step 2: Execute tool calls asynchronously
    AsyncRetryStrategy<ToolCallResponse> asyncRetryStrategy =
        new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<ToolCallResponse>(3, 1000L)
            .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
            .build();

    DataStream<ToolCallResponse> toolCallResponses =
        AsyncDataStream.orderedWaitWithRetry(
                toolCallRequests,
                new ToolCallAsyncFunctionV2(toolRegistry, executorRegistry),
                30000,
                TimeUnit.MILLISECONDS,
                100,
                asyncRetryStrategy)
            .uid(ToolCallAsyncFunctionV2.UID)
            .name(ToolCallAsyncFunctionV2.UID);

    // Step 3: Convert responses back to events
    DataStream<AgentEvent> toolCallEvents =
        toolCallResponses
            .map(ToolCallAsyncFunctionV2::responseToEvent)
            .name("tool-call-response-to-event");

    // Step 4: Validation
    DataStream<AgentEvent> validationInput =
        toolCallEvents.filter(event -> event.getEventType() == AgentEventType.TOOL_CALL_COMPLETED);

    DataStream<AgentEvent> validationEvents =
        AsyncDataStream.unorderedWait(
                validationInput,
                new ValidationFunction(config.getValidationPrompt()),
                10000,
                TimeUnit.MILLISECONDS,
                100)
            .uid(ValidationFunction.UID)
            .name(ValidationFunction.UID);

    // Step 5: Correction (for failed validation)
    DataStream<AgentEvent> correctionInput =
        validationEvents.filter(event -> event.getEventType() == AgentEventType.VALIDATION_FAILED);

    DataStream<AgentEvent> correctionEvents =
        AsyncDataStream.unorderedWait(
                correctionInput,
                new CorrectionFunction(null, config.getMaxCorrectionAttempts()),
                15000,
                TimeUnit.MILLISECONDS,
                50)
            .uid(CorrectionFunction.UID)
            .name(CorrectionFunction.UID);

    // Step 6: Supervisor review (for escalated cases)
    DataStream<AgentEvent> supervisorInput =
        correctionEvents.filter(
            event -> event.getEventType() == AgentEventType.SUPERVISOR_REVIEW_REQUESTED);

    DataStream<AgentEvent> supervisorEvents =
        supervisorInput
            .process(new SupervisorReviewFunction(true))
            .uid(SupervisorReviewFunction.UID)
            .name(SupervisorReviewFunction.UID);

    // Step 7: Union all pathways
    DataStream<AgentEvent> allEvents =
        validationEvents
            .union(correctionEvents)
            .union(supervisorEvents)
            .union(toolCallEvents.filter(
                event -> event.getEventType() == AgentEventType.TOOL_CALL_FAILED));

    // Step 8: Loop handling with side outputs
    SingleOutputStreamOperator<AgentEvent> loopProcessed =
        allEvents
            .keyBy(AgentEvent::getFlowId)
            .process(new AgentLoopProcessFunction())
            .uid(AgentLoopProcessFunction.UID)
            .name(AgentLoopProcessFunction.UID);

    // Step 9: Get loop output and union back
    DataStream<AgentEvent> loopOutput =
        loopProcessed.getSideOutput(AgentLoopProcessFunction.LOOP_OUTPUT_TAG);

    // Union loop output back with original input (creating feedback cycle)
    DataStream<AgentEvent> feedbackStream = inputStream.union(loopOutput);

    // Return the main output stream
    return loopProcessed;
  }

  private ToolCallRequest eventToToolCallRequest(AgentEvent event) {
    String toolId = event.getData("toolId", String.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> parameters =
        event.getData("parameters", Map.class);
        if (parameters == null) {
      parameters = new HashMap<>();
    }

    return new ToolCallRequest(
        event.getFlowId(), event.getUserId(), event.getAgentId(), toolId, parameters);
  }

  public Map<String, ToolDefinition> getToolRegistry() {
    return toolRegistry;
  }

  public void registerTool(ToolDefinition toolDef) {
    this.toolRegistry.put(toolDef.getToolId(), toolDef);
    LOG.info("Registered tool: {} ({})", toolDef.getName(), toolDef.getToolId());
  }
}
