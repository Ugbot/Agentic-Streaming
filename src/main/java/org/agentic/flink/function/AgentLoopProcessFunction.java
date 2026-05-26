package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.AgentExecutionState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentLoopProcessFunction extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(AgentLoopProcessFunction.class);
  public static final String UID = AgentLoopProcessFunction.class.getSimpleName();

  // Side output for looping back
  public static final OutputTag<AgentEvent> LOOP_OUTPUT_TAG =
      new OutputTag<AgentEvent>("loop-output") {};

  private ValueState<AgentExecutionState> executionState;

  @Override
  public void open(OpenContext openContext) {
    ValueStateDescriptor<AgentExecutionState> stateDescriptor =
        new ValueStateDescriptor<>("agentExecutionState", AgentExecutionState.class);
    executionState = getRuntimeContext().getState(stateDescriptor);
  }

  @Override
  public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out)
      throws Exception {

    String flowId = event.getFlowId();
    AgentExecutionState state = executionState.value();

    // Initialize state if not exists
    if (state == null) {
      state = new AgentExecutionState(flowId, event.getUserId(), event.getAgentId());
      state.setMaxIterations(10); // default
    }

    // Update state based on event
    state.updateLastActive();
    state.setCurrentStage(event.getCurrentStage());

    // Check if this is a loop trigger event
    if (shouldLoop(event, state)) {
      state.incrementIteration();

      if (state.hasReachedMaxIterations()) {
        LOG.warn(
            "Flow {} has reached max iterations ({}), completing",
            flowId,
            state.getMaxIterations());

        AgentEvent completionEvent = createCompletionEvent(event, "Max iterations reached");
        out.collect(completionEvent);

        // Clean up state
        executionState.clear();
      } else {
        LOG.info(
            "Flow {} looping back for iteration {}/{}",
            flowId,
            state.getCurrentIteration(),
            state.getMaxIterations());

        // Create loop event
        AgentEvent loopEvent = createLoopEvent(event, state.getCurrentIteration());

        // Send to loop output (will be re-injected to main stream)
        ctx.output(LOOP_OUTPUT_TAG, loopEvent);

        // Update state
        executionState.update(state);
      }
    } else if (isCompletionEvent(event)) {
      LOG.info("Flow {} completed successfully", flowId);
      out.collect(event);

      // Clean up state
      executionState.clear();
    } else {
      // Pass through
      out.collect(event);
      executionState.update(state);
    }
  }

  private boolean shouldLoop(AgentEvent event, AgentExecutionState state) {
    // Loop if:
    // 1. Correction was completed (needs re-validation)
    // 2. Validation failed and auto-correction is enabled
    // 3. Explicit loop iteration event

    return event.getEventType() == AgentEventType.CORRECTION_COMPLETED
        || event.getEventType() == AgentEventType.LOOP_ITERATION_STARTED
        || (event.getEventType() == AgentEventType.VALIDATION_FAILED
            && !state.hasReachedMaxIterations());
  }

  private boolean isCompletionEvent(AgentEvent event) {
    return event.getEventType() == AgentEventType.FLOW_COMPLETED
        || event.getEventType() == AgentEventType.VALIDATION_PASSED
        || event.getEventType() == AgentEventType.SUPERVISOR_APPROVED;
  }

  private AgentEvent createLoopEvent(AgentEvent originalEvent, int iterationNumber) {
    AgentEvent loopEvent = new AgentEvent();
    loopEvent.setFlowId(originalEvent.getFlowId());
    loopEvent.setUserId(originalEvent.getUserId());
    loopEvent.setAgentId(originalEvent.getAgentId());
    loopEvent.setEventType(AgentEventType.LOOP_ITERATION_STARTED);
    loopEvent.setTimestamp(System.currentTimeMillis());
    loopEvent.setIterationNumber(iterationNumber);
    loopEvent.setCurrentStage("VALIDATION"); // Loop back to validation stage
    loopEvent.putData("loopReason", "Correction completed, re-validating");
    loopEvent.putData("result", originalEvent.getData("correctedResult"));
    return loopEvent;
  }

  private AgentEvent createCompletionEvent(AgentEvent originalEvent, String reason) {
    AgentEvent completionEvent = new AgentEvent();
    completionEvent.setFlowId(originalEvent.getFlowId());
    completionEvent.setUserId(originalEvent.getUserId());
    completionEvent.setAgentId(originalEvent.getAgentId());
    completionEvent.setEventType(AgentEventType.FLOW_COMPLETED);
    completionEvent.setTimestamp(System.currentTimeMillis());
    completionEvent.putData("completionReason", reason);
    completionEvent.putData("finalResult", originalEvent.getData("result"));
    return completionEvent;
  }
}
