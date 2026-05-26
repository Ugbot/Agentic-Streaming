package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorReviewFunction extends ProcessFunction<AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SupervisorReviewFunction.class);
  public static final String UID = SupervisorReviewFunction.class.getSimpleName();

  private final boolean autoApprove;

  public SupervisorReviewFunction(boolean autoApprove) {
    this.autoApprove = autoApprove;
  }

  @Override
  public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out) {

    LOG.info("Supervisor reviewing flow: {}", event.getFlowId());

    // In a real implementation, this would:
    // 1. Send notification to human supervisor
    // 2. Store review request in state/database
    // 3. Wait for approval (could use async pattern or separate stream)
    //
    // For now, we'll implement a simple auto-approve logic

    if (autoApprove) {
      LOG.info("Auto-approving supervisor review for flow: {}", event.getFlowId());
      AgentEvent approvalEvent = createApprovalEvent(event, true, "Auto-approved");
      out.collect(approvalEvent);
    } else {
      // In production, this would emit to a side output for manual review
      // and be re-injected into the stream after human approval
      LOG.info(
          "Manual supervisor review required for flow: {} - would wait for human input",
          event.getFlowId());

      // For now, auto-reject to show the flow
      AgentEvent rejectionEvent = createApprovalEvent(event, false, "Manual review required");
      out.collect(rejectionEvent);
    }
  }

  private AgentEvent createApprovalEvent(AgentEvent originalEvent, boolean approved, String reason) {
    AgentEvent event = new AgentEvent();
    event.setFlowId(originalEvent.getFlowId());
    event.setUserId(originalEvent.getUserId());
    event.setAgentId(originalEvent.getAgentId());
    event.setEventType(
        approved ? AgentEventType.SUPERVISOR_APPROVED : AgentEventType.SUPERVISOR_REJECTED);
    event.setTimestamp(System.currentTimeMillis());
    event.setCurrentStage(originalEvent.getCurrentStage());
    event.setIterationNumber(originalEvent.getIterationNumber());
    event.putData("approvalReason", reason);
    event.putData("originalResult", originalEvent.getData("originalResult"));
    return event;
  }
}
