package org.agentic.flink.a2a.gateway;

import io.a2a.server.tasks.AgentEmitter;
import io.a2a.spec.TaskState;
import java.util.List;
import org.agentic.flink.a2a.A2APart;

/**
 * Production {@link GatewayEmitter} that forwards to the A2A SDK's {@link AgentEmitter}, which in
 * turn feeds the server's event queue (driving SSE streaming and push notifications).
 */
final class SdkGatewayEmitter implements GatewayEmitter {

  private final AgentEmitter delegate;

  SdkGatewayEmitter(AgentEmitter delegate) {
    this.delegate = delegate;
  }

  @Override
  public void working(String statusMessage) {
    delegate.updateStatus(
        TaskState.TASK_STATE_WORKING,
        statusMessage == null ? null : GatewayMapping.agentText(statusMessage));
  }

  @Override
  public void artifact(String name, List<A2APart> parts) {
    delegate.addArtifact(GatewayMapping.toSdkParts(parts), name, null, null);
  }

  @Override
  public void complete() {
    delegate.complete();
  }

  @Override
  public void fail(String message) {
    delegate.updateStatus(
        TaskState.TASK_STATE_FAILED,
        message == null ? null : GatewayMapping.agentText(message));
  }

  @Override
  public void inputRequired(String message) {
    delegate.updateStatus(
        TaskState.TASK_STATE_INPUT_REQUIRED,
        message == null ? null : GatewayMapping.agentText(message));
  }
}
