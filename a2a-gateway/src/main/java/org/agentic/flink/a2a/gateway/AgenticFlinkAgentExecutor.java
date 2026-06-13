package org.agentic.flink.a2a.gateway;

import io.a2a.server.agentexecution.AgentExecutor;
import io.a2a.server.agentexecution.RequestContext;
import io.a2a.server.tasks.AgentEmitter;
import io.a2a.spec.A2AError;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server-side {@link AgentExecutor} the A2A reference servers (JSON-RPC / gRPC / REST) invoke
 * for every inbound {@code message/send} / {@code message/stream}. It bridges the request into the
 * running Flink job via the {@link A2AGatewayConnector} and pumps the job's responses back into the
 * SDK's {@link AgentEmitter} (which drives SSE + push), using the transport-agnostic {@link
 * A2ARequestBridge}.
 *
 * <p>This single bean backs all three transport bindings — no per-binding duplication.
 */
@ApplicationScoped
public class AgenticFlinkAgentExecutor implements AgentExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(AgenticFlinkAgentExecutor.class);

  @Inject GatewayConfig config;
  @Inject A2AGatewayConnector connector;

  @Override
  public void execute(RequestContext context, AgentEmitter emitter) throws A2AError {
    String taskId = context.getTaskId() != null ? context.getTaskId() : UUID.randomUUID().toString();
    String contextId =
        context.getContextId() != null ? context.getContextId() : UUID.randomUUID().toString();
    A2AMessage message = GatewayMapping.toModel(context.getMessage());

    A2ARequest request =
        new A2ARequest(taskId, contextId, config.agentId(), message, false, null, null);

    LOG.debug("Dispatching A2A task {} (context {}) to Flink job", taskId, contextId);
    try {
      new A2ARequestBridge(connector, config.requestTimeoutMs())
          .run(request, new SdkGatewayEmitter(emitter));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      emitter.updateStatus(
          io.a2a.spec.TaskState.TASK_STATE_FAILED,
          GatewayMapping.agentText("Interrupted while awaiting Flink job"));
    }
  }

  @Override
  public void cancel(RequestContext context, AgentEmitter emitter) throws A2AError {
    // Best-effort: the Flink job is fire-and-forget per request; mark the task canceled.
    LOG.info("Cancel requested for A2A task {}", context.getTaskId());
    emitter.updateStatus(
        io.a2a.spec.TaskState.TASK_STATE_CANCELED, GatewayMapping.agentText("Canceled by caller"));
  }
}
