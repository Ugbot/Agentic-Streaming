package org.agentic.flink.a2a.gateway;

import java.util.List;
import org.agentic.flink.a2a.A2APart;

/**
 * Sink for A2A task events the gateway produces while servicing one request, abstracting the A2A
 * SDK's {@code AgentEmitter}.
 *
 * <p>Keeping the bridge logic ({@link A2ARequestBridge}) against this interface — rather than the
 * SDK type directly — makes it unit-testable with a fake emitter (no Quarkus / event-queue boot).
 * {@link SdkGatewayEmitter} is the production binding.
 */
public interface GatewayEmitter {

  /** Report an intermediate {@code working} status update. */
  void working(String statusMessage);

  /** Emit a produced artifact (its parts) under the given name. */
  void artifact(String name, List<A2APart> parts);

  /** Mark the task completed (terminal). */
  void complete();

  /** Mark the task failed (terminal) with a message. */
  void fail(String message);

  /** Mark the task as needing more caller input (interrupted, resumable). */
  void inputRequired(String message);
}
