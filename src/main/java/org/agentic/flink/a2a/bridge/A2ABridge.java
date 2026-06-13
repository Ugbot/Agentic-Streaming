package org.agentic.flink.a2a.bridge;

import java.io.Serializable;
import org.agentic.flink.channel.Channel;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Pluggable transport connecting the Quarkus A2A gateway to a running Flink agent job.
 *
 * <p>The gateway publishes an {@link A2ARequest} for each inbound A2A call; the Flink job consumes
 * it (its agent input stream is {@code union}-ed with {@link #requestChannel()}), runs the agent,
 * and emits {@link A2AResponse}s to {@link #responseSink()}; the gateway receives them through its
 * {@link A2AGatewayConnector} and drives SSE / push back to the external caller. Correlated by A2A
 * {@code taskId}.
 *
 * <p>Implementations are selected by {@code a2a.bridge.transport} via {@link A2ABridgeFactory}:
 * {@code inproc} (embedded / tests), {@code zeromq} (localhost / single host — the default),
 * {@code redis} (distributed-light). The Flink-facing factory ({@link #requestChannel()} /
 * {@link #responseSink()}) is {@link Serializable} so it ships in the job graph; the live transport
 * is built on the task side, per the {@link Channel} convention.
 */
public interface A2ABridge extends Serializable {

  /** Transport name (e.g. {@code "inproc"}, {@code "zeromq"}, {@code "redis"}). */
  String transport();

  /** Flink source emitting requests published by the gateway. Union this into the agent input. */
  Channel<A2ARequest> requestChannel();

  /** Flink sink the job writes agent results/updates to; delivered back to the gateway. */
  SinkFunction<A2AResponse> responseSink();

  /** Open the gateway-side connector (non-Flink side). Call from the gateway process. */
  A2AGatewayConnector openGateway() throws Exception;
}
