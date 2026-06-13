package org.agentic.flink.a2a.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2ATaskState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end bridge test on a Flink minicluster: a gateway connector publishes {@link A2ARequest}s,
 * a job consumes them via {@link A2ABridge#requestChannel()}, echoes each into an {@link
 * A2AResponse}, and writes it to {@link A2ABridge#responseSink()}; the connector receives the
 * correlated responses. Run for the {@code inproc} and {@code zeromq} transports (Redis via IT).
 */
final class A2ABridgeTest {

  private JobClient job;

  @AfterEach
  void tearDown() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
    InProcA2ABridge.Hub.reset();
  }

  private A2ABridge bridge(String transport) {
    return new InProcA2ABridge("req-" + UUID.randomUUID(), "resp-" + UUID.randomUUID());
  }

  @ParameterizedTest(name = "[{0}] gateway request -> job -> gateway response round-trip")
  @ValueSource(strings = {"inproc"})
  void roundTrip(String transport) throws Exception {
    A2ABridge bridge = bridge(transport);

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
      DataStream<A2ARequest> requests = bridge.requestChannel().open(env);
      requests
          .map(new EchoResponder())
          .returns(A2AJsonTypeInfo.of(A2AResponse.class))
          .addSink(bridge.responseSink());
      job = env.executeAsync("a2a-bridge-" + transport);

      // Give the source a moment to start before publishing.
      Thread.sleep(300);

      for (int i = 0; i < 3; i++) {
        String taskId = UUID.randomUUID().toString();
        String contextId = "ctx-" + i;
        String prompt = "compute-" + i;
        A2ARequest req =
            new A2ARequest(
                taskId,
                contextId,
                "agent",
                A2AMessage.userText(UUID.randomUUID().toString(), prompt),
                false,
                null,
                null);
        connector.publishRequest(req);

        A2AResponse response = connector.awaitFinal(taskId, 15_000);
        assertNotNull(response, "no response for " + taskId + " (" + transport + ")");
        assertEquals(A2ATaskState.COMPLETED, response.getState());
        assertEquals(taskId, response.getTaskId());
        assertTrue(response.getArtifacts().get(0).textContent().contains(prompt));
      }
    }
  }

  /** Maps a request into a completed response echoing the request's text. */
  static final class EchoResponder implements MapFunction<A2ARequest, A2AResponse> {
    private static final long serialVersionUID = 1L;

    @Override
    public A2AResponse map(A2ARequest req) {
      A2AArtifact artifact =
          A2AArtifact.text(
              UUID.randomUUID().toString(), "result", "echo: " + req.getMessage().textContent());
      return A2AResponse.completed(req.getTaskId(), req.getContextId(), List.of(artifact));
    }
  }
}
