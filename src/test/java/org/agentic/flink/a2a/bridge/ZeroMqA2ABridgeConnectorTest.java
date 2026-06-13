package org.agentic.flink.a2a.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AJson;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2ATaskState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Deterministic wire test for {@link ZeroMqA2ABridge}'s gateway connector against raw ZMQ sockets
 * that play the Flink side: a PULL bound on the request endpoint (what {@code ZeroMqChannel.pull}
 * does) and a PUSH connected to the response endpoint (what {@code ZeroMqSink.push} does). Verifies
 * the socket topology and that envelopes round-trip through the shared {@link A2AJson} codec.
 */
final class ZeroMqA2ABridgeConnectorTest {

  @Test
  @DisplayName("zeromq connector: publish reaches a bound PULL; a pushed response is delivered")
  void wireRoundTrip() throws Exception {
    int base = 5400 + (int) (System.nanoTime() % 1000) * 2;
    String requestEndpoint = "tcp://127.0.0.1:" + base;
    String responseEndpoint = "tcp://127.0.0.1:" + (base + 1);

    ZeroMqA2ABridge bridge = new ZeroMqA2ABridge(requestEndpoint, responseEndpoint);

    try (ZContext ctx = new ZContext();
        A2AGatewayConnector connector = bridge.openGateway()) {
      // Flink-side PULL for requests (binds) and PUSH for responses (connects).
      ZMQ.Socket flinkRequests = ctx.createSocket(SocketType.PULL);
      flinkRequests.setReceiveTimeOut(5000);
      flinkRequests.bind(requestEndpoint);
      ZMQ.Socket flinkResponses = ctx.createSocket(SocketType.PUSH);
      flinkResponses.connect(responseEndpoint);

      Thread.sleep(300); // let connect/bind settle

      String taskId = UUID.randomUUID().toString();
      String prompt = "do-" + UUID.randomUUID();
      connector.publishRequest(
          new A2ARequest(
              taskId,
              "ctx",
              "agent",
              A2AMessage.userText(UUID.randomUUID().toString(), prompt),
              false,
              null,
              null));

      // The Flink side receives the request off the bound PULL.
      byte[] reqBytes = flinkRequests.recv(0);
      assertNotNull(reqBytes, "request not received on PULL");
      A2ARequest received = A2AJson.mapper().readValue(reqBytes, A2ARequest.class);
      assertEquals(taskId, received.getTaskId());
      assertTrue(received.getMessage().textContent().contains(prompt));

      // The Flink side pushes a response; the connector delivers it via awaitFinal.
      A2AArtifact artifact = A2AArtifact.text(UUID.randomUUID().toString(), "r", "echo: " + prompt);
      flinkResponses.send(
          A2AJson.mapper()
              .writeValueAsBytes(A2AResponse.completed(taskId, "ctx", List.of(artifact))),
          0);

      A2AResponse response = connector.awaitFinal(taskId, 5000);
      assertNotNull(response, "response not delivered to connector");
      assertEquals(A2ATaskState.COMPLETED, response.getState());
      assertTrue(response.getArtifacts().get(0).textContent().contains(prompt));
    }
  }
}
