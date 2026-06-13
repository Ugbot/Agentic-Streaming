package org.agentic.flink.a2a.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;
import org.agentic.flink.a2a.bridge.A2AGatewayConnector;
import org.agentic.flink.a2a.bridge.A2AJsonTypeInfo;
import org.agentic.flink.a2a.bridge.A2ARequest;
import org.agentic.flink.a2a.bridge.A2AResponse;
import org.agentic.flink.a2a.bridge.InProcA2ABridge;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exercises the gateway's core bridging ({@link A2ARequestBridge} → {@link GatewayEmitter}) against
 * a real Flink job over the in-process bridge — no Quarkus/SDK boot. Proves an inbound A2A request
 * reaches the job and the job's response drives the expected emitter calls.
 */
final class A2ARequestBridgeTest {

  private JobClient job;

  @AfterEach
  void tearDown() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
    InProcA2ABridge.Hub.reset();
  }

  @Test
  @DisplayName("inbound request -> Flink job -> emitter receives artifact + complete")
  void completedFlow() throws Exception {
    InProcA2ABridge bridge =
        new InProcA2ABridge("gw-req-" + UUID.randomUUID(), "gw-resp-" + UUID.randomUUID());

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
      DataStream<A2ARequest> requests = bridge.requestChannel().open(env);
      requests
          .map(new EchoResponder())
          .returns(A2AJsonTypeInfo.of(A2AResponse.class))
          .sinkTo(bridge.responseSink());
      job = env.executeAsync("gateway-bridge-test");
      Thread.sleep(300);

      String taskId = UUID.randomUUID().toString();
      String prompt = "analyze-" + UUID.randomUUID();
      A2ARequest request =
          new A2ARequest(
              taskId,
              "ctx",
              "agentic-flink",
              A2AMessage.userText(UUID.randomUUID().toString(), prompt),
              false,
              null,
              null);

      RecordingEmitter emitter = new RecordingEmitter();
      boolean finished = new A2ARequestBridge(connector, 15_000).run(request, emitter);

      assertTrue(finished, "bridge did not finish");
      assertTrue(emitter.completed, "expected complete()");
      assertFalse(emitter.failed, "did not expect fail()");
      assertEquals(1, emitter.artifacts.size());
      String text = emitter.artifacts.get(0).get(0).getText();
      assertTrue(text.contains(prompt), "artifact text missing prompt: " + text);
    }
  }

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

  static final class RecordingEmitter implements GatewayEmitter {
    final List<List<A2APart>> artifacts = new CopyOnWriteArrayList<>();
    final List<String> working = new ArrayList<>();
    volatile boolean completed;
    volatile boolean failed;
    volatile String inputRequired;

    @Override
    public void working(String statusMessage) {
      working.add(statusMessage);
    }

    @Override
    public void artifact(String name, List<A2APart> parts) {
      artifacts.add(parts);
    }

    @Override
    public void complete() {
      completed = true;
    }

    @Override
    public void fail(String message) {
      failed = true;
    }

    @Override
    public void inputRequired(String message) {
      inputRequired = message;
    }
  }
}
