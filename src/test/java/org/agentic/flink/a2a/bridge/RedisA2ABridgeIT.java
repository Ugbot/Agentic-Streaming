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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for the Redis {@link A2ABridge} against a real Redis (Testcontainers), runs only
 * under {@code -P integration-tests}. The headline assertion is <b>non-lossiness</b>: a request
 * published <em>before</em> the Flink source starts is still delivered (the exact failure mode the
 * old pub/sub transport had, where such a request was silently dropped).
 */
@Tag("integration")
class RedisA2ABridgeIT {

  private static GenericContainer<?> redis;
  private JobClient job;

  @BeforeAll
  static void startRedis() {
    redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    redis.start();
  }

  @AfterAll
  static void stopRedis() {
    if (redis != null) {
      redis.stop();
    }
  }

  @AfterEach
  void cancelJob() throws Exception {
    if (job != null) {
      job.cancel().get();
      job = null;
    }
  }

  private RedisA2ABridge bridge() {
    return new RedisA2ABridge(
        redis.getHost(),
        redis.getMappedPort(6379),
        "a2a:it:req:" + UUID.randomUUID(),
        "a2a:it:resp:" + UUID.randomUUID());
  }

  @Test
  @DisplayName("redis bridge is non-lossy: a request published BEFORE the source starts is delivered")
  void nonLossyPrePublish() throws Exception {
    RedisA2ABridge bridge = bridge();

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      String taskId = UUID.randomUUID().toString();
      String prompt = "compute-" + UUID.randomUUID();
      A2ARequest req =
          new A2ARequest(
              taskId,
              "ctx-it",
              "agent",
              A2AMessage.userText(UUID.randomUUID().toString(), prompt),
              false,
              null,
              null);

      // Publish FIRST — the Flink source/job does not exist yet. With pub/sub this request would be
      // dropped; with a Redis list it waits in the queue.
      connector.publishRequest(req);

      // NOW start the job that consumes requests and echoes responses.
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
      DataStream<A2ARequest> requests = bridge.requestChannel().open(env);
      requests
          .map(new EchoResponder())
          .returns(A2AJsonTypeInfo.of(A2AResponse.class))
          .sinkTo(bridge.responseSink());
      job = env.executeAsync("redis-bridge-nonlossy");

      A2AResponse response = connector.awaitFinal(taskId, 25_000);
      assertNotNull(response, "pre-published request must NOT be lost (was dropped under pub/sub)");
      assertEquals(A2ATaskState.COMPLETED, response.getState());
      assertEquals(taskId, response.getTaskId());
      assertTrue(response.getArtifacts().get(0).textContent().contains(prompt));
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
