package org.agentic.flink.a2a.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for savepoint restore of an A2A bridge stream. A keyed operator remembers the
 * previous {@link A2ARequest} of every context in Flink state typed with {@link A2AJsonTypeInfo};
 * the job is stopped with a savepoint and restarted from it. Restoring that state used to fail
 * because the serializer snapshot carried no element class, so the restored serializer was built
 * with a null type.
 */
final class A2ABridgeSavepointRestoreTest {

  private MiniCluster cluster;
  private JobClient job;

  @BeforeEach
  void setUp() throws Exception {
    cluster = TestClusters.start(2);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (job != null && !job.getJobStatus().get().isGloballyTerminalState()) {
      job.cancel().get();
    }
    if (cluster != null) {
      cluster.close();
    }
    InProcA2ABridge.Hub.reset();
  }

  @Test
  @DisplayName(
      "stop-with-savepoint then restore keeps per-context request state typed with A2AJsonTypeInfo")
  void savepointRestoreKeepsKeyedA2AState(@TempDir Path dir) throws Exception {
    A2ABridge bridge = new InProcA2ABridge("req-" + UUID.randomUUID(), "resp-" + UUID.randomUUID());
    String contextId = "ctx-" + UUID.randomUUID();
    int before = 2 + new java.util.Random().nextInt(3);

    try (A2AGatewayConnector connector = bridge.openGateway()) {
      job = start(bridge, dir, null);
      String previousTask = null;
      for (int i = 0; i < before; i++) {
        String taskId = UUID.randomUUID().toString();
        connector.publishRequest(request(taskId, contextId, "turn-" + i));
        A2AResponse response = connector.awaitFinal(taskId, 30_000);
        assertNotNull(response, "no response for turn " + i);
        assertEquals(A2ATaskState.COMPLETED, response.getState());
        assertEquals((i + 1) + ":" + previousTask, response.getArtifacts().get(0).textContent());
        previousTask = taskId;
      }

      String savepoint =
          job.stopWithSavepoint(
                  false, dir.resolve("sp").toUri().toString(), SavepointFormatType.CANONICAL)
              .get(60, TimeUnit.SECONDS);
      job = start(bridge, dir, savepoint);

      String taskId = UUID.randomUUID().toString();
      connector.publishRequest(request(taskId, contextId, "after-restore"));
      A2AResponse response = connector.awaitFinal(taskId, 30_000);
      assertNotNull(response, () -> "no response after restore; job failure: " + failureInfo());
      assertEquals(A2ATaskState.COMPLETED, response.getState());
      assertEquals((before + 1) + ":" + previousTask, response.getArtifacts().get(0).textContent());
    }
  }

  private String failureInfo() {
    try {
      ErrorInfo info = cluster.getArchivedExecutionGraph(job.getJobID()).get().getFailureInfo();
      return info == null ? "none" : info.getExceptionAsString();
    } catch (Exception e) {
      return "unavailable: " + e;
    }
  }

  private static A2ARequest request(String taskId, String contextId, String text) {
    return new A2ARequest(
        taskId,
        contextId,
        "agent",
        A2AMessage.userText(UUID.randomUUID().toString(), text),
        false,
        null,
        null);
  }

  private JobClient start(A2ABridge bridge, Path dir, String savepoint) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dir.resolve("cp").toUri().toString());
    if (savepoint != null) {
      conf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    }
    StreamExecutionEnvironment env =
        new TestStreamEnvironment(cluster, conf, 1, List.of(), List.of());
    env.setParallelism(1);
    bridge
        .requestChannel()
        .open(env)
        .keyBy(A2ARequest::key)
        .process(new RememberPreviousRequest())
        .uid("remember-previous")
        .returns(A2AJsonTypeInfo.of(A2AResponse.class))
        .sinkTo(bridge.responseSink())
        .uid("response-sink");
    JobClient client = env.executeAsync("a2a-savepoint-" + UUID.randomUUID());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (client.getJobStatus().get() != JobStatus.RUNNING) {
      if (client.getJobStatus().get().isGloballyTerminalState()) {
        client.getJobExecutionResult().get();
        throw new AssertionError("job terminated before RUNNING");
      }
      if (System.nanoTime() > deadline) {
        throw new AssertionError("job did not reach RUNNING");
      }
      Thread.sleep(20);
    }
    return client;
  }

  /** Keeps the previous request and the turn count per context in state and echoes both. */
  static final class RememberPreviousRequest
      extends KeyedProcessFunction<String, A2ARequest, A2AResponse> {
    private static final long serialVersionUID = 1L;
    private transient ValueState<A2ARequest> previous;
    private transient ValueState<Integer> turns;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
      previous =
          getRuntimeContext()
              .getState(
                  new ValueStateDescriptor<>(
                      "previous-request", A2AJsonTypeInfo.of(A2ARequest.class)));
      turns = getRuntimeContext().getState(new ValueStateDescriptor<>("turns", Integer.class));
    }

    @Override
    public void processElement(A2ARequest req, Context ctx, Collector<A2AResponse> out)
        throws Exception {
      int turn = (turns.value() == null ? 0 : turns.value()) + 1;
      A2ARequest prev = previous.value();
      turns.update(turn);
      previous.update(req);
      String text = turn + ":" + (prev == null ? null : prev.getTaskId());
      out.collect(
          A2AResponse.completed(
              req.getTaskId(),
              req.getContextId(),
              List.of(A2AArtifact.text(UUID.randomUUID().toString(), "result", text))));
    }
  }
}
