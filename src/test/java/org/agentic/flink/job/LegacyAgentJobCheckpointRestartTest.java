package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.agentic.flink.channel.sink.ForEachSink;
import org.agentic.flink.channel.source.PollingSource;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Legacy DSL job (CEP dispatch, async brain, result router) built by {@link AgentJobGenerator} on a
 * MiniCluster: one turn completes, a checkpoint is taken, the source task is killed and the job
 * restarts from that checkpoint. After the restart the source resumes at the checkpointed position,
 * a replayed copy of the completed turn is dropped by the restored dedup state so the brain is not
 * called again for it, and the next turn of the same conversation runs once.
 */
final class LegacyAgentJobCheckpointRestartTest {

  private static final Map<String, List<AgentEvent>> EVENTS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicBoolean> KILL = new ConcurrentHashMap<>();
  private static final Map<String, List<String>> SEEKS = new ConcurrentHashMap<>();
  private static final Map<String, Map<String, AtomicInteger>> BRAIN_CALLS =
      new ConcurrentHashMap<>();
  private static final Map<String, List<AgentEvent>> RESULTS = new ConcurrentHashMap<>();

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
  }

  @Test
  void completedTurnSurvivesRestartWithoutReinvokingTheBrain(@TempDir Path dir) throws Exception {
    String id = UUID.randomUUID().toString();
    EVENTS.put(id, new CopyOnWriteArrayList<>());
    KILL.put(id, new AtomicBoolean(false));
    SEEKS.put(id, new CopyOnWriteArrayList<>());
    BRAIN_CALLS.put(id, new ConcurrentHashMap<>());
    RESULTS.put(id, new CopyOnWriteArrayList<>());

    String agentId = "legacy-" + id;
    String flowId = "flow-" + UUID.randomUUID();
    Agent agent =
        Agent.builder()
            .withId(agentId)
            .withSystemPrompt("sys " + UUID.randomUUID())
            .withStateMachine(stateMachine())
            .withChatConnection(new CountingBrain(id))
            .withMaxIterations(3)
            .build();
    AgentJob agentJob =
        AgentJob.builder()
            .withId("job-" + id)
            .withAgent(agent)
            .withJobDefaults(
                FlinkJobDefaults.fromConfig(AgenticFlinkConfig.forTesting())
                    .withInterval(Duration.ofHours(1))
                    .withStorageDir(dir.resolve("cp").toUri().toString()))
            .build();

    Configuration conf = new Configuration();
    conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
    StreamExecutionEnvironment env =
        new TestStreamEnvironment(cluster, conf, 1, List.of(), List.of());
    env.setParallelism(1);
    DataStream<AgentEvent> input =
        env.fromSource(
            new PollingSource<>(new ScriptedEvents(id)),
            WatermarkStrategy.noWatermarks(),
            "turns",
            TypeInformation.of(AgentEvent.class));
    AgentJobGenerator.create(env, agentJob)
        .generate(input)
        .sinkTo(new ForEachSink<>(new ResultWriteFn(id)))
        .name("results");
    job = env.executeAsync("legacy-restart-" + id);
    awaitRunning();

    String turn1 = "t1-" + UUID.randomUUID();
    String turn2 = "t2-" + UUID.randomUUID();
    long[] clock = {System.currentTimeMillis()};

    emitTurn(id, flowId, agentId, turn1, clock);
    await(
        () -> completed(id, turn1) == 1,
        () ->
            "turn 1 completed; brain="
                + BRAIN_CALLS.get(id)
                + " results="
                + RESULTS.get(id)
                + " seeks="
                + SEEKS.get(id)
                + "; "
                + failureInfo());
    assertEquals(1, brainCalls(id, turn1));
    int eventsBeforeCheckpoint = EVENTS.get(id).size();
    assertEquals(List.of("<start>"), SEEKS.get(id), "fresh start, no restore yet");

    cluster.triggerCheckpoint(job.getJobID()).get(60, TimeUnit.SECONDS);

    KILL.get(id).set(true);
    await(() -> !KILL.get(id).get(), "source task killed");
    await(
        () -> SEEKS.get(id).size() >= 2,
        () -> "restarted source restored its split; " + failureInfo());
    awaitRunning();
    assertEquals(
        List.of("<start>", String.valueOf(eventsBeforeCheckpoint)),
        SEEKS.get(id),
        "the source resumes right after the events covered by the checkpoint");

    emitTurn(id, flowId, agentId, turn1, clock);
    emitTurn(id, flowId, agentId, turn2, clock);
    await(() -> completed(id, turn2) == 1, "turn 2 completed after the restart; " + failureInfo());

    assertEquals(1, brainCalls(id, turn1), "brain not re-invoked for the completed turn");
    assertEquals(1, brainCalls(id, turn2), "brain invoked once for the new turn");
    assertEquals(1, completed(id, turn1), "the completed turn is delivered exactly once");
    assertEquals(2, RESULTS.get(id).size(), "exactly one result per turn");
    assertTrue(RESULTS.get(id).stream().allMatch(e -> flowId.equals(e.getFlowId())));
    assertEquals(turn1, turnOf(RESULTS.get(id).get(0)));
    assertEquals(turn2, turnOf(RESULTS.get(id).get(1)));
    assertEquals(JobStatus.RUNNING, job.getJobStatus().get(10, TimeUnit.SECONDS));
  }

  private static void emitTurn(
      String id, String flowId, String agentId, String turnId, long[] clock) {
    EVENTS.get(id).add(event(flowId, agentId, turnId, AgentEventType.FLOW_STARTED, ++clock[0]));
    EVENTS
        .get(id)
        .add(event(flowId, agentId, turnId, AgentEventType.LOOP_ITERATION_STARTED, ++clock[0]));
    EVENTS.get(id).add(event(flowId, agentId, turnId, AgentEventType.FLOW_COMPLETED, ++clock[0]));
    // Advances the event-time watermark past the terminal event so the CEP match is emitted.
    clock[0] += 1_000;
    EVENTS
        .get(id)
        .add(
            event(
                "nudge-" + flowId, agentId, turnId, AgentEventType.USER_INPUT_RECEIVED, clock[0]));
  }

  private static AgentEvent event(
      String flowId, String agentId, String turnId, AgentEventType type, long timestamp) {
    AgentEvent e = new AgentEvent(flowId, "user", agentId, type);
    e.setTimestamp(timestamp);
    e.putData("turn_id", turnId);
    e.putData("user_message", "turn " + turnId);
    return e;
  }

  private static String turnOf(AgentEvent e) {
    return String.valueOf(e.getData("turn_id"));
  }

  private static long completed(String id, String turnId) {
    return RESULTS.get(id).stream()
        .filter(e -> turnId.equals(turnOf(e)))
        .filter(e -> AgentState.COMPLETED.name().equals(e.getMetadata("state")))
        .count();
  }

  private static int brainCalls(String id, String turnId) {
    AtomicInteger n = BRAIN_CALLS.get(id).get(turnId);
    return n == null ? 0 : n.get();
  }

  private void awaitRunning() throws Exception {
    await(
        () -> {
          try {
            return job.getJobStatus().get() == JobStatus.RUNNING;
          } catch (Exception e) {
            return false;
          }
        },
        "job running; " + failureInfo());
  }

  private String failureInfo() {
    try {
      ErrorInfo info = cluster.getArchivedExecutionGraph(job.getJobID()).get().getFailureInfo();
      return info == null ? "job has no failure" : info.getExceptionAsString();
    } catch (Exception e) {
      return "failure info unavailable: " + e;
    }
  }

  private static void await(BooleanSupplier condition, String what) throws InterruptedException {
    await(condition, () -> what);
  }

  private static void await(BooleanSupplier condition, Supplier<String> what)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what.get());
      }
      Thread.sleep(20);
    }
  }

  static AgentStateMachine stateMachine() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withInitialState(AgentState.INITIALIZED);
    b.addTransition(t(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED));
    b.addTransition(t(AgentState.EXECUTING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(
        t(AgentState.CORRECTING, AgentState.COMPLETED, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(
        t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(t(AgentState.PAUSED, AgentState.COMPLETED, AgentEventType.FLOW_RESUMED));
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition t(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }

  /** Scripted event log with an index cursor; {@code KILL} makes the next poll fail the task. */
  static final class ScriptedEvents implements PollingSource.PositionedPollFn<AgentEvent> {
    private static final long serialVersionUID = 1L;
    private final String id;
    private int next;

    ScriptedEvents(String id) {
      this.id = id;
    }

    @Override
    public void seek(String position) {
      SEEKS.get(id).add(position == null ? "<start>" : position);
      next = position == null ? 0 : Integer.parseInt(position);
    }

    @Override
    public String position() {
      return String.valueOf(next);
    }

    @Override
    public AgentEvent poll(long timeoutMs) throws InterruptedException {
      if (KILL.get(id).getAndSet(false)) {
        throw new IllegalStateException("killed by test " + id);
      }
      List<AgentEvent> events = EVENTS.get(id);
      if (next < events.size()) {
        return events.get(next++);
      }
      Thread.sleep(Math.min(20, Math.max(1, timeoutMs)));
      return null;
    }
  }

  /** Brain that counts invocations per turn (the turn id is in the user message). */
  static final class CountingBrain implements ChatConnection {
    private static final long serialVersionUID = 1L;
    private final String id;

    CountingBrain(String id) {
      this.id = id;
    }

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          String turn = null;
          for (ChatMessage m : messages) {
            if (m.getRole() == ChatRole.USER && m.getContent().startsWith("turn ")) {
              turn = m.getContent().substring("turn ".length());
            }
          }
          BRAIN_CALLS
              .get(id)
              .computeIfAbsent(String.valueOf(turn), t -> new AtomicInteger())
              .incrementAndGet();
          return new ChatResponse(
              "answer for " + turn,
              setup.getModelName(),
              List.of(),
              1L,
              ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "counting";
        }
      };
    }

    @Override
    public String providerName() {
      return "counting";
    }
  }

  static final class ResultWriteFn implements ForEachSink.WriteFn<AgentEvent> {
    private static final long serialVersionUID = 1L;
    private final String id;

    ResultWriteFn(String id) {
      this.id = id;
    }

    @Override
    public void write(AgentEvent event) {
      RESULTS.get(id).add(event);
    }
  }
}
