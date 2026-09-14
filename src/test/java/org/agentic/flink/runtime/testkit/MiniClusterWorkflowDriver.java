package org.agentic.flink.runtime.testkit;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.agentic.flink.pipeline.FlinkPipelineRunner;
import org.agentic.flink.runtime.FlinkRuntimeOptions;
import org.agentic.flink.runtime.WorkflowTurnFunction;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;

/**
 * Drives one workflow job on a running {@link MiniCluster} the way a fixture drives a runtime:
 * submit a turn, wait for its normalized result, restart the job from a savepoint, and observe
 * timer-driven results.
 *
 * <p>Events enter through an unbounded {@link DataGeneratorSource} that polls a per-driver in-JVM
 * queue (the source emits a filtered heartbeat while the queue is empty, so checkpoints and
 * stop-with-savepoint keep flowing); results leave through an in-JVM sink. Both registries are
 * static because MiniCluster tasks share the test JVM. Events already pulled from the queue but not
 * yet checkpointed are lost on failure, like any at-most-once source; the tests only fail the job
 * when nothing but the poison marker is in flight.
 *
 * <p>The source chain (generator, poison map, heartbeat filter) always runs at parallelism 1 so
 * events reach {@code keyBy(conversationId)} in submission order; the keyed workflow operator and
 * the sink run at the driver's {@code parallelism}, so distinct conversations execute on distinct
 * subtasks concurrently while each conversation stays on one subtask.
 */
public final class MiniClusterWorkflowDriver implements AutoCloseable {

  static final String HEARTBEAT = "__heartbeat__";
  /** Metadata key whose presence makes {@link FailOnce} throw exactly once per job incarnation. */
  public static final String POISON = "x-fail-once";

  private static final Map<String, ConcurrentLinkedQueue<Event>> INPUTS = new ConcurrentHashMap<>();
  private static final Map<String, CopyOnWriteArrayList<TurnResult>> OUTPUTS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicBoolean> POISONED = new ConcurrentHashMap<>();

  private final MiniCluster cluster;
  private final Map<String, Object> spec;
  private final FlinkRuntimeOptions options;
  private final Path savepointDir;
  private final String driverId = "driver-" + UUID.randomUUID();
  private final Duration timeout;
  private final boolean strictTypes;
  private final boolean checkpointing;
  private final int parallelism;
  private JobClient job;
  private int consumed = 0;

  public MiniClusterWorkflowDriver(MiniCluster cluster, Map<String, Object> spec, FlinkRuntimeOptions options,
                                   Path savepointDir) {
    this(cluster, spec, options, savepointDir, Duration.ofSeconds(60), false, true);
  }

  public MiniClusterWorkflowDriver(MiniCluster cluster, Map<String, Object> spec, FlinkRuntimeOptions options,
                                   Path savepointDir, Duration timeout, boolean strictTypes, boolean checkpointing) {
    this(cluster, spec, options, savepointDir, timeout, strictTypes, checkpointing, 1);
  }

  /** A driver at the given operator parallelism (the workflow runs the same job at 1 or more subtasks). */
  public MiniClusterWorkflowDriver(MiniCluster cluster, Map<String, Object> spec, FlinkRuntimeOptions options,
                                   Path savepointDir, int parallelism) {
    this(cluster, spec, options, savepointDir, Duration.ofSeconds(60), false, true, parallelism);
  }

  public MiniClusterWorkflowDriver(MiniCluster cluster, Map<String, Object> spec, FlinkRuntimeOptions options,
                                   Path savepointDir, Duration timeout, boolean strictTypes, boolean checkpointing,
                                   int parallelism) {
    if (parallelism < 1) {
      throw new IllegalArgumentException("parallelism must be at least 1, got " + parallelism);
    }
    this.parallelism = parallelism;
    this.cluster = Objects.requireNonNull(cluster);
    this.spec = Objects.requireNonNull(spec);
    this.options = Objects.requireNonNull(options);
    this.savepointDir = Objects.requireNonNull(savepointDir);
    this.timeout = timeout;
    this.strictTypes = strictTypes;
    this.checkpointing = checkpointing;
    INPUTS.put(driverId, new ConcurrentLinkedQueue<>());
    OUTPUTS.put(driverId, new CopyOnWriteArrayList<>());
    POISONED.put(driverId, new AtomicBoolean(false));
  }

  public String driverId() {
    return driverId;
  }

  public int parallelism() {
    return parallelism;
  }

  /** Starts a fresh job (empty state). */
  public void start() throws Exception {
    startFrom(null);
  }

  /** Stops the running job with a savepoint and starts a new incarnation from it. */
  public void restart() throws Exception {
    String savepoint = job.stopWithSavepoint(false, savepointDir.toUri().toString(), SavepointFormatType.CANONICAL)
        .get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    job = null;
    POISONED.get(driverId).set(false);
    startFrom(savepoint);
  }

  /** Triggers a checkpoint on the running job and waits for it to complete. */
  public String checkpoint() throws Exception {
    return cluster.triggerCheckpoint(job.getJobID()).get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
  }

  /** Injects a failure into the running job: the next element throws once, the job recovers from its last checkpoint. */
  public void failJobOnce() throws Exception {
    submitEvent(new Event(HEARTBEAT, "poison-" + UUID.randomUUID(), "driver", "", Map.of(POISON, "true"), null));
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!POISONED.get(driverId).get()) {
      sleepOrFail(deadline, "poison was never observed by the job");
    }
    while (job.getJobStatus().get() == JobStatus.RUNNING) {
      sleepOrFail(deadline, "job never left RUNNING after the injected failure");
    }
    awaitRunning(job, deadline);
  }

  /** Submits one turn and waits for the result that the job emits for it. */
  public TurnResult submit(Event event) throws Exception {
    List<TurnResult> r = submitAll(List.of(event));
    return r.get(0);
  }

  /**
   * Submits events back to back (the fixtures' {@code concurrent_with}) and returns one result per
   * event in the order the job emitted them.
   */
  public List<TurnResult> submitAll(List<Event> events) throws Exception {
    int want = consumed + events.size();
    for (Event e : events) {
      submitEvent(e);
    }
    List<TurnResult> emitted = new ArrayList<>(awaitResults(want));
    consumed = want;
    // Different keys may interleave across subtasks; within a key emission order is submission
    // order, so re-align results to the submitted events (same conversation/turn, first unclaimed).
    List<TurnResult> out = new ArrayList<>(events.size());
    for (Event e : events) {
      int at = -1;
      for (int i = 0; i < emitted.size(); i++) {
        TurnResult r = emitted.get(i);
        if (r.conversationId.equals(e.conversationId()) && r.turnId.equals(e.turnId())) {
          at = i;
          break;
        }
      }
      if (at < 0) {
        throw new IllegalStateException("no result for " + e.conversationId() + "/" + e.turnId() + " in " + emitted);
      }
      out.add(emitted.remove(at));
    }
    return out;
  }

  /** Waits for the next result the job emits without submitting anything (timer-driven resumes). */
  public TurnResult awaitNext() throws Exception {
    List<TurnResult> out = awaitResults(consumed + 1);
    consumed++;
    return out.get(0);
  }

  /** Waits for the next result matching {@code matcher}, consuming everything before it. */
  public TurnResult awaitNext(Predicate<TurnResult> matcher) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      List<TurnResult> all = OUTPUTS.get(driverId);
      for (int i = consumed; i < all.size(); i++) {
        if (matcher.test(all.get(i))) {
          consumed = i + 1;
          return all.get(i);
        }
      }
      sleepOrFail(deadline, "no matching result within " + timeout + "; have " + all);
    }
  }

  /** Every result the job has emitted so far, in emission order. */
  public List<TurnResult> allResults() {
    return List.copyOf(OUTPUTS.get(driverId));
  }

  public JobStatus status() throws Exception {
    return job.getJobStatus().get();
  }

  private void submitEvent(Event e) {
    INPUTS.get(driverId).add(e);
  }

  private List<TurnResult> awaitResults(int want) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (OUTPUTS.get(driverId).size() < want) {
      sleepOrFail(deadline, "job produced " + OUTPUTS.get(driverId).size() + " results, wanted " + want
          + "; status=" + status());
    }
    List<TurnResult> all = OUTPUTS.get(driverId);
    return new ArrayList<>(all.subList(consumed, want));
  }

  private void startFrom(String savepoint) throws Exception {
    Configuration conf = new Configuration();
    conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
    conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 10);
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
    if (checkpointing) {
      conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
      conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, savepointDir.resolve("checkpoints").toUri().toString());
    }
    if (strictTypes) {
      conf.set(PipelineOptions.GENERIC_TYPES, false);
    }
    if (savepoint != null) {
      conf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    }
    StreamExecutionEnvironment env = new TestStreamEnvironment(cluster, conf, parallelism, List.of(), List.of());
    env.setParallelism(parallelism);

    DataGeneratorSource<Event> gen = new DataGeneratorSource<>(new QueueGenerator(driverId), Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(400), WorkflowTurnFunction.EVENT_TYPE);
    DataStream<Event> source = env.fromSource(gen, WatermarkStrategy.noWatermarks(), "turns").setParallelism(1)
        .map(new FailOnce(driverId)).setParallelism(1).returns(WorkflowTurnFunction.EVENT_TYPE)
        .filter(e -> !HEARTBEAT.equals(e.conversationId())).setParallelism(1);

    FlinkPipelineRunner.assembleResults(env, spec, source, options)
        .sinkTo(new CollectingSink(driverId));

    job = env.executeAsync("workflow-" + driverId);
    awaitRunning(job, System.nanoTime() + timeout.toNanos());
  }

  private void awaitRunning(JobClient client, long deadline) throws Exception {
    while (true) {
      JobStatus s = client.getJobStatus().get();
      if (s == JobStatus.RUNNING) {
        return;
      }
      if (s.isGloballyTerminalState()) {
        throw new IllegalStateException("job ended with " + s);
      }
      sleepOrFail(deadline, "job did not reach RUNNING, last status " + s);
    }
  }

  private static void sleepOrFail(long deadline, String message) throws TimeoutException, InterruptedException {
    if (System.nanoTime() > deadline) {
      throw new TimeoutException(message);
    }
    Thread.sleep(10);
  }

  @Override
  public void close() throws Exception {
    if (job != null) {
      try {
        job.cancel().get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
      } catch (Exception ignored) {
        // already finished or failed; nothing left to cancel
      }
      job = null;
    }
    INPUTS.remove(driverId);
    OUTPUTS.remove(driverId);
    POISONED.remove(driverId);
  }

  /** Pulls the next submitted event, or a heartbeat when none is pending. */
  private static final class QueueGenerator implements GeneratorFunction<Long, Event> {
    private static final long serialVersionUID = 1L;
    private final String driverId;

    QueueGenerator(String driverId) {
      this.driverId = driverId;
    }

    @Override
    public Event map(Long index) {
      ConcurrentLinkedQueue<Event> q = INPUTS.get(driverId);
      Event e = q == null ? null : q.poll();
      return e != null ? e : new Event(HEARTBEAT, "hb-" + index, "driver", "", Map.of(), null);
    }
  }

  /** Throws once per job incarnation when the poison marker passes, forcing checkpoint recovery. */
  private static final class FailOnce implements MapFunction<Event, Event> {
    private static final long serialVersionUID = 1L;
    private final String driverId;

    FailOnce(String driverId) {
      this.driverId = driverId;
    }

    @Override
    public Event map(Event e) {
      if (e.metadata() != null && e.metadata().containsKey(POISON)) {
        AtomicBoolean tripped = POISONED.get(driverId);
        if (tripped != null && tripped.compareAndSet(false, true)) {
          throw new IllegalStateException("injected failure for " + driverId);
        }
      }
      return e;
    }
  }

  /** Appends every result to the driver's in-JVM output list. */
  private static final class CollectingSink implements Sink<TurnResult> {
    private static final long serialVersionUID = 1L;
    private final String driverId;

    CollectingSink(String driverId) {
      this.driverId = driverId;
    }

    @Override
    public SinkWriter<TurnResult> createWriter(WriterInitContext context) {
      return new SinkWriter<>() {
        @Override
        public void write(TurnResult element, Context context) {
          List<TurnResult> out = OUTPUTS.get(driverId);
          if (out != null) {
            out.add(element);
          }
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
      };
    }
  }

}
