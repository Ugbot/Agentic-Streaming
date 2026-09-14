package org.agentic.flink.runtime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.agentic.flink.pipeline.FlinkPipelineRunner;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;

/**
 * One long-running workflow job on an in-process local Flink environment, driven turn by turn
 * from the embedding process (the Python {@code flink-jvm} runtime, or a test).
 *
 * <p>Turns enter through an unbounded source that polls a per-session in-JVM queue (a filtered
 * heartbeat is emitted while the queue is empty, so checkpoints and stop-with-savepoint keep
 * flowing) and the normalized {@link TurnResult}s leave through an in-JVM sink. Both registries
 * are static because the local cluster's tasks share the caller's JVM.
 *
 * <p>{@link #restart()} models a runtime restart the way the Flink conformance binding does: the
 * running job is stopped with a savepoint, the local cluster it ran on shuts down, and a new job is
 * started that restores the savepoint. Only checkpointed keyed state survives; the
 * {@link WorkflowTurnFunction} rebuilds each conversation from its {@link KeyedConversationLog} on
 * the next turn, without running brains or tools for the turns already recorded.
 */
public final class LocalWorkflowSession implements AutoCloseable {

  static final String HEARTBEAT = "__heartbeat__";
  private static final long IDLE_POLL_MILLIS = 2;

  private static final Map<String, LinkedBlockingQueue<Event>> INPUTS = new ConcurrentHashMap<>();
  private static final Map<String, LinkedBlockingQueue<TurnResult>> OUTPUTS = new ConcurrentHashMap<>();

  private final Map<String, Object> spec;
  private final FlinkRuntimeOptions options;
  private final int parallelism;
  private final Duration checkpointInterval;
  private final Path savepointDir;
  private final Duration timeout;
  private final String jobName;
  private final String sessionId = "session-" + UUID.randomUUID();
  private final List<TurnResult> unclaimed = new ArrayList<>();
  private JobClient job;
  private String lastSavepoint;
  private int restarts;

  /**
   * @param spec the validated agentic/v1 workflow document
   * @param options Flink runtime options ({@code runtime.flink} of the document)
   * @param parallelism job parallelism (at least 1)
   * @param checkpointInterval periodic checkpoint interval; {@code null} keeps checkpoints off between
   *     restarts (stop-with-savepoint does not need them)
   * @param savepointDir local directory that receives savepoints and checkpoints
   * @param timeout how long to wait for a result, a savepoint, or the job to start
   * @param jobName the Flink job name
   */
  public LocalWorkflowSession(Map<String, Object> spec, FlinkRuntimeOptions options, int parallelism,
                              Duration checkpointInterval, Path savepointDir, Duration timeout, String jobName) {
    this.spec = Objects.requireNonNull(spec, "spec");
    this.options = Objects.requireNonNull(options, "options");
    if (parallelism < 1) {
      throw new IllegalArgumentException("parallelism must be >= 1, got " + parallelism);
    }
    this.parallelism = parallelism;
    this.checkpointInterval = checkpointInterval;
    this.savepointDir = Objects.requireNonNull(savepointDir, "savepointDir");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.jobName = Objects.requireNonNull(jobName, "jobName");
    INPUTS.put(sessionId, new LinkedBlockingQueue<>());
    OUTPUTS.put(sessionId, new LinkedBlockingQueue<>());
  }

  public String sessionId() {
    return sessionId;
  }

  /** The savepoint the current job was restored from, or {@code null} for a fresh job. */
  public String lastSavepoint() {
    return lastSavepoint;
  }

  /** How many times {@link #restart()} has completed. */
  public int restarts() {
    return restarts;
  }

  /** Starts a fresh job with empty state. */
  public void start() throws Exception {
    requireOpen();
    if (job != null) {
      throw new IllegalStateException("session " + sessionId + " already has a running job");
    }
    startFrom(null);
  }

  /** Stops the running job with a savepoint, then starts a new job restored from it. */
  public void restart() throws Exception {
    requireOpen();
    requireRunning();
    String savepoint = job.stopWithSavepoint(false, savepointDir.toUri().toString(), SavepointFormatType.CANONICAL)
        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    job = null;
    startFrom(savepoint);
    restarts++;
  }

  /** Submits one turn and waits for the result the job emits for it. */
  public TurnResult submit(Event event) throws Exception {
    return submitAll(List.of(event)).get(0);
  }

  /**
   * Submits events back to back and returns one result per event, in submission order. Results
   * for other turns that arrive meanwhile are kept for later calls.
   */
  public List<TurnResult> submitAll(List<Event> events) throws Exception {
    requireOpen();
    requireRunning();
    LinkedBlockingQueue<Event> in = INPUTS.get(sessionId);
    for (Event e : events) {
      in.add(Objects.requireNonNull(e, "event"));
    }
    List<TurnResult> out = new ArrayList<>(events.size());
    long deadline = System.nanoTime() + timeout.toNanos();
    for (Event e : events) {
      out.add(await(e.conversationId(), e.turnId(), deadline));
    }
    return out;
  }

  public JobStatus status() throws Exception {
    requireRunning();
    return job.getJobStatus().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private TurnResult await(String conversationId, String turnId, long deadline) throws Exception {
    for (int i = 0; i < unclaimed.size(); i++) {
      TurnResult r = unclaimed.get(i);
      if (r.conversationId.equals(conversationId) && r.turnId.equals(turnId)) {
        return unclaimed.remove(i);
      }
    }
    LinkedBlockingQueue<TurnResult> outputs = OUTPUTS.get(sessionId);
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        throw new TimeoutException("no result for " + conversationId + "/" + turnId + " within " + timeout
            + "; job status " + status() + ", unclaimed results " + unclaimed);
      }
      TurnResult r = outputs.poll(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
      if (r == null) {
        JobStatus s = status();
        if (s.isGloballyTerminalState()) {
          throw new IllegalStateException("job ended with " + s + " before emitting a result for "
              + conversationId + "/" + turnId);
        }
        continue;
      }
      if (r.conversationId.equals(conversationId) && r.turnId.equals(turnId)) {
        return r;
      }
      unclaimed.add(r);
    }
  }

  private void startFrom(String savepoint) throws Exception {
    Configuration conf = new Configuration();
    conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
    conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 10);
    conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, savepointDir.resolve("checkpoints").toUri().toString());
    if (checkpointInterval != null) {
      conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
    }
    if (savepoint != null) {
      conf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
    env.setParallelism(parallelism);

    // One poller: with several, turns of one conversation submitted back to back could be taken
    // by different subtasks and reach the keyed operator out of submission order.
    DataStream<Event> source = env.fromSequence(0, Long.MAX_VALUE / 2).setParallelism(1)
        .map(new QueuePoller(sessionId)).returns(WorkflowTurnFunction.EVENT_TYPE).setParallelism(1)
        .filter(e -> !HEARTBEAT.equals(e.conversationId())).setParallelism(1);

    FlinkPipelineRunner.assembleResults(env, spec, source, options)
        .sinkTo(new CollectingSink(sessionId));

    job = env.executeAsync(jobName);
    lastSavepoint = savepoint;
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      JobStatus s = job.getJobStatus().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (s == JobStatus.RUNNING) {
        return;
      }
      if (s.isGloballyTerminalState()) {
        throw new IllegalStateException("job ended with " + s + " before reaching RUNNING");
      }
      if (System.nanoTime() > deadline) {
        throw new TimeoutException("job did not reach RUNNING within " + timeout + "; last status " + s);
      }
      Thread.sleep(10);
    }
  }

  private void requireOpen() {
    if (!INPUTS.containsKey(sessionId)) {
      throw new IllegalStateException("session " + sessionId + " is closed");
    }
  }

  private void requireRunning() {
    if (job == null) {
      throw new IllegalStateException("session " + sessionId + " has no running job; call start() first");
    }
  }

  @Override
  public void close() throws Exception {
    if (job != null) {
      try {
        job.cancel().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (Exception alreadyDown) {
        // the job already finished or failed; there is nothing left to cancel
      }
      try {
        // Completes (exceptionally, with the cancellation) once the job is terminal, which is
        // when the per-job local cluster behind it shuts down; leaving before that lets the
        // caller tear the JVM down under a live cluster.
        job.getJobExecutionResult().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (Exception terminal) {
        // cancelled or failed: either way the job is done
      }
      job = null;
    }
    INPUTS.remove(sessionId);
    OUTPUTS.remove(sessionId);
    unclaimed.clear();
  }

  /** Emits the next submitted turn, or a heartbeat after a short idle wait when none is pending. */
  private static final class QueuePoller implements MapFunction<Long, Event> {
    private static final long serialVersionUID = 1L;
    private final String sessionId;

    QueuePoller(String sessionId) {
      this.sessionId = sessionId;
    }

    @Override
    public Event map(Long index) throws InterruptedException {
      LinkedBlockingQueue<Event> q = INPUTS.get(sessionId);
      Event e = q == null ? null : q.poll(IDLE_POLL_MILLIS, TimeUnit.MILLISECONDS);
      return e != null ? e : new Event(HEARTBEAT, "hb-" + index, "session", "", Map.of(), null);
    }
  }

  /** Hands every result to the session's in-JVM output queue. */
  private static final class CollectingSink implements Sink<TurnResult> {
    private static final long serialVersionUID = 1L;
    private final String sessionId;

    CollectingSink(String sessionId) {
      this.sessionId = sessionId;
    }

    @Override
    public SinkWriter<TurnResult> createWriter(WriterInitContext context) {
      return new SinkWriter<>() {
        @Override
        public void write(TurnResult element, Context context) {
          LinkedBlockingQueue<TurnResult> out = OUTPUTS.get(sessionId);
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
