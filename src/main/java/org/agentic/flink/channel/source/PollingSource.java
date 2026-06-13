package org.agentic.flink.channel.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic, native Flink 2.x ({@link org.apache.flink.api.connector.source FLIP-27}) unbounded
 * source that drives a serializable {@link PollFn} — the modern replacement for the framework's
 * deprecated {@code SourceFunction} usages (Redis {@code BLPOP}, ZeroMQ, webhook queue, …).
 *
 * <p>Single-split, single-reader by design (subtask 0 owns the one split; other subtasks idle): the
 * underlying transports are point-to-point pulls, not partitioned logs, so fan-in happens at the
 * transport, not via Flink split assignment. A background thread runs {@code pollFn.poll(timeout)}
 * and feeds a bounded queue; {@link SourceReader#pollNext} drains it, and availability is signalled
 * through a {@link CompletableFuture} so the runtime never busy-waits. Use it via
 * {@code env.fromSource(new PollingSource<>(fn, typeInfo-less), WatermarkStrategy.noWatermarks(), name)}.
 *
 * @param <T> the produced element type
 */
public final class PollingSource<T> implements Source<T, PollingSource.PollingSplit, Integer> {
  private static final long serialVersionUID = 1L;

  /** The per-reader polling behavior. Must be {@link Serializable} (it ships in the job graph). */
  public interface PollFn<T> extends Serializable {
    /** Initialize per-reader resources (open a socket / connection). */
    default void open(int subtaskIndex) throws Exception {}

    /**
     * Return the next element, or {@code null} if none arrived within {@code timeoutMs}. Must honor
     * the timeout so the reader can shut down promptly.
     */
    T poll(long timeoutMs) throws Exception;

    /** Release resources. */
    default void close() throws Exception {}
  }

  private final PollFn<T> pollFn;
  private final int queueCapacity;

  public PollingSource(PollFn<T> pollFn) {
    this(pollFn, 1024);
  }

  public PollingSource(PollFn<T> pollFn, int queueCapacity) {
    this.pollFn = java.util.Objects.requireNonNull(pollFn, "pollFn");
    this.queueCapacity = Math.max(1, queueCapacity);
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<T, PollingSplit> createReader(SourceReaderContext context) {
    return new PollingReader<>(pollFn, queueCapacity, context.getIndexOfSubtask());
  }

  @Override
  public SplitEnumerator<PollingSplit, Integer> createEnumerator(
      SplitEnumeratorContext<PollingSplit> context) {
    return new PollingEnumerator(context, false);
  }

  @Override
  public SplitEnumerator<PollingSplit, Integer> restoreEnumerator(
      SplitEnumeratorContext<PollingSplit> context, Integer checkpoint) {
    return new PollingEnumerator(context, checkpoint != null && checkpoint == 1);
  }

  @Override
  public SimpleVersionedSerializer<PollingSplit> getSplitSerializer() {
    return PollingSplit.SERIALIZER;
  }

  @Override
  public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
    return ASSIGNED_SERIALIZER;
  }

  // ==================== split ====================

  /** The single, stateless split owned by reader 0. */
  public static final class PollingSplit implements SourceSplit {
    static final PollingSplit INSTANCE = new PollingSplit();
    static final String ID = "polling-split-0";

    @Override
    public String splitId() {
      return ID;
    }

    static final SimpleVersionedSerializer<PollingSplit> SERIALIZER =
        new SimpleVersionedSerializer<>() {
          @Override
          public int getVersion() {
            return 1;
          }

          @Override
          public byte[] serialize(PollingSplit obj) {
            return new byte[0];
          }

          @Override
          public PollingSplit deserialize(int version, byte[] serialized) {
            return INSTANCE;
          }
        };
  }

  /** Enumerator checkpoint: 1 = the split is still unassigned, 0 = already handed out. */
  private static final SimpleVersionedSerializer<Integer> ASSIGNED_SERIALIZER =
      new SimpleVersionedSerializer<>() {
        @Override
        public int getVersion() {
          return 1;
        }

        @Override
        public byte[] serialize(Integer obj) {
          return new byte[] {(byte) (obj == null ? 0 : obj.intValue())};
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) {
          return serialized.length == 0 ? 0 : (int) serialized[0];
        }
      };

  // ==================== enumerator ====================

  /** Hands the single split to the first registered reader; idempotent across restore. */
  private static final class PollingEnumerator implements SplitEnumerator<PollingSplit, Integer> {
    private final SplitEnumeratorContext<PollingSplit> context;
    private boolean unassigned;

    PollingEnumerator(SplitEnumeratorContext<PollingSplit> context, boolean alreadyAssigned) {
      this.context = context;
      this.unassigned = !alreadyAssigned;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
      assignIfNeeded(subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
      assignIfNeeded(subtaskId);
    }

    private void assignIfNeeded(int subtaskId) {
      // Only subtask 0 gets the single split; assign exactly once.
      if (unassigned && subtaskId == 0) {
        unassigned = false;
        context.assignSplit(PollingSplit.INSTANCE, 0);
      }
      // Readers that won't get a split must be told so they don't wait forever.
      if (subtaskId != 0) {
        context.signalNoMoreSplits(subtaskId);
      }
    }

    @Override
    public void addSplitsBack(List<PollingSplit> splits, int subtaskId) {
      if (!splits.isEmpty()) {
        unassigned = true; // a failed reader returned it; reassign on next registration
      }
    }

    @Override
    public Integer snapshotState(long checkpointId) {
      return unassigned ? 1 : 0;
    }

    @Override
    public void close() {}
  }

  // ==================== reader ====================

  private static final class PollingReader<T> implements SourceReader<T, PollingSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PollingReader.class);

    private final PollFn<T> pollFn;
    private final int subtaskIndex;
    private final LinkedBlockingQueue<T> queue;
    private volatile boolean running = true;
    private volatile boolean assigned = false;
    private volatile Throwable failure;
    private Thread pollThread;

    private final Object lock = new Object();
    private CompletableFuture<Void> available = new CompletableFuture<>();

    PollingReader(PollFn<T> pollFn, int queueCapacity, int subtaskIndex) {
      this.pollFn = pollFn;
      this.subtaskIndex = subtaskIndex;
      this.queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    @Override
    public void start() {
      // Polling starts when the split is assigned (addSplits), not before.
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
      if (failure != null) {
        throw new IOException("PollingSource reader failed", failure);
      }
      T record = queue.poll();
      if (record != null) {
        output.collect(record);
        return queue.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
      }
      return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      if (!queue.isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      synchronized (lock) {
        if (available.isDone()) {
          available = new CompletableFuture<>();
        }
        // Re-check after acquiring the lock to avoid missing a producer signal.
        if (!queue.isEmpty()) {
          available.complete(null);
        }
        return available;
      }
    }

    private void signalAvailable() {
      synchronized (lock) {
        if (!available.isDone()) {
          available.complete(null);
        }
      }
    }

    @Override
    public void addSplits(List<PollingSplit> splits) {
      if (splits.isEmpty() || assigned) {
        return;
      }
      assigned = true;
      pollThread = new Thread(this::runPollLoop, "polling-source-" + subtaskIndex);
      pollThread.setDaemon(true);
      pollThread.start();
    }

    private void runPollLoop() {
      try {
        pollFn.open(subtaskIndex);
        while (running) {
          T rec = pollFn.poll(500);
          if (rec != null) {
            queue.put(rec); // blocks under backpressure (bounded queue)
            signalAvailable();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        failure = t;
        signalAvailable();
        LOG.warn("PollingSource poll loop failed on subtask {}: {}", subtaskIndex, t.toString());
      } finally {
        // Close on the poll thread: transports like ZMQ sockets are thread-affined and must be torn
        // down on the same thread that used them.
        try {
          pollFn.close();
        } catch (Exception e) {
          LOG.warn("PollingSource pollFn.close failed on subtask {}: {}", subtaskIndex, e.toString());
        }
      }
    }

    @Override
    public List<PollingSplit> snapshotState(long checkpointId) {
      // The split is stateless (the transport owns delivery position); report it if we hold it.
      List<PollingSplit> held = new ArrayList<>();
      if (assigned) {
        held.add(PollingSplit.INSTANCE);
      }
      return held;
    }

    @Override
    public void notifyNoMoreSplits() {
      // Single-split source: a reader with no split simply produces nothing.
    }

    @Override
    public void close() throws Exception {
      running = false;
      if (pollThread != null) {
        // The poll thread closes pollFn in its finally block (thread-affinity); just signal + join.
        pollThread.interrupt();
        pollThread.join(5000);
      } else {
        // No split was ever assigned, so the poll loop never ran (pollFn never opened): close here.
        pollFn.close();
      }
    }
  }
}
