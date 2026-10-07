package org.agentic.flink.channel.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
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
 * source that drives a serializable {@link PollFn}, the modern replacement for the framework's
 * deprecated {@code SourceFunction} usages (Redis {@code BLPOP}, ZeroMQ, webhook queue, Postgres
 * polling).
 *
 * <p>Single-split, single-reader by design (subtask 0 owns the one split; other subtasks idle): the
 * underlying transports are point-to-point pulls, not partitioned logs, so fan-in happens at the
 * transport, not via Flink split assignment. A background thread runs {@code pollFn.poll(timeout)}
 * and feeds a bounded queue; {@link SourceReader#pollNext} drains it, and availability is signalled
 * through a {@link CompletableFuture} so the runtime never busy-waits. Use it via {@code
 * env.fromSource(new PollingSource<>(fn), WatermarkStrategy.noWatermarks(), name, typeInfo)}.
 *
 * <p>Fault tolerance: the split carries the {@linkplain PositionedPollFn#position() position} of
 * the last record the reader <em>emitted</em> (not merely polled into the queue). On restore the
 * reader calls {@link PositionedPollFn#seek} with that position before polling, so records that sat
 * in the queue but were never emitted are re-read from the target and nothing is skipped or
 * duplicated. A plain {@link PollFn} (a push transport without replay, such as a webhook or Redis
 * pub/sub) snapshots a null position and the reader logs at WARN that delivery across a restart is
 * at most once.
 *
 * @param <T> the produced element type
 */
public final class PollingSource<T>
    implements Source<T, PollingSource.PollingSplit, PollingSource.EnumeratorState> {
  private static final long serialVersionUID = 2L;

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

  /**
   * A {@link PollFn} over a target that exposes a durable, monotonic position (offset, cursor,
   * high-water mark). The position is stored in the split, checkpointed with the reader and handed
   * back through {@link #seek} on restore.
   *
   * @param <T> the produced element type
   */
  public interface PositionedPollFn<T> extends PollFn<T> {
    /**
     * Called once, after {@link #open}, with the position of the last record emitted before the
     * checkpoint the job restored from, or {@code null} on a fresh start. Subsequent {@link #poll}
     * calls must return only records strictly after that position, in position order.
     */
    void seek(String position) throws Exception;

    /**
     * The position of the record most recently returned by {@link #poll}, or {@code null} if none
     * has been returned since {@link #seek}. Called on the poll thread right after {@code poll}.
     */
    String position();
  }

  private final PollFn<T> pollFn;
  private final int queueCapacity;

  public PollingSource(PollFn<T> pollFn) {
    this(pollFn, 1024);
  }

  public PollingSource(PollFn<T> pollFn, int queueCapacity) {
    this.pollFn = Objects.requireNonNull(pollFn, "pollFn");
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
  public SplitEnumerator<PollingSplit, EnumeratorState> createEnumerator(
      SplitEnumeratorContext<PollingSplit> context) {
    return new PollingEnumerator(context, EnumeratorState.INITIAL);
  }

  @Override
  public SplitEnumerator<PollingSplit, EnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<PollingSplit> context, EnumeratorState checkpoint) {
    return new PollingEnumerator(
        context, checkpoint == null ? EnumeratorState.INITIAL : checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<PollingSplit> getSplitSerializer() {
    return PollingSplit.SERIALIZER;
  }

  @Override
  public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
    return EnumeratorState.SERIALIZER;
  }

  // ==================== split ====================

  /**
   * The single split owned by reader 0. Immutable; {@link #position()} is the last emitted position
   * ({@code null} before the first record or for a {@link PollFn} without positions).
   */
  public static final class PollingSplit implements SourceSplit {
    static final String ID = "polling-split-0";
    static final PollingSplit INITIAL = new PollingSplit(null);
    static final int SERIALIZER_VERSION = 2;

    private final String position;

    public PollingSplit(String position) {
      this.position = position;
    }

    @Override
    public String splitId() {
      return ID;
    }

    public String position() {
      return position;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof PollingSplit && Objects.equals(position, ((PollingSplit) o).position);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(position);
    }

    @Override
    public String toString() {
      return "PollingSplit{position=" + position + "}";
    }

    /**
     * Version 1 (pre-position splits) wrote zero bytes; version 2 writes a presence flag and the
     * UTF-8 position.
     */
    static final SimpleVersionedSerializer<PollingSplit> SERIALIZER =
        new SimpleVersionedSerializer<>() {
          @Override
          public int getVersion() {
            return SERIALIZER_VERSION;
          }

          @Override
          public byte[] serialize(PollingSplit obj) throws IOException {
            return writePosition(obj.position);
          }

          @Override
          public PollingSplit deserialize(int version, byte[] serialized) throws IOException {
            switch (version) {
              case 1:
                return INITIAL;
              case SERIALIZER_VERSION:
                return new PollingSplit(readPosition(serialized));
              default:
                throw new IOException("Unknown PollingSplit serializer version " + version);
            }
          }
        };
  }

  /**
   * Enumerator checkpoint: whether the split has been handed out, and, while it is unassigned, the
   * split itself (so a split returned by a failed reader keeps its position).
   */
  public static final class EnumeratorState {
    static final EnumeratorState INITIAL = new EnumeratorState(PollingSplit.INITIAL);
    static final EnumeratorState ASSIGNED = new EnumeratorState(null);
    static final int SERIALIZER_VERSION = 2;

    /** The unassigned split, or {@code null} once it has been handed to reader 0. */
    private final PollingSplit pending;

    EnumeratorState(PollingSplit pending) {
      this.pending = pending;
    }

    PollingSplit pending() {
      return pending;
    }

    boolean assigned() {
      return pending == null;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof EnumeratorState && Objects.equals(pending, ((EnumeratorState) o).pending);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(pending);
    }

    /**
     * Version 1 wrote a single byte (1 = unassigned, 0 = assigned) and no split; version 2 writes
     * the flag followed by the pending split's serialized form when unassigned.
     */
    static final SimpleVersionedSerializer<EnumeratorState> SERIALIZER =
        new SimpleVersionedSerializer<>() {
          @Override
          public int getVersion() {
            return SERIALIZER_VERSION;
          }

          @Override
          public byte[] serialize(EnumeratorState obj) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeBoolean(!obj.assigned());
            if (!obj.assigned()) {
              byte[] split = PollingSplit.SERIALIZER.serialize(obj.pending);
              out.writeInt(split.length);
              out.write(split);
            }
            out.flush();
            return bytes.toByteArray();
          }

          @Override
          public EnumeratorState deserialize(int version, byte[] serialized) throws IOException {
            switch (version) {
              case 1:
                return serialized.length != 0 && serialized[0] == 1 ? INITIAL : ASSIGNED;
              case SERIALIZER_VERSION:
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
                if (!in.readBoolean()) {
                  return ASSIGNED;
                }
                byte[] split = new byte[in.readInt()];
                in.readFully(split);
                return new EnumeratorState(
                    PollingSplit.SERIALIZER.deserialize(PollingSplit.SERIALIZER_VERSION, split));
              default:
                throw new IOException("Unknown EnumeratorState serializer version " + version);
            }
          }
        };
  }

  static byte[] writePosition(String position) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    if (position == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      byte[] utf8 = position.getBytes(StandardCharsets.UTF_8);
      out.writeInt(utf8.length);
      out.write(utf8);
    }
    out.flush();
    return bytes.toByteArray();
  }

  static String readPosition(byte[] serialized) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
    if (!in.readBoolean()) {
      return null;
    }
    byte[] utf8 = new byte[in.readInt()];
    in.readFully(utf8);
    return new String(utf8, StandardCharsets.UTF_8);
  }

  // ==================== enumerator ====================

  /** Hands the single split to the first registered reader; idempotent across restore. */
  private static final class PollingEnumerator
      implements SplitEnumerator<PollingSplit, EnumeratorState> {
    private final SplitEnumeratorContext<PollingSplit> context;
    private PollingSplit pending;

    PollingEnumerator(SplitEnumeratorContext<PollingSplit> context, EnumeratorState state) {
      this.context = context;
      this.pending = state.pending();
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
      if (pending != null && subtaskId == 0) {
        PollingSplit split = pending;
        pending = null;
        context.assignSplit(split, 0);
      }
      // Readers that won't get a split must be told so they don't wait forever.
      if (subtaskId != 0) {
        context.signalNoMoreSplits(subtaskId);
      }
    }

    @Override
    public void addSplitsBack(List<PollingSplit> splits, int subtaskId) {
      if (!splits.isEmpty()) {
        // A failed reader returned it; keep its position and reassign on next registration.
        pending = splits.get(splits.size() - 1);
      }
    }

    @Override
    public EnumeratorState snapshotState(long checkpointId) {
      return pending == null ? EnumeratorState.ASSIGNED : new EnumeratorState(pending);
    }

    @Override
    public void close() {}
  }

  // ==================== reader ====================

  /** A polled record together with the target position it advanced to. */
  private static final class Polled<T> {
    final T record;
    final String position;

    Polled(T record, String position) {
      this.record = record;
      this.position = position;
    }
  }

  private static final class PollingReader<T> implements SourceReader<T, PollingSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PollingReader.class);

    private final PollFn<T> pollFn;
    private final int subtaskIndex;
    private final LinkedBlockingQueue<Polled<T>> queue;
    private volatile boolean running = true;
    private volatile boolean assigned = false;
    private volatile Throwable failure;
    private Thread pollThread;

    /** Position handed over in the assigned split; the poll thread seeks to it before polling. */
    private String restoredPosition;

    /** Position of the last record emitted through {@link #pollNext}; mailbox thread only. */
    private String emittedPosition;

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
      Polled<T> polled = queue.poll();
      if (polled != null) {
        output.collect(polled.record);
        emittedPosition = polled.position;
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
      PollingSplit split = splits.get(splits.size() - 1);
      restoredPosition = split.position();
      emittedPosition = restoredPosition;
      if (pollFn instanceof PositionedPollFn) {
        LOG.info(
            "PollingSource subtask {} starting from position {}",
            subtaskIndex,
            restoredPosition == null ? "<beginning>" : restoredPosition);
      } else {
        LOG.warn(
            "PollingSource subtask {} drives {} which exposes no replay position; records "
                + "delivered between the last checkpoint and a failure are not redelivered",
            subtaskIndex,
            pollFn.getClass().getName());
      }
      pollThread = new Thread(this::runPollLoop, "polling-source-" + subtaskIndex);
      pollThread.setDaemon(true);
      pollThread.start();
    }

    private void runPollLoop() {
      try {
        pollFn.open(subtaskIndex);
        PositionedPollFn<T> positioned =
            pollFn instanceof PositionedPollFn ? (PositionedPollFn<T>) pollFn : null;
        if (positioned != null) {
          positioned.seek(restoredPosition);
        }
        while (running) {
          T rec = pollFn.poll(500);
          if (rec != null) {
            String position = positioned == null ? null : positioned.position();
            queue.put(new Polled<>(rec, position)); // blocks under backpressure (bounded queue)
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
          LOG.warn(
              "PollingSource pollFn.close failed on subtask {}: {}", subtaskIndex, e.toString());
        }
      }
    }

    @Override
    public List<PollingSplit> snapshotState(long checkpointId) {
      // Records still in the queue were not emitted, so they are not part of this checkpoint; the
      // poll thread re-reads them from the target after a restore.
      List<PollingSplit> held = new ArrayList<>();
      if (assigned) {
        held.add(new PollingSplit(emittedPosition));
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
