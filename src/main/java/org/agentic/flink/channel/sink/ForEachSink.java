package org.agentic.flink.channel.sink;

import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * A generic, native Flink 2.x ({@link org.apache.flink.api.connector.sink2 FLIP-143}) sink that runs
 * a serializable {@link WriteFn} per element — the modern replacement for the framework's deprecated
 * {@code RichSinkFunction}/{@code addSink} usages.
 *
 * <p>One {@code WriteFn} is built per subtask: {@link WriteFn#open(int)} initializes the per-subtask
 * resource (a socket, a client), {@link WriteFn#write(Object)} handles each element, {@link
 * WriteFn#flush()} is called on checkpoint/end-of-input, and {@link WriteFn#close()} releases it.
 * Use it via {@code stream.sinkTo(new ForEachSink<>(fn))}.
 *
 * @param <T> the element type
 */
public final class ForEachSink<T> implements Sink<T> {
  private static final long serialVersionUID = 1L;

  /** The per-subtask write behavior. Must be {@link Serializable} (it ships in the job graph). */
  public interface WriteFn<T> extends Serializable {
    /** Initialize per-subtask resources. {@code subtaskIndex} is this writer's parallel slot. */
    default void open(int subtaskIndex) throws Exception {}

    /** Handle a single element. */
    void write(T element) throws Exception;

    /** Flush buffered work (called on checkpoint and end-of-input). */
    default void flush() throws Exception {}

    /** Release resources. */
    default void close() throws Exception {}
  }

  private final WriteFn<T> writeFn;

  public ForEachSink(WriteFn<T> writeFn) {
    this.writeFn = java.util.Objects.requireNonNull(writeFn, "writeFn");
  }

  @Override
  public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
    int subtask = context.getTaskInfo().getIndexOfThisSubtask();
    Writer<T> writer = new Writer<>(writeFn);
    try {
      writer.open(subtask);
    } catch (Exception e) {
      throw new IOException("ForEachSink writer open failed", e);
    }
    return writer;
  }

  private static final class Writer<T> implements SinkWriter<T> {
    private final WriteFn<T> writeFn;

    Writer(WriteFn<T> writeFn) {
      this.writeFn = writeFn;
    }

    void open(int subtaskIndex) throws Exception {
      writeFn.open(subtaskIndex);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
      try {
        writeFn.write(element);
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("ForEachSink write failed", e);
      }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
      try {
        writeFn.flush();
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("ForEachSink flush failed", e);
      }
    }

    @Override
    public void close() throws Exception {
      writeFn.close();
    }
  }
}
