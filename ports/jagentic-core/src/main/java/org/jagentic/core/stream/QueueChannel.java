package org.jagentic.core.stream;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

/** An unbounded in-memory channel: producers {@link #offer} events; the runtime polls them in FIFO
 * order. Thread-safe, so a producer thread and the stream loop can share it. */
public final class QueueChannel<T> implements Channel<T> {

  private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();

  public QueueChannel<T> offer(T event) {
    queue.add(event);
    return this;
  }

  @Override
  public Optional<T> poll() {
    return Optional.ofNullable(queue.poll());
  }
}
