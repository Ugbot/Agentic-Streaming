package org.jagentic.core.stream;

import java.util.Optional;

/**
 * A source of events — the portable counterpart of a Flink source / Kafka consumer / Pekko Source.
 * Pull-based: {@link #poll()} returns the next available event, or empty when none is available right
 * now. Bounded sources (a seed list) eventually return empty forever; unbounded sources (a queue) may
 * return empty transiently and more later. The {@link StreamRuntime} drives a channel into the agent.
 */
public interface Channel<T> {

  /** The next event if one is available, else empty. Never blocks. */
  Optional<T> poll();
}
