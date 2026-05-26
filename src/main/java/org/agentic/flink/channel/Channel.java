package org.agentic.flink.channel;

import java.io.Serializable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Continuous-input primitive: a serializable factory that produces a {@link DataStream} of
 * elements at job-graph-construction time.
 *
 * <p>Generalizes the older {@code MemoryFeed} from any source — Kafka, Postgres CDC, Redis
 * pub/sub, an HTTP webhook, an LLM tool invocation, a static seed set — into a single
 * abstraction. Multiple channels can be {@link DataStream#union(DataStream[])}-ed together to
 * feed one operator (the crawler frontier is the canonical case: seeds + discovery + LLM-driven
 * fetches + external feeders all wired into the same input).
 *
 * <p>Implementations must be {@link Serializable} because they ride along with the operator
 * spec in the Flink job graph; the live transport (Kafka consumer, HTTP server, JDBC handle) is
 * constructed inside {@link #open(StreamExecutionEnvironment)} when the job is built.
 *
 * @param <T> element type emitted by this channel
 */
public interface Channel<T> extends Serializable {

  /** Build the channel's source into the supplied execution environment. */
  DataStream<T> open(StreamExecutionEnvironment env) throws Exception;

  /** The Flink type information of elements emitted by this channel. */
  TypeInformation<T> elementType();

  /** Human-readable provider name; used for source naming and logging. */
  default String providerName() {
    return getClass().getSimpleName();
  }
}
