package org.jagentic.gateway;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;

/** Where the gateway hands a turn: a local runtime, a Pekko system, or a Flink job over Kafka. */
public interface TurnBackend {

  /** One turn or signal. The event carries the principal and the scoped conversation key. */
  CompletionStage<TurnResult> submit(Event event);

  /** A conversation's log, empty when there is none. */
  CompletionStage<List<LogEvent>> conversation(String key);
}
