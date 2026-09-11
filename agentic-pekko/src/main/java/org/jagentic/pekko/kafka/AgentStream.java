package org.jagentic.pekko.kafka;

import java.time.Duration;

import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.stream.javadsl.Flow;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.ConversationManager;
import org.jagentic.pekko.runtime.PekkoRuntime;

/**
 * Pekko Streams flow: {@link Event} in, normalized {@link ConversationEntity.TurnReply} out. Uses
 * {@code mapAsync(parallelism)} (order-preserving) so the stream keeps its input order end to end
 * while the entities apply each conversation's turns in mailbox order. The event's own
 * {@code turn_id} is what the entity dedupes on, so a redelivered record yields a {@code duplicate}.
 */
public final class AgentStream {
  private AgentStream() {}

  public static Flow<Event, ConversationEntity.TurnReply, NotUsed> flow(
      ActorSystem<ConversationManager.Command> system, int parallelism, Duration timeout) {
    return Flow.of(Event.class).mapAsync(parallelism, event -> PekkoRuntime.ask(system, event, timeout));
  }
}
