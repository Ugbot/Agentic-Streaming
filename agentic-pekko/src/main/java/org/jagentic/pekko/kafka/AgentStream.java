package org.jagentic.pekko.kafka;

import java.time.Duration;
import java.util.UUID;

import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.stream.javadsl.Flow;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.ConversationManager;

/** The reusable Pekko Streams stage at the heart of "streaming things around": a backpressured
 * {@code mapAsync} that turns a stream of {@link Event}s into a stream of {@link ConversationEntity.TurnReply}s
 * by asking the per-conversation entity. {@code parallelism} bounds the in-flight turns (flow
 * control); same-conversation bursts are further serialized by the entity's stash. Broker-agnostic
 * — drop any Source in front (Kafka, HTTP, a test list) and any Sink behind. */
public final class AgentStream {

  private AgentStream() {}

  public static Flow<Event, ConversationEntity.TurnReply, NotUsed> flow(
      ActorSystem<ConversationManager.Command> system, int parallelism, Duration timeout) {
    return Flow.<Event>create().mapAsync(parallelism, event ->
        AskPattern.ask(
            system,
            (ActorRef<ConversationEntity.TurnReply> replyTo) -> new ConversationManager.Envelope(
                event.conversationId(),
                new ConversationEntity.ProcessTurn(UUID.randomUUID().toString(), event, replyTo)),
            timeout,
            system.scheduler()));
  }
}
