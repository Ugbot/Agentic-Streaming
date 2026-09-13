package org.jagentic.pekko.runtime;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.entity.ConversationEntity;

/**
 * Adapts the Pekko actor system to the core {@link Runtime} SPI. {@link #submit} asks the
 * conversation entity for the turn and blocks for its normalized {@link TurnResult}; the incoming
 * {@link Event#turnId()} is carried through untouched, so idempotency is decided by the entity's
 * journal. {@link #submitAsync} is the non-blocking form; turns of one conversation still apply in
 * the order the entity's mailbox receives them.
 */
public final class PekkoRuntime implements Runtime, AutoCloseable {
  private final ActorSystem<ConversationManager.Command> system;
  private final Duration timeout;
  private final boolean ownsSystem;

  public PekkoRuntime(ActorSystem<ConversationManager.Command> system, Duration timeout) {
    this(system, timeout, false);
  }

  /** {@code ownsSystem=true} when this runtime created the actor system and should terminate it on {@link #close()}. */
  public PekkoRuntime(ActorSystem<ConversationManager.Command> system, Duration timeout, boolean ownsSystem) {
    this.system = system;
    this.timeout = timeout;
    this.ownsSystem = ownsSystem;
  }

  public ActorSystem<ConversationManager.Command> system() {
    return system;
  }

  @Override
  public TurnResult submit(Event event) {
    try {
      return submitAsync(event).get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new RuntimeException("pekko submit failed for " + event.conversationId() + "/" + event.turnId()
          + ": " + e.getMessage(), e);
    }
  }

  public CompletableFuture<TurnResult> submitAsync(Event event) {
    return ask(system, event, timeout).thenApply(ConversationEntity.TurnReply::toResult).toCompletableFuture();
  }

  /** Ask the entity for one turn: the single ask every front door (HTTP, Kafka, streams) uses. */
  public static CompletionStage<ConversationEntity.TurnReply> ask(
      ActorSystem<ConversationManager.Command> system, Event event, Duration timeout) {
    return AskPattern.ask(
        system,
        (ActorRef<ConversationEntity.TurnReply> replyTo) -> new ConversationManager.Envelope(
            event.conversationId(), new ConversationEntity.ProcessTurn(event, replyTo)),
        timeout,
        system.scheduler());
  }

  /** Durably schedule a timer on a conversation (see {@link ConversationEntity.ScheduleTimer}). */
  public ConversationEntity.TimerAck scheduleTimer(String conversationId, String timerId, Duration delay, Event fire) {
    CompletionStage<ConversationEntity.TimerAck> cs = AskPattern.ask(
        system,
        (ActorRef<ConversationEntity.TimerAck> replyTo) -> new ConversationManager.Envelope(conversationId,
            new ConversationEntity.ScheduleTimer(timerId, delay.toMillis(), fire, replyTo)),
        timeout,
        system.scheduler());
    return await(cs, "scheduleTimer " + conversationId + "/" + timerId);
  }

  /** The folded state and journal of one conversation. */
  public ConversationEntity.StateSnapshot state(String conversationId) {
    CompletionStage<ConversationEntity.StateSnapshot> cs = AskPattern.ask(
        system,
        (ActorRef<ConversationEntity.StateSnapshot> replyTo) -> new ConversationManager.Envelope(conversationId,
            new ConversationEntity.GetState(replyTo)),
        timeout,
        system.scheduler());
    return await(cs, "state " + conversationId);
  }

  /** Stop the live entity of one conversation; the next command recovers it from the journal. */
  public void passivate(String conversationId) {
    CompletionStage<Done> cs = AskPattern.ask(
        system,
        (ActorRef<Done> ack) -> new ConversationManager.Passivate(conversationId, ack),
        timeout,
        system.scheduler());
    await(cs, "passivate " + conversationId);
  }

  private <T> T await(CompletionStage<T> cs, String what) {
    try {
      return cs.toCompletableFuture().get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new RuntimeException("pekko " + what + " failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if (ownsSystem) {
      system.terminate();
    }
  }
}
