package org.jagentic.pekko.runtime;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.entity.ConversationEntity;

/** Adapts the Pekko actor system to the core {@link Runtime} SPI: {@code submit(Event)} asks the
 * conversation entity for the turn and blocks for the reply (the {@code Runtime.submit} contract
 * is synchronous, like {@code LocalRuntime}'s per-conversation lock). The entity itself is never
 * blocked — turns run inside the actor; concurrency is bounded by the ask timeout. */
public final class PekkoRuntime implements Runtime, AutoCloseable {

  private final ActorSystem<ConversationManager.Command> system;
  private final Duration timeout;
  private final boolean ownsSystem;

  public PekkoRuntime(ActorSystem<ConversationManager.Command> system, Duration timeout) {
    this(system, timeout, false);
  }

  /** {@code ownsSystem=true} when this runtime created the actor system (e.g. via the backend
   * provider) and should terminate it on {@link #close()}. */
  public PekkoRuntime(ActorSystem<ConversationManager.Command> system, Duration timeout, boolean ownsSystem) {
    this.system = system;
    this.timeout = timeout;
    this.ownsSystem = ownsSystem;
  }

  @Override
  public TurnResult submit(Event event) {
    CompletionStage<ConversationEntity.TurnReply> cs = AskPattern.ask(
        system,
        (ActorRef<ConversationEntity.TurnReply> replyTo) ->
            new ConversationManager.Envelope(event.conversationId(),
                new ConversationEntity.ProcessTurn(UUID.randomUUID().toString(), event, replyTo)),
        timeout,
        system.scheduler());
    try {
      ConversationEntity.TurnReply r =
          cs.toCompletableFuture().get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
      TurnResult tr = new TurnResult(r.conversationId(), r.reply(), r.toolCalls());
      tr.path = r.path();
      tr.ok = r.ok();
      return tr;
    } catch (Exception e) {
      throw new RuntimeException("pekko submit failed for " + event.conversationId() + ": " + e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if (ownsSystem) {
      system.terminate();
    }
  }
}
