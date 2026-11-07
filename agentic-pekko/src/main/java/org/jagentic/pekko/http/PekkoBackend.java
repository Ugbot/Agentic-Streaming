package org.jagentic.pekko.http;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.gateway.TurnBackend;
import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.ConversationManager;
import org.jagentic.pekko.runtime.PekkoRuntime;

/** A Pekko system behind the gateway: a turn goes to the conversation's entity, and so does a read. */
public final class PekkoBackend implements TurnBackend {

  private final ActorSystem<ConversationManager.Command> system;
  private final PekkoRuntime runtime;
  private final Duration timeout;

  public PekkoBackend(ActorSystem<ConversationManager.Command> system, Duration timeout) {
    this.system = system;
    this.runtime = new PekkoRuntime(system, timeout, false);
    this.timeout = timeout;
  }

  @Override
  public CompletionStage<TurnResult> submit(Event event) {
    return runtime.submitAsync(event);
  }

  @Override
  public CompletionStage<List<LogEvent>> conversation(String key) {
    CompletionStage<ConversationEntity.StateSnapshot> ask = AskPattern.ask(
        system,
        (ActorRef<ConversationEntity.StateSnapshot> replyTo) -> new ConversationManager.Envelope(key,
            new ConversationEntity.GetState(replyTo)),
        timeout,
        system.scheduler());
    return ask.thenApply(ConversationEntity.StateSnapshot::events);
  }
}
