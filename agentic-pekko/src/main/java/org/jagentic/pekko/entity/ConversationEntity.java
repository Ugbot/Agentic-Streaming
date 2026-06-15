package org.jagentic.pekko.entity;

import java.util.List;
import java.util.Map;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.javadsl.CommandHandler;
import org.apache.pekko.persistence.typed.javadsl.Effect;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;

import org.jagentic.core.AgentContext;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.serialization.CborSerializable;

/**
 * The per-conversation agent as an <b>event-sourced</b> Pekko entity — one instance per
 * {@code conversationId} (single-writer via the mailbox; durable + recoverable via Pekko
 * Persistence). It hosts the Flink-free {@link org.jagentic.core.RoutedGraph} verbatim; only the
 * actor + persistence shell is Pekko.
 *
 * <p>Per turn: the committed transcript/attribute view is wrapped in a {@link RecordingConversationStore}
 * /{@link RecordingKeyedStateStore} overlay that <b>buffers</b> the pipeline's writes, the
 * (synchronous, in Phase 1) {@code graph.handle} runs against it, and the buffered delta is
 * persisted as one {@code TurnCommitted} event. Recovery replays those events to rebuild state
 * <b>without re-invoking the pipeline</b>.</p>
 */
public final class ConversationEntity
    extends EventSourcedBehavior<ConversationEntity.Command, ConversationEntity.Evt, ConversationEntity.State> {

  // ---- protocol ----
  public sealed interface Command extends CborSerializable permits ProcessTurn, GetState {}

  /** Run one turn and reply with the result. */
  public record ProcessTurn(Event event, ActorRef<TurnReply> replyTo) implements Command {}

  /** Read-only state probe (used by tests / admin). */
  public record GetState(ActorRef<StateSnapshot> replyTo) implements Command {}

  public record TurnReply(String conversationId, String reply, String path, boolean ok,
                          List<String> toolCalls) implements CborSerializable {}

  public record StateSnapshot(String conversationId, int messageCount,
                              Map<String, String> attributes) implements CborSerializable {}

  // ---- events ----
  public sealed interface Evt extends CborSerializable permits TurnCommitted {}

  public record TurnCommitted(TurnMutations mutations, String reply, String path, boolean ok)
      implements Evt {}

  // ---- state (materialized view, rebuilt from events) ----
  public static final class State {
    final ConversationStore.InMemory store = new ConversationStore.InMemory();
    final KeyedStateStore.InMemory keyed = new KeyedStateStore.InMemory();
  }

  private final String conversationId;
  private final AgentDeps deps;

  public static Behavior<Command> create(String conversationId, AgentDeps deps) {
    return Behaviors.setup(ctx -> new ConversationEntity(conversationId, deps));
  }

  private ConversationEntity(String conversationId, AgentDeps deps) {
    super(PersistenceId.ofUniqueId("Conversation|" + conversationId));
    this.conversationId = conversationId;
    this.deps = deps;
  }

  @Override
  public State emptyState() {
    return new State();
  }

  @Override
  public CommandHandler<Command, Evt, State> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(ProcessTurn.class, this::onProcessTurn)
        .onCommand(GetState.class, this::onGetState)
        .build();
  }

  private Effect<Evt, State> onProcessTurn(State state, ProcessTurn cmd) {
    TurnMutations.Recorder rec = new TurnMutations.Recorder();
    RecordingConversationStore store = new RecordingConversationStore(state.store, rec);
    RecordingKeyedStateStore keyed = new RecordingKeyedStateStore(state.keyed, rec);
    AgentContext ctx =
        new AgentContext(conversationId, cmd.event().userId(), store, keyed, deps.tools(), deps.retriever());

    // Phase 1: run the turn synchronously. Phase 2 moves graph.handle off the actor thread
    // (blocking dispatcher + pipeToSelf) and stashes concurrent ProcessTurns.
    TurnResult r = deps.graph().handle(cmd.event(), ctx);

    TurnCommitted evt = new TurnCommitted(rec.build(), r.reply, r.path, r.ok);
    TurnReply reply =
        new TurnReply(conversationId, r.reply, r.path, r.ok, List.copyOf(r.toolCalls));
    return Effect().persist(evt).thenReply(cmd.replyTo(), updated -> reply);
  }

  private Effect<Evt, State> onGetState(State state, GetState cmd) {
    StateSnapshot snap =
        new StateSnapshot(conversationId, state.store.messageCount(conversationId),
            state.store.attributes(conversationId));
    return Effect().none().thenReply(cmd.replyTo(), s -> snap);
  }

  @Override
  public EventHandler<State, Evt> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(TurnCommitted.class, (state, evt) -> {
          evt.mutations().applyTo(conversationId, state.store, state.keyed);
          return state;
        })
        .build();
  }
}
