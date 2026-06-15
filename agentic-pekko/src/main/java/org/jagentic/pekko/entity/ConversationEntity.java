package org.jagentic.pekko.entity;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
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
 * {@code conversationId} (single-writer; durable + recoverable). It hosts the Flink-free
 * {@link org.jagentic.core.RoutedGraph} verbatim; only the actor + persistence shell is Pekko.
 *
 * <p><b>Async turn loop (C4):</b> {@code graph.handle} (which blocks on LLM/tool I/O) runs on a
 * blocking dispatcher off the actor thread via {@code pipeToSelf}; while a turn is in flight the
 * entity <b>stashes</b> further {@code ProcessTurn}s, so each conversation still serializes one
 * turn at a time. The committed view is wrapped in {@link RecordingConversationStore}/
 * {@link RecordingKeyedStateStore} overlays that buffer the pipeline's writes; on completion the
 * buffered delta is persisted as one {@code TurnCommitted} event. Recovery replays those events to
 * rebuild state <b>without re-invoking the pipeline</b>. A {@code turnId} dedupes redelivered
 * turns (at-least-once Kafka ingress).</p>
 */
public final class ConversationEntity
    extends EventSourcedBehavior<ConversationEntity.Command, ConversationEntity.Evt, ConversationEntity.State> {

  // ---- protocol ----
  public sealed interface Command extends CborSerializable permits ProcessTurn, GetState, TurnCompleted {}

  /** Run one turn and reply. {@code turnId} makes redelivery idempotent. */
  public record ProcessTurn(String turnId, Event event, ActorRef<TurnReply> replyTo) implements Command {}

  /** Read-only state probe (tests / admin). */
  public record GetState(ActorRef<StateSnapshot> replyTo) implements Command {}

  /** Internal: the async turn finished (sent to self via pipeToSelf). Never crosses the network. */
  public record TurnCompleted(String turnId, ActorRef<TurnReply> replyTo, TurnOutcome outcome,
                              Throwable error) implements Command {}

  public record TurnReply(String conversationId, String reply, String path, boolean ok,
                          List<String> toolCalls) implements CborSerializable {}

  public record StateSnapshot(String conversationId, int messageCount,
                              Map<String, String> attributes) implements CborSerializable {}

  /** The result of running a turn off-thread (internal). */
  public record TurnOutcome(TurnMutations mutations, String reply, String path, boolean ok,
                            List<String> toolCalls) {}

  // ---- events ----
  public sealed interface Evt extends CborSerializable permits TurnCommitted {}

  public record TurnCommitted(String turnId, TurnMutations mutations, String reply, String path,
                              boolean ok) implements Evt {}

  // ---- state (materialized view, rebuilt from events) ----
  public static final class State {
    final ConversationStore.InMemory store = new ConversationStore.InMemory();
    final KeyedStateStore.InMemory keyed = new KeyedStateStore.InMemory();
    String lastTurnId;
    String lastReply;
    String lastPath;
    boolean lastOk = true;
  }

  private final ActorContext<Command> context;
  private final String conversationId;
  private final AgentDeps deps;
  private final Executor blockingExecutor;
  private boolean inFlight = false; // runtime-only; reset on (re)start, which is correct

  public static Behavior<Command> create(String conversationId, AgentDeps deps) {
    return Behaviors.setup(ctx -> new ConversationEntity(ctx, conversationId, deps));
  }

  private ConversationEntity(ActorContext<Command> context, String conversationId, AgentDeps deps) {
    super(PersistenceId.ofUniqueId("Conversation|" + conversationId));
    this.context = context;
    this.conversationId = conversationId;
    this.deps = deps;
    // Pekko's built-in blocking dispatcher — keeps LLM/tool I/O off the actor (and shard) thread.
    this.blockingExecutor = context.getSystem().dispatchers().lookup(DispatcherSelector.blocking());
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
        .onCommand(TurnCompleted.class, this::onTurnCompleted)
        .onCommand(GetState.class, this::onGetState)
        .build();
  }

  private Effect<Evt, State> onProcessTurn(State state, ProcessTurn cmd) {
    // Idempotency: a redelivered turn returns the prior reply without re-running the pipeline.
    if (cmd.turnId() != null && cmd.turnId().equals(state.lastTurnId)) {
      TurnReply dup = new TurnReply(conversationId, state.lastReply, state.lastPath, state.lastOk, List.of());
      return Effect().none().thenReply(cmd.replyTo(), s -> dup);
    }
    // One turn at a time per conversation: stash while a turn is in flight.
    if (inFlight) {
      return Effect().stash();
    }
    inFlight = true;
    CompletableFuture<TurnOutcome> future =
        CompletableFuture.supplyAsync(() -> runTurn(state, cmd), blockingExecutor);
    context.pipeToSelf(future,
        (outcome, error) -> new TurnCompleted(cmd.turnId(), cmd.replyTo(), outcome, error));
    return Effect().none();
  }

  /** Runs OFF the actor thread: reads the committed view (thread-safe InMemory), buffers writes. */
  private TurnOutcome runTurn(State state, ProcessTurn cmd) {
    TurnMutations.Recorder rec = new TurnMutations.Recorder();
    RecordingConversationStore store = new RecordingConversationStore(state.store, rec);
    RecordingKeyedStateStore keyed = new RecordingKeyedStateStore(state.keyed, rec);
    AgentContext ctx =
        new AgentContext(conversationId, cmd.event().userId(), store, keyed, deps.tools(), deps.retriever());
    TurnResult r = deps.graph().handle(cmd.event(), ctx);
    return new TurnOutcome(rec.build(), r.reply, r.path, r.ok, List.copyOf(r.toolCalls));
  }

  private Effect<Evt, State> onTurnCompleted(State state, TurnCompleted c) {
    inFlight = false;
    if (c.error() != null || c.outcome() == null) {
      String msg = c.error() == null ? "turn failed" : c.error().getMessage();
      TurnReply err = new TurnReply(conversationId, "[error] " + msg, "error", false, List.of());
      // nothing persisted on failure — committed view stays clean
      return Effect().none().thenReply(c.replyTo(), s -> err).thenUnstashAll();
    }
    TurnOutcome o = c.outcome();
    TurnCommitted evt = new TurnCommitted(c.turnId(), o.mutations(), o.reply(), o.path(), o.ok());
    TurnReply reply = new TurnReply(conversationId, o.reply(), o.path(), o.ok(), o.toolCalls());
    return Effect().persist(evt).thenReply(c.replyTo(), s -> reply).thenUnstashAll();
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
          state.lastTurnId = evt.turnId();
          state.lastReply = evt.reply();
          state.lastPath = evt.path();
          state.lastOk = evt.ok();
          return state;
        })
        .build();
  }
}
