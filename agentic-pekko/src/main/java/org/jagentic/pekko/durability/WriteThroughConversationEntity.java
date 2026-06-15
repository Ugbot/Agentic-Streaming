package org.jagentic.pekko.durability;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.StashBuffer;

import org.jagentic.core.AgentContext;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.TurnResult;
import org.jagentic.pekko.entity.ConversationEntity.Command;
import org.jagentic.pekko.entity.ConversationEntity.GetState;
import org.jagentic.pekko.entity.ConversationEntity.ProcessTurn;
import org.jagentic.pekko.entity.ConversationEntity.StateSnapshot;
import org.jagentic.pekko.entity.ConversationEntity.TurnCompleted;
import org.jagentic.pekko.entity.ConversationEntity.TurnOutcome;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.runtime.AgentDeps;

/** The Redis (write-through) durability profile: a non-event-sourced entity that runs turns
 * directly against a durable {@link ConversationStore} (e.g. jagentic-core's RedisConversationStore),
 * so the conversation transcript lives in Redis. Speaks the SAME {@link Command} protocol as the
 * event-sourced {@code ConversationEntity}, so the manager / runtime / HTTP / Kafka layers are
 * unchanged. Single-writer is preserved by sharding + the in-flight stash; turns run off the actor
 * thread (pipeToSelf). Keyed scalar state is per-actor (in-memory) — the transcript is the durable
 * part. There is no maintained Pekko Redis journal, which is why this is write-through rather than
 * event-sourced. */
public final class WriteThroughConversationEntity extends AbstractBehavior<Command> {

  private final StashBuffer<Command> stash;
  private final String conversationId;
  private final AgentDeps deps;
  private final ConversationStore store; // durable (shared)
  private final KeyedStateStore keyed = new KeyedStateStore.InMemory();
  private final Executor blockingExecutor;
  private boolean inFlight = false;
  private String lastTurnId;
  private String lastReply;
  private String lastPath;
  private boolean lastOk = true;

  public static Behavior<Command> create(String conversationId, AgentDeps deps, ConversationStore store) {
    return Behaviors.withStash(128, stash ->
        Behaviors.setup(ctx -> new WriteThroughConversationEntity(ctx, stash, conversationId, deps, store)));
  }

  private WriteThroughConversationEntity(ActorContext<Command> ctx, StashBuffer<Command> stash,
                                         String conversationId, AgentDeps deps, ConversationStore store) {
    super(ctx);
    this.stash = stash;
    this.conversationId = conversationId;
    this.deps = deps;
    this.store = store;
    this.blockingExecutor = ctx.getSystem().dispatchers().lookup(DispatcherSelector.blocking());
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(ProcessTurn.class, this::onProcessTurn)
        .onMessage(TurnCompleted.class, this::onTurnCompleted)
        .onMessage(GetState.class, this::onGetState)
        .build();
  }

  private Behavior<Command> onProcessTurn(ProcessTurn cmd) {
    if (cmd.turnId() != null && cmd.turnId().equals(lastTurnId)) {
      cmd.replyTo().tell(new TurnReply(conversationId, lastReply, lastPath, lastOk, List.of()));
      return this;
    }
    if (inFlight) {
      stash.stash(cmd);
      return this;
    }
    inFlight = true;
    CompletableFuture<TurnOutcome> future = CompletableFuture.supplyAsync(() -> runTurn(cmd), blockingExecutor);
    getContext().pipeToSelf(future, (outcome, error) -> new TurnCompleted(cmd.turnId(), cmd.replyTo(), outcome, error));
    return this;
  }

  private TurnOutcome runTurn(ProcessTurn cmd) {
    // write-through: graph.handle writes the transcript straight to the durable store.
    AgentContext ctx = new AgentContext(conversationId, cmd.event().userId(), store, keyed, deps.tools(), deps.retriever());
    TurnResult r = deps.graph().handle(cmd.event(), ctx);
    return new TurnOutcome(null, r.reply, r.path, r.ok, List.copyOf(r.toolCalls));
  }

  private Behavior<Command> onTurnCompleted(TurnCompleted c) {
    inFlight = false;
    if (c.error() != null || c.outcome() == null) {
      String msg = c.error() == null ? "turn failed" : c.error().getMessage();
      c.replyTo().tell(new TurnReply(conversationId, "[error] " + msg, "error", false, List.of()));
      return stash.unstashAll(this);
    }
    TurnOutcome o = c.outcome();
    lastTurnId = c.turnId();
    lastReply = o.reply();
    lastPath = o.path();
    lastOk = o.ok();
    c.replyTo().tell(new TurnReply(conversationId, o.reply(), o.path(), o.ok(), o.toolCalls()));
    return stash.unstashAll(this);
  }

  private Behavior<Command> onGetState(GetState c) {
    c.replyTo().tell(new StateSnapshot(conversationId, store.messageCount(conversationId),
        store.attributes(conversationId)));
    return this;
  }
}
