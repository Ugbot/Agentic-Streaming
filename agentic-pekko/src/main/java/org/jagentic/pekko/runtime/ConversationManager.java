package org.jagentic.pekko.runtime;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.Terminated;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.serialization.CborSerializable;

/**
 * Local get-or-spawn router for conversation entities (one supervised child per conversationId):
 * the single-node stand-in for Cluster Sharding. Children are supervised with backoff restarts, so
 * a crashing entity is recreated over its journal rather than lost. {@link Passivate} stops an
 * entity and acknowledges once it is gone; the next envelope recreates it from the journal, which is
 * how tests and demos exercise entity restart and replay.
 */
public final class ConversationManager {

  public interface Command {}

  /** Route a command to the entity for {@code conversationId}. */
  public record Envelope(String conversationId, ConversationEntity.Command command)
      implements Command, CborSerializable {}

  /** Stop the live entity for {@code conversationId} (if any) and acknowledge once it has terminated. */
  public record Passivate(String conversationId, ActorRef<Done> ack) implements Command, CborSerializable {}

  public static final Duration MIN_BACKOFF = Duration.ofMillis(100);
  public static final Duration MAX_BACKOFF = Duration.ofSeconds(10);

  private ConversationManager() {}

  public static Behavior<Command> create(AgentDeps deps) {
    return create(cid -> ConversationEntity.create(cid, deps));
  }

  /** Route to entities built by {@code entityFactory}; each child is wrapped in {@link #supervised}. */
  public static Behavior<Command> create(Function<String, Behavior<ConversationEntity.Command>> entityFactory) {
    return Behaviors.setup(ctx -> new Router(ctx, entityFactory).behavior());
  }

  /** The supervision every conversation entity runs under: restart with backoff on any failure. */
  public static Behavior<ConversationEntity.Command> supervised(Behavior<ConversationEntity.Command> entity) {
    return Behaviors.supervise(entity)
        .onFailure(SupervisorStrategy.restartWithBackoff(MIN_BACKOFF, MAX_BACKOFF, 0.2));
  }

  private static final class Router {
    private final ActorContext<Command> ctx;
    private final Function<String, Behavior<ConversationEntity.Command>> factory;
    private final Map<String, ActorRef<ConversationEntity.Command>> children = new HashMap<>();
    private final Map<ActorRef<ConversationEntity.Command>, String> owners = new HashMap<>();
    private final Map<String, Deque<ActorRef<Done>>> passivating = new HashMap<>();
    private final Map<String, Integer> incarnations = new HashMap<>();

    Router(ActorContext<Command> ctx, Function<String, Behavior<ConversationEntity.Command>> factory) {
      this.ctx = ctx;
      this.factory = factory;
    }

    Behavior<Command> behavior() {
      return Behaviors.receive(Command.class)
          .onMessage(Envelope.class, this::onEnvelope)
          .onMessage(Passivate.class, this::onPassivate)
          .onSignal(Terminated.class, this::onTerminated)
          .build();
    }

    private Behavior<Command> onEnvelope(Envelope env) {
      children.computeIfAbsent(env.conversationId(), this::spawn).tell(env.command());
      return Behaviors.same();
    }

    private Behavior<Command> onPassivate(Passivate p) {
      ActorRef<ConversationEntity.Command> child = children.get(p.conversationId());
      if (child == null) {
        p.ack().tell(Done.getInstance());
        return Behaviors.same();
      }
      passivating.computeIfAbsent(p.conversationId(), k -> new ArrayDeque<>()).add(p.ack());
      ctx.stop(child);
      return Behaviors.same();
    }

    private Behavior<Command> onTerminated(Terminated t) {
      @SuppressWarnings("unchecked")
      ActorRef<ConversationEntity.Command> ref = (ActorRef<ConversationEntity.Command>) (ActorRef<?>) t.getRef();
      String cid = owners.remove(ref);
      if (cid != null) {
        children.remove(cid, ref);
        Deque<ActorRef<Done>> acks = passivating.remove(cid);
        if (acks != null) {
          acks.forEach(a -> a.tell(Done.getInstance()));
        }
      }
      return Behaviors.same();
    }

    private ActorRef<ConversationEntity.Command> spawn(String cid) {
      int n = incarnations.merge(cid, 1, Integer::sum);
      ActorRef<ConversationEntity.Command> ref =
          ctx.spawn(supervised(factory.apply(cid)), "conv-" + sanitize(cid) + "-" + n);
      ctx.watch(ref);
      owners.put(ref, cid);
      return ref;
    }
  }

  private static String sanitize(String cid) {
    return cid.replaceAll("[^a-zA-Z0-9_-]", "_");
  }
}
