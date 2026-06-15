package org.jagentic.pekko.runtime;

import java.util.HashMap;
import java.util.Map;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.serialization.CborSerializable;

/** Local get-or-spawn router for conversation entities (one child per conversationId) — the
 * single-node stand-in for Cluster Sharding (which replaces it in the cluster phase). Forwards
 * each {@link Envelope}'s inner command to the right entity. */
public final class ConversationManager {

  public interface Command {}

  /** Route a command to the entity for {@code conversationId}. */
  public record Envelope(String conversationId, ConversationEntity.Command command)
      implements Command, CborSerializable {}

  private ConversationManager() {}

  public static Behavior<Command> create(AgentDeps deps) {
    return Behaviors.setup(ctx -> {
      Map<String, ActorRef<ConversationEntity.Command>> children = new HashMap<>();
      return Behaviors.receive(Command.class)
          .onMessage(Envelope.class, env -> {
            ActorRef<ConversationEntity.Command> child =
                children.computeIfAbsent(env.conversationId(),
                    cid -> ctx.spawn(ConversationEntity.create(cid, deps), "conv-" + sanitize(cid)));
            child.tell(env.command());
            return Behaviors.same();
          })
          .build();
    });
  }

  private static String sanitize(String cid) {
    return cid.replaceAll("[^a-zA-Z0-9_-]", "_");
  }
}
