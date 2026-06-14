package org.jagentic.ports.pekko;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;
import org.jagentic.core.Event;

/**
 * Cluster Sharding wiring: one {@link ConversationActor} entity per
 * {@code conversationId}, distributed across the cluster, with Pekko guaranteeing
 * a <b>single live instance</b> per id (single-writer-per-conversation, C2) and
 * location transparency. This is the actor-model equivalent of Flink's keyBy: the
 * entity id is the conversation key, the entity's fields are the keyed state (C1),
 * and Pekko routes each message to the one shard hosting that entity.
 *
 * <p>{@link #ask} sends a turn to the right entity and returns a non-blocking
 * {@link CompletionStage} (C4).
 */
public final class BankingSharding {

  public static final EntityTypeKey<ConversationActor.Command> TYPE_KEY =
      EntityTypeKey.create(ConversationActor.Command.class, "Conversation");

  private BankingSharding() {}

  /** Register the entity type with the cluster's sharding extension. Call once at
   * startup (requires {@code pekko.actor.provider = cluster}). */
  public static void init(ActorSystem<?> system) {
    ClusterSharding.get(system)
        .init(Entity.of(TYPE_KEY, entityCtx -> ConversationActor.create(entityCtx.getEntityId())));
  }

  /** Route a turn to the entity for {@code event.conversationId()} and await its reply. */
  public static CompletionStage<ConversationActor.Reply> ask(
      ActorSystem<?> system, Event event, Duration timeout) {
    EntityRef<ConversationActor.Command> ref =
        ClusterSharding.get(system).entityRefFor(TYPE_KEY, event.conversationId());
    return ref.ask(replyTo -> new ConversationActor.ProcessTurn(event, replyTo), timeout);
  }
}
