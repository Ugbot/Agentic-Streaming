package org.jagentic.pekko.cluster;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.ConversationManager;

/** Cluster Sharding wiring — the production distributed single-writer: Pekko guarantees exactly
 * one live {@link ConversationEntity} per {@code conversationId} across the cluster, and migrates
 * it on rebalance/failover (the event-sourced journal makes recovery seamless). This replaces the
 * single-node {@code ConversationManager} when running with {@code provider = cluster} (see
 * {@code application-cluster-*.conf}). The entity protocol is unchanged: {@link #router} speaks
 * the same {@link ConversationManager.Command} envelopes as the local router, so every front door
 * ({@code PekkoRuntime}, HTTP, Kafka) runs unmodified on a sharded system
 * ({@code PekkoSystem.clustered}, {@code ClusterMain}). */
public final class ConversationSharding {

  public static final EntityTypeKey<ConversationEntity.Command> TYPE_KEY =
      EntityTypeKey.create(ConversationEntity.Command.class, "Conversation");

  private ConversationSharding() {}

  /** Register the entity type with the cluster's shard region. Call once at startup. */
  public static void init(ActorSystem<?> system, AgentDeps deps) {
    ClusterSharding.get(system).init(
        Entity.of(TYPE_KEY, entityCtx ->
            ConversationManager.supervised(ConversationEntity.create(entityCtx.getEntityId(), deps,
                self -> entityCtx.getShard().tell(new ClusterSharding.Passivate<>(self)))))
            .withStopMessage(new ConversationEntity.Stop()));
  }

  /** A reference to the (sharded) entity for a conversation — created on demand on the owning node. */
  public static EntityRef<ConversationEntity.Command> entityRef(ActorSystem<?> system, String conversationId) {
    return ClusterSharding.get(system).entityRefFor(TYPE_KEY, conversationId);
  }

  /**
   * Guardian for a clustered system: routes each {@link ConversationManager.Envelope} to the sharded
   * entity of its conversation and each {@link ConversationManager.Passivate} to that entity's
   * {@link ConversationEntity.Passivate}, so the shard region, not a heap map, decides which node
   * hosts a conversation. Registers the entity type ({@link #init}) as it starts.
   */
  public static Behavior<ConversationManager.Command> router(AgentDeps deps) {
    return Behaviors.setup(ctx -> {
      init(ctx.getSystem(), deps);
      return Behaviors.receive(ConversationManager.Command.class)
        .onMessage(ConversationManager.Envelope.class, env -> {
          entityRef(ctx.getSystem(), env.conversationId()).tell(env.command());
          return Behaviors.same();
        })
        .onMessage(ConversationManager.Passivate.class, p -> {
          entityRef(ctx.getSystem(), p.conversationId()).tell(new ConversationEntity.Passivate(p.ack()));
          return Behaviors.same();
        })
        .build();
    });
  }
}
