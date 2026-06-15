package org.jagentic.pekko.cluster;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

import org.jagentic.pekko.entity.ConversationEntity;
import org.jagentic.pekko.runtime.AgentDeps;

/** Cluster Sharding wiring — the production distributed single-writer: Pekko guarantees exactly
 * one live {@link ConversationEntity} per {@code conversationId} across the cluster, and migrates
 * it on rebalance/failover (the event-sourced journal makes recovery seamless). This replaces the
 * single-node {@code ConversationManager} when running with {@code provider = cluster} (see
 * {@code application-cluster-*.conf}). The entity protocol is unchanged. */
public final class ConversationSharding {

  public static final EntityTypeKey<ConversationEntity.Command> TYPE_KEY =
      EntityTypeKey.create(ConversationEntity.Command.class, "Conversation");

  private ConversationSharding() {}

  /** Register the entity type with the cluster's shard region. Call once at startup. */
  public static void init(ActorSystem<?> system, AgentDeps deps) {
    ClusterSharding.get(system).init(
        Entity.of(TYPE_KEY, entityCtx -> ConversationEntity.create(entityCtx.getEntityId(), deps)));
  }

  /** A reference to the (sharded) entity for a conversation — created on demand on the owning node. */
  public static EntityRef<ConversationEntity.Command> entityRef(ActorSystem<?> system, String conversationId) {
    return ClusterSharding.get(system).entityRefFor(TYPE_KEY, conversationId);
  }
}
