package org.jagentic.pekko.runtime;

import java.util.function.Function;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;

import org.jagentic.core.ConversationStore;
import org.jagentic.core.store.RedisConversationStore;
import org.jagentic.pekko.durability.DurabilityProfile;
import org.jagentic.pekko.durability.WriteThroughConversationEntity;
import org.jagentic.pekko.entity.ConversationEntity;

/** Boots the Pekko {@link ActorSystem} whose guardian routes turns to per-conversation entities.
 * The durability profile chooses the entity flavour: event-sourced (memory/postgres/cassandra —
 * the journal is picked by the active config) or Redis write-through. AutoCloseable for demos/tests. */
public final class PekkoSystem implements AutoCloseable {

  public static final String SYSTEM_NAME = "AgenticPekko";

  private final ActorSystem<ConversationManager.Command> system;

  /** Event-sourced (journal chosen by config). */
  public PekkoSystem(AgentDeps deps) {
    this(ConversationManager.create(deps));
  }

  /** Profile-aware: {@code REDIS} → write-through to {@code redisUrl}; otherwise event-sourced. */
  public PekkoSystem(AgentDeps deps, DurabilityProfile profile, String redisUrl) {
    this(profile.isWriteThrough()
        ? ConversationManager.create(redisFactory(deps, redisUrl))
        : ConversationManager.create(deps));
  }

  private PekkoSystem(Behavior<ConversationManager.Command> guardian) {
    this.system = ActorSystem.create(guardian, SYSTEM_NAME);
  }

  private static Function<String, Behavior<ConversationEntity.Command>> redisFactory(
      AgentDeps deps, String redisUrl) {
    ConversationStore redis = new RedisConversationStore(redisUrl, 200);
    return cid -> WriteThroughConversationEntity.create(cid, deps, redis);
  }

  public ActorSystem<ConversationManager.Command> system() {
    return system;
  }

  @Override
  public void close() {
    system.terminate();
  }
}
