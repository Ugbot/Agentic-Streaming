package org.jagentic.pekko.runtime;

import com.typesafe.config.Config;

import org.apache.pekko.actor.typed.ActorSystem;

import org.jagentic.pekko.durability.DurabilityProfile;

/**
 * Boots the Pekko {@link ActorSystem} whose guardian routes turns to per-conversation entities.
 * The {@link DurabilityProfile} chooses the journal (memory, Postgres, Cassandra, Redis) purely by
 * configuration; the entity code is identical under every profile. AutoCloseable for demos/tests.
 */
public final class PekkoSystem implements AutoCloseable {
  public static final String SYSTEM_NAME = "AgenticPekko";

  private final ActorSystem<ConversationManager.Command> system;
  private final DurabilityProfile profile;

  /** The {@link DurabilityProfile#MEMORY} profile. */
  public PekkoSystem(AgentDeps deps) {
    this(deps, DurabilityProfile.MEMORY);
  }

  /** Boots with the profile's resolved configuration; fails clearly when the profile's journal is not configured. */
  public PekkoSystem(AgentDeps deps, DurabilityProfile profile) {
    this(deps, profile, profile.config());
  }

  /**
   * Boots with an explicit configuration (tests that layer their own overrides on a profile). The
   * profile's {@link DurabilityProfile#preflight preflight} runs first, so a store that is
   * unreachable or not durable fails here, synchronously.
   */
  public PekkoSystem(AgentDeps deps, DurabilityProfile profile, Config config) {
    this.profile = profile;
    profile.preflight(config);
    this.system = ActorSystem.create(ConversationManager.create(deps), SYSTEM_NAME, config);
  }

  public ActorSystem<ConversationManager.Command> system() {
    return system;
  }

  public DurabilityProfile profile() {
    return profile;
  }

  /** The journal plugin the running system persists to. */
  public String journalPlugin() {
    return system.settings().config().getString("pekko.persistence.journal.plugin");
  }

  @Override
  public void close() {
    system.terminate();
  }
}
