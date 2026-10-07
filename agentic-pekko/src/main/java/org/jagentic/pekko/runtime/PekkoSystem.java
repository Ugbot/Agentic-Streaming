package org.jagentic.pekko.runtime;

import java.time.Duration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;

import org.jagentic.pekko.cluster.ConversationSharding;
import org.jagentic.pekko.durability.DurabilityProfile;

/**
 * Boots the Pekko {@link ActorSystem} whose guardian routes turns to per-conversation entities.
 * The {@link DurabilityProfile} chooses the journal (memory, Postgres, Cassandra, Redis) purely by
 * configuration; the entity code is identical under every profile. The constructors boot the
 * single-node system (local {@link ConversationManager} router); {@link #clustered} boots a cluster
 * node whose guardian is the Cluster Sharding router, so the same {@link PekkoRuntime} and front
 * doors run either way. AutoCloseable for demos/tests.
 */
public final class PekkoSystem implements AutoCloseable {
  public static final String SYSTEM_NAME = "AgenticPekko";

  /** How long {@link #clustered} waits for this node to reach {@link MemberStatus#up()}. */
  public static final Duration CLUSTER_JOIN_TIMEOUT = Duration.ofSeconds(30);

  private final ActorSystem<ConversationManager.Command> system;
  private final DurabilityProfile profile;
  private final boolean sharded;

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
    this(profile, ConversationManager.create(deps), config, false);
  }

  private PekkoSystem(DurabilityProfile profile, Behavior<ConversationManager.Command> guardian, Config config,
                      boolean sharded) {
    this.profile = profile;
    this.sharded = sharded;
    profile.preflight(config);
    this.system = ActorSystem.create(guardian, SYSTEM_NAME, config);
  }

  /**
   * Boots a cluster node on the profile's configuration: {@code pekko.actor.provider} is forced to
   * {@code cluster}, the node joins {@code pekko.cluster.seed-nodes} (or itself when none are
   * configured, the single-node cluster), the {@code Conversation} entity type is registered with
   * the shard region and every turn is routed through it. Blocks until the member is up.
   *
   * @param overrides configuration layered over the profile's, for example {@code AGENTIC_PEKKO_PORT}
   *     as {@code pekko.remote.artery.canonical.port}
   * @throws IllegalStateException when the profile's journal is not configured or reachable, or the
   *     member does not reach {@code Up} within {@link #CLUSTER_JOIN_TIMEOUT}
   */
  public static PekkoSystem clustered(AgentDeps deps, DurabilityProfile profile, Config overrides) {
    Config config = ConfigFactory.parseString("pekko.actor.provider = cluster")
        .withFallback(overrides)
        .withFallback(profile.config())
        .resolve();
    PekkoSystem sys = new PekkoSystem(profile, ConversationSharding.router(deps), config, true);
    Cluster cluster = Cluster.get(sys.system);
    if (config.getStringList("pekko.cluster.seed-nodes").isEmpty()) {
      cluster.manager().tell(Join.create(cluster.selfMember().address()));
    }
    long deadline = System.nanoTime() + CLUSTER_JOIN_TIMEOUT.toNanos();
    while (!MemberStatus.up().equals(cluster.selfMember().status())) {
      if (System.nanoTime() > deadline) {
        sys.close();
        throw new IllegalStateException("cluster member " + cluster.selfMember().address() + " did not reach Up within "
            + CLUSTER_JOIN_TIMEOUT + " (status " + cluster.selfMember().status() + "); seed-nodes="
            + config.getStringList("pekko.cluster.seed-nodes"));
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        sys.close();
        throw new IllegalStateException("interrupted while joining the cluster", e);
      }
    }
    return sys;
  }

  /** The profile's configuration with no overrides; see {@link #clustered(AgentDeps, DurabilityProfile, Config)}. */
  public static PekkoSystem clustered(AgentDeps deps, DurabilityProfile profile) {
    return clustered(deps, profile, ConfigFactory.empty());
  }

  /** {@code true} when turns route through Cluster Sharding rather than the local router. */
  public boolean sharded() {
    return sharded;
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
