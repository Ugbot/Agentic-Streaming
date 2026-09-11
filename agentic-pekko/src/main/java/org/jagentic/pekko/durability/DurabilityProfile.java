package org.jagentic.pekko.durability;

import java.util.Locale;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.jagentic.pekko.persistence.redis.RedisJournal;

/**
 * Where the conversation journal durably lives. Every profile is event-sourced: the entity is
 * journal-agnostic, so a profile is exactly a Pekko configuration selecting
 * {@code pekko.persistence.journal.plugin} (and the actor provider that goes with it). There is no
 * profile that keeps state outside the journal, because the journal is the spec's event log.
 */
public enum DurabilityProfile {
  /** In-memory journal, local actor provider: dev/test, not durable across a JVM restart. */
  MEMORY("application.conf", "pekko.persistence.journal.inmem", null),
  /** Cluster + Postgres via pekko-persistence-jdbc; needs {@code AGENTIC_PG_URL} (and user/password). */
  POSTGRES("application-cluster-jdbc.conf", "jdbc-journal", "pekko.persistence.jdbc.shared-databases.default.db.url"),
  /** Cluster + Cassandra via pekko-persistence-cassandra. */
  CASSANDRA("application-cluster-cassandra.conf", "pekko.persistence.cassandra.journal",
      "datastax-java-driver.basic.contact-points"),
  /**
   * Cluster + Redis via this module's {@code RedisJournal}; needs {@code AGENTIC_REDIS_URL} and a
   * Redis with {@code appendonly yes} (verified at start, see {@code RedisDurabilityCheck}).
   */
  REDIS("application-redis.conf", "agentic-redis-journal", "agentic-redis-journal.url");

  public static final String ENV_VAR = "AGENTIC_PEKKO_DURABILITY";

  private final String resource;
  private final String journalPlugin;
  private final String requiredPath;

  DurabilityProfile(String resource, String journalPlugin, String requiredPath) {
    this.resource = resource;
    this.journalPlugin = journalPlugin;
    this.requiredPath = requiredPath;
  }

  /** The classpath resource this profile is defined in. */
  public String resource() {
    return resource;
  }

  /** The journal plugin id this profile must resolve to. */
  public String journalPlugin() {
    return journalPlugin;
  }

  /**
   * The fully resolved Pekko configuration for this profile, with system properties layered on top
   * and the reference configuration underneath.
   *
   * @throws IllegalStateException when the profile is selected but the connection it needs is not
   *     configured, or the resource does not select the journal this profile stands for
   */
  public Config config() {
    Config c = ConfigFactory.defaultOverrides()
        .withFallback(ConfigFactory.parseResourcesAnySyntax(resource))
        .withFallback(ConfigFactory.defaultReference())
        .resolve();
    String actual = c.getString("pekko.persistence.journal.plugin");
    if (!journalPlugin.equals(actual)) {
      throw new IllegalStateException("profile " + this + " (" + resource + ") selects journal " + actual
          + " but must select " + journalPlugin);
    }
    if (requiredPath != null && !c.hasPath(requiredPath)) {
      throw new IllegalStateException("profile " + this + " needs " + requiredPath + " (set "
          + connectionHint() + "); refusing to start without a durable journal");
    }
    return c;
  }

  /**
   * Profile-specific checks that must pass before the actor system boots, run against the
   * configuration that will actually be used. The journal plugins repeat these checks when they
   * start; running them here first turns a misconfigured store into a synchronous, actionable
   * failure instead of an actor-initialization error followed by a recovery timeout.
   *
   * @throws IllegalStateException when the store is unreachable or not durable (REDIS: server
   *     has {@code appendonly no}, or {@code CONFIG} is forbidden and the check was not explicitly
   *     disabled)
   */
  public void preflight(Config resolved) {
    if (this == REDIS) {
      RedisJournal.preflight(resolved.getConfig(journalPlugin), journalPlugin);
    }
  }

  private String connectionHint() {
    return switch (this) {
      case POSTGRES -> "AGENTIC_PG_URL, AGENTIC_PG_USER and AGENTIC_PG_PASSWORD";
      case CASSANDRA -> "datastax-java-driver.basic.contact-points";
      case REDIS -> "AGENTIC_REDIS_URL, e.g. redis://localhost:6379/0";
      case MEMORY -> "nothing";
    };
  }

  /** Parses a profile name; blank or null means {@link #MEMORY}. Unknown names are rejected. */
  public static DurabilityProfile from(String value) {
    if (value == null || value.isBlank()) {
      return MEMORY;
    }
    try {
      return DurabilityProfile.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("unknown durability profile '" + value + "'; one of "
          + java.util.Arrays.toString(values()), e);
    }
  }

  /** The profile named by {@code AGENTIC_PEKKO_DURABILITY}, defaulting to {@link #MEMORY}. */
  public static DurabilityProfile fromEnvironment() {
    return from(System.getenv(ENV_VAR));
  }
}
