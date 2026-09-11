package org.jagentic.pekko.testing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.jagentic.pekko.durability.DurabilityProfile;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A real Redis for the {@code @Tag("integration")} tests, started through Testcontainers against
 * the Podman socket ({@code DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock}; Ryuk must be
 * disabled for rootless Podman: {@code TESTCONTAINERS_RYUK_DISABLED=true}). Fails loudly when no
 * container engine is reachable rather than skipping.
 */
public final class RedisContainer extends GenericContainer<RedisContainer> {
  public static final DockerImageName IMAGE = DockerImageName.parse("docker.io/library/redis:7.4");

  /** Persistent Redis: {@code appendonly yes}, {@code appendfsync always}. */
  public static RedisContainer durable() {
    return new RedisContainer().withCommand("redis-server", "--appendonly", "yes", "--appendfsync", "always");
  }

  /** Cache-only Redis: the journal must refuse it. */
  public static RedisContainer cacheOnly() {
    return new RedisContainer().withCommand("redis-server", "--appendonly", "no", "--save", "");
  }

  private RedisContainer() {
    super(IMAGE);
    withExposedPorts(6379);
  }

  public String url() {
    return "redis://" + getHost() + ":" + getMappedPort(6379) + "/0";
  }

  /**
   * The REDIS profile's configuration pointed at this container, single-node local provider
   * (the resource selects the cluster provider for production), keys isolated by {@code prefix}.
   */
  public Config profileConfig(String prefix) {
    return ConfigFactory.parseString(String.join("\n",
            "agentic-redis-journal.url = \"" + url() + "\"",
            "agentic-redis-journal.key-prefix = \"" + prefix + "\"",
            "pekko.actor.provider = local",
            "pekko.persistence.snapshot-store.local.dir = \"target/pekko-snapshots-" + prefix + "\""))
        .withFallback(ConfigFactory.parseResourcesAnySyntax(DurabilityProfile.REDIS.resource()))
        .withFallback(ConfigFactory.defaultReference())
        .resolve();
  }
}
