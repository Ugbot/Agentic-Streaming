package org.jagentic.pekko.persistence.redis;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Refuses to treat a cache-only Redis as a journal. At plugin start we read the server's
 * {@code appendonly} and {@code appendfsync} settings: {@code appendonly} must be {@code yes},
 * otherwise every event is lost on a server restart and the profile fails loudly with the exact
 * {@code redis-server}/{@code CONFIG SET} incantation to fix it.
 *
 * <p>What AOF does and does not promise, so nobody is surprised: with {@code appendfsync always}
 * an acknowledged write is on disk before the entity sees it as persisted (no loss on power
 * failure, slowest). With {@code appendfsync everysec} (Redis' default when AOF is on) an
 * acknowledged write may live up to ~1s only in the OS buffer: a <em>process</em> crash loses
 * nothing (the kernel still flushes), a power loss / kernel panic loses at most the last second
 * of acknowledged turns. With {@code appendfsync no} the loss window is whatever the OS decides
 * (typically up to 30s). Events lost in that window are exactly that: the journal is the truth, so
 * the affected turns are simply not in it and a redelivery re-runs them as new turns. We log the
 * observed {@code appendfsync} at WARN level unless it is {@code always}.</p>
 *
 * <p>Managed offerings (Redis Enterprise, Valkey/ElastiCache with persistence) often disable the
 * {@code CONFIG} command. That is not silently accepted either: the check fails and tells the
 * operator to set {@code durability-check = off} explicitly, which is logged at WARN on every
 * start.</p>
 */
public final class RedisDurabilityCheck {
  private static final Logger LOG = LoggerFactory.getLogger(RedisDurabilityCheck.class);

  private RedisDurabilityCheck() {}

  /** The verified server persistence settings. */
  public record Verified(String appendonly, String appendfsync) {}

  public static Verified verify(Jedis jedis, String pluginId) {
    Map<String, String> cfg;
    try {
      cfg = new HashMap<>(jedis.configGet("appendonly", "appendfsync"));
    } catch (JedisDataException e) {
      throw new IllegalStateException(pluginId + ": cannot verify Redis persistence because CONFIG GET"
          + " is not permitted on this server (" + e.getMessage() + "). If this is a managed Redis with"
          + " persistence enabled by the provider, set '" + pluginId + ".durability-check = off'"
          + " explicitly; otherwise enable CONFIG or point the profile at a Redis you control.", e);
    }
    String appendonly = cfg.getOrDefault("appendonly", "");
    String appendfsync = cfg.getOrDefault("appendfsync", "");
    if (!"yes".equalsIgnoreCase(appendonly)) {
      throw new IllegalStateException(pluginId + ": Redis at this url has appendonly=" + appendonly
          + " so it is a cache, not a journal: every conversation event would be lost on restart."
          + " Start it with 'redis-server --appendonly yes --appendfsync always' (or"
          + " 'CONFIG SET appendonly yes' + 'CONFIG REWRITE'), or choose another durability profile.");
    }
    if (!"always".equalsIgnoreCase(appendfsync)) {
      LOG.warn("{}: Redis appendfsync={} — acknowledged events may sit in the OS buffer before fsync"
          + " (everysec: up to ~1s lost on power failure; no: OS-dependent). Use appendfsync=always"
          + " for no loss window.", pluginId, appendfsync);
    } else {
      LOG.info("{}: Redis appendonly=yes appendfsync=always — every acknowledged event is fsynced", pluginId);
    }
    return new Verified(appendonly, appendfsync);
  }
}
