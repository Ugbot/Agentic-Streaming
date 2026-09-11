package org.jagentic.pekko.persistence.redis;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.typesafe.config.Config;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;

/**
 * Settings shared by {@link RedisJournal} and {@link RedisSnapshotStore}, parsed from the plugin's
 * config block ({@code agentic-redis-journal} / {@code agentic-redis-snapshot-store} in
 * {@code reference.conf}).
 *
 * <p>{@code url} is a {@code redis://[:password@]host[:port][/db]} URI and is mandatory: a plugin
 * without one refuses to start, exactly like the JDBC profile without {@code AGENTIC_PG_URL}.</p>
 */
public record RedisSettings(
    URI url,
    String keyPrefix,
    int replayBatch,
    boolean durabilityCheck,
    int connectTimeoutMs) {

  public static final String URL_PATH = "url";

  public static RedisSettings from(Config c) {
    if (!c.hasPath(URL_PATH) || c.getString(URL_PATH).isBlank()) {
      throw new IllegalStateException(
          "redis persistence plugin needs '" + URL_PATH + "' (set AGENTIC_REDIS_URL, e.g."
              + " redis://localhost:6379/0); refusing to start without a durable journal");
    }
    URI url = URI.create(c.getString(URL_PATH).trim());
    if (url.getHost() == null || !("redis".equals(url.getScheme()) || "rediss".equals(url.getScheme()))) {
      throw new IllegalStateException("redis url must look like redis://host:port[/db], got " + url);
    }
    int batch = c.getInt("replay-batch");
    if (batch <= 0) {
      throw new IllegalStateException("replay-batch must be positive, got " + batch);
    }
    return new RedisSettings(
        url,
        c.getString("key-prefix"),
        batch,
        c.getBoolean("durability-check"),
        (int) c.getDuration("connect-timeout").toMillis());
  }

  public int port() {
    return url.getPort() < 0 ? 6379 : url.getPort();
  }

  public int database() {
    String path = url.getPath();
    if (path == null || path.isBlank() || "/".equals(path)) {
      return 0;
    }
    return Integer.parseInt(path.substring(1));
  }

  public String password() {
    String info = url.getUserInfo();
    if (info == null) {
      return null;
    }
    int colon = info.indexOf(':');
    return colon < 0 ? info : info.substring(colon + 1);
  }

  public JedisPool newPool() {
    DefaultJedisClientConfig.Builder cfg = DefaultJedisClientConfig.builder()
        .database(database())
        .connectionTimeoutMillis(connectTimeoutMs)
        .socketTimeoutMillis(connectTimeoutMs)
        .ssl("rediss".equals(url.getScheme()));
    String pw = password();
    if (pw != null && !pw.isEmpty()) {
      cfg.password(pw);
    }
    return new JedisPool(new HostAndPort(url.getHost(), port()), cfg.build());
  }

  byte[] journalKey(String persistenceId) {
    return (keyPrefix + ":j:" + persistenceId).getBytes(StandardCharsets.UTF_8);
  }

  byte[] highestKey(String persistenceId) {
    return (keyPrefix + ":hi:" + persistenceId).getBytes(StandardCharsets.UTF_8);
  }

  byte[] snapshotKey(String persistenceId) {
    return (keyPrefix + ":s:" + persistenceId).getBytes(StandardCharsets.UTF_8);
  }
}
