package org.jagentic.pekko.persistence.redis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import com.typesafe.config.Config;

import org.apache.pekko.dispatch.Futures;
import org.apache.pekko.persistence.AtomicWrite;
import org.apache.pekko.persistence.PersistentRepr;
import org.apache.pekko.persistence.journal.japi.AsyncWriteJournal;
import org.apache.pekko.serialization.Serialization;
import org.apache.pekko.serialization.SerializationExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.jdk.javaapi.CollectionConverters;

/**
 * A Pekko persistence journal over Redis: the journal <em>is</em> the spec event log, one hash
 * per {@code persistenceId}.
 *
 * <h2>Key layout</h2>
 * <pre>
 *   {prefix}:j:{persistenceId}   HASH   field = sequenceNr (decimal), value = serialized PersistentRepr
 *   {prefix}:hi:{persistenceId}  STRING highest sequenceNr ever written (never decreases, survives deletes)
 * </pre>
 *
 * <h2>Semantics</h2>
 * <ul>
 *   <li><b>Atomic appends</b>: every {@link AtomicWrite} (one {@code persist} or one
 *       {@code persistAll}) is applied by a single Lua script, so all of its events become visible
 *       together or not at all. The script refuses to overwrite an existing sequence number,
 *       which turns a second writer for the same conversation into a failed write instead of a
 *       silently rewritten log.</li>
 *   <li><b>Monotonic sequence numbers</b>: {@code highestSequenceNr} is the max ever written and
 *       is kept in its own key so {@code deleteMessagesTo} never makes the entity reuse a number.</li>
 *   <li><b>Replay</b> walks {@code [from, min(to, highest)]} in {@code replay-batch}-sized
 *       {@code HMGET}s, skipping deleted fields, honouring {@code max}.</li>
 *   <li><b>deleteMessagesTo</b> removes the hash fields {@code <= toSequenceNr}; the highest
 *       marker stays.</li>
 * </ul>
 *
 * <p>Redis calls are blocking Jedis calls run on this plugin's dispatcher
 * ({@code plugin-dispatcher}), never on the entity's thread. Durability is verified once at start
 * by {@link RedisDurabilityCheck}: a server with {@code appendonly no} makes the plugin, and
 * therefore the actor system, refuse to start.</p>
 */
public final class RedisJournal extends AsyncWriteJournal {
  private static final Logger LOG = LoggerFactory.getLogger(RedisJournal.class);

  /** KEYS[1]=journal hash, KEYS[2]=highest key; ARGV=(seq, bytes)*; returns 1 on success, 0 if any seq exists. */
  static final byte[] APPEND_SCRIPT = String.join("\n",
      "for i = 1, #ARGV, 2 do",
      "  if redis.call('HEXISTS', KEYS[1], ARGV[i]) == 1 then return 0 end",
      "end",
      "for i = 1, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end",
      "local hi = tonumber(redis.call('GET', KEYS[2]) or '0')",
      "local last = tonumber(ARGV[#ARGV - 1])",
      "if last > hi then redis.call('SET', KEYS[2], tostring(last)) end",
      "return 1").getBytes(StandardCharsets.UTF_8);

  private final String pluginId;
  private final RedisSettings settings;
  private final JedisPool pool;
  private final Serialization serialization;
  private final ExecutionContext ec;

  public RedisJournal(Config config, String configPath) {
    this.pluginId = configPath;
    this.settings = RedisSettings.from(config);
    this.pool = settings.newPool();
    this.serialization = SerializationExtension.get(context().system());
    this.ec = context().dispatcher();
    verify(pool, settings, pluginId);
  }

  /**
   * Connects to the configured Redis and verifies it persists (or logs that verification was
   * explicitly disabled). Called by the plugin actors when they start, and synchronously by the
   * REDIS profile before the actor system boots so a misconfigured server fails the process with
   * the actionable message instead of an actor-initialization log line and a recovery timeout.
   *
   * @throws IllegalStateException when {@code url} is missing, the server is unreachable, or
   *     {@code appendonly} is not {@code yes}
   */
  public static RedisSettings preflight(Config pluginConfig, String pluginId) {
    RedisSettings settings = RedisSettings.from(pluginConfig);
    try (JedisPool pool = settings.newPool()) {
      verify(pool, settings, pluginId);
    }
    return settings;
  }

  private static void verify(JedisPool pool, RedisSettings settings, String pluginId) {
    try (Jedis j = pool.getResource()) {
      if (settings.durabilityCheck()) {
        RedisDurabilityCheck.verify(j, pluginId);
      } else {
        LOG.warn("{}: durability-check = off — trusting that {} persists (AOF / Enterprise / Valkey"
            + " persistence) without verifying it; a cache-only server here loses every event on restart",
            pluginId, settings.url());
      }
    } catch (JedisConnectionException e) {
      throw new IllegalStateException(pluginId + ": cannot reach Redis at " + settings.url()
          + " (" + e.getMessage() + "); start it or fix AGENTIC_REDIS_URL", e);
    }
  }

  @Override
  public void postStop() {
    pool.close();
  }

  @Override
  public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
    List<AtomicWrite> batch = new ArrayList<>();
    messages.forEach(batch::add);
    return async(() -> {
      List<Optional<Exception>> results = new ArrayList<>(batch.size());
      try (Jedis j = pool.getResource()) {
        for (AtomicWrite aw : batch) {
          results.add(writeAtomically(j, aw));
        }
      }
      return results;
    });
  }

  private Optional<Exception> writeAtomically(Jedis j, AtomicWrite aw) {
    List<PersistentRepr> reprs = CollectionConverters.asJava(aw.payload());
    List<byte[]> args = new ArrayList<>(reprs.size() * 2);
    for (PersistentRepr r : reprs) {
      byte[] bytes;
      try {
        bytes = serialization.serialize(r).get();
      } catch (Exception e) {
        return Optional.of(e);
      }
      args.add(Long.toString(r.sequenceNr()).getBytes(StandardCharsets.UTF_8));
      args.add(bytes);
    }
    List<byte[]> keys = List.of(settings.journalKey(aw.persistenceId()), settings.highestKey(aw.persistenceId()));
    Object result = j.eval(APPEND_SCRIPT, keys, args);
    if (!(result instanceof Long ok) || ok != 1L) {
      throw new IllegalStateException("journal " + aw.persistenceId() + " already holds one of sequence numbers "
          + aw.lowestSequenceNr() + ".." + aw.highestSequenceNr() + ": refusing to overwrite the event log");
    }
    return Optional.empty();
  }

  @Override
  public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
    return async(() -> {
      try (Jedis j = pool.getResource()) {
        byte[] key = settings.journalKey(persistenceId);
        long to = Math.min(toSequenceNr, highest(j, persistenceId));
        List<byte[]> doomed = new ArrayList<>();
        for (byte[] field : j.hkeys(key)) {
          if (Long.parseLong(new String(field, StandardCharsets.UTF_8)) <= to) {
            doomed.add(field);
          }
        }
        for (int i = 0; i < doomed.size(); i += settings.replayBatch()) {
          List<byte[]> chunk = doomed.subList(i, Math.min(doomed.size(), i + settings.replayBatch()));
          j.hdel(key, chunk.toArray(new byte[0][]));
        }
      }
      return null;
    });
  }

  @Override
  public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr,
                                            long max, Consumer<PersistentRepr> replayCallback) {
    return async(() -> {
      if (max <= 0) {
        return null;
      }
      try (Jedis j = pool.getResource()) {
        byte[] key = settings.journalKey(persistenceId);
        long to = Math.min(toSequenceNr, highest(j, persistenceId));
        long remaining = max;
        for (long start = Math.max(1, fromSequenceNr); start <= to && remaining > 0; start += settings.replayBatch()) {
          long end = Math.min(to, start + settings.replayBatch() - 1);
          byte[][] fields = new byte[(int) (end - start + 1)][];
          for (int i = 0; i < fields.length; i++) {
            fields[i] = Long.toString(start + i).getBytes(StandardCharsets.UTF_8);
          }
          List<byte[]> values = j.hmget(key, fields);
          for (byte[] v : values) {
            if (v == null) {
              continue;
            }
            replayCallback.accept(serialization.deserialize(v, PersistentRepr.class).get());
            if (--remaining == 0) {
              break;
            }
          }
        }
      }
      return null;
    });
  }

  @Override
  public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
    return async(() -> {
      try (Jedis j = pool.getResource()) {
        return highest(j, persistenceId);
      }
    });
  }

  private long highest(Jedis j, String persistenceId) {
    byte[] v = j.get(settings.highestKey(persistenceId));
    return v == null ? 0L : Long.parseLong(new String(v, StandardCharsets.UTF_8));
  }

  private <T> Future<T> async(Callable<T> body) {
    return Futures.future(body, ec);
  }
}
