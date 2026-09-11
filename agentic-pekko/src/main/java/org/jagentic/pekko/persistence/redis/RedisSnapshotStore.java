package org.jagentic.pekko.persistence.redis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.typesafe.config.Config;

import org.apache.pekko.dispatch.Futures;
import org.apache.pekko.persistence.SelectedSnapshot;
import org.apache.pekko.persistence.SnapshotMetadata;
import org.apache.pekko.persistence.SnapshotSelectionCriteria;
import org.apache.pekko.persistence.serialization.Snapshot;
import org.apache.pekko.persistence.snapshot.japi.SnapshotStore;
import org.apache.pekko.serialization.Serialization;
import org.apache.pekko.serialization.SerializationExtension;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

/**
 * Snapshot store companion to {@link RedisJournal}: {@code {prefix}:s:{persistenceId}} is a hash
 * whose field is the snapshot's sequenceNr and whose value is {@code [8-byte timestamp][serialized
 * Snapshot]}. Load picks the highest sequenceNr matching the criteria. Snapshots are an
 * optimisation only — the journal remains the source of truth — but they share the Redis and so
 * the same durability check.
 */
public final class RedisSnapshotStore extends SnapshotStore {
  private final RedisSettings settings;
  private final JedisPool pool;
  private final Serialization serialization;
  private final ExecutionContext ec;

  public RedisSnapshotStore(Config config, String configPath) {
    this.settings = RedisSettings.from(config);
    this.pool = settings.newPool();
    this.serialization = SerializationExtension.get(context().system());
    this.ec = context().dispatcher();
    if (settings.durabilityCheck()) {
      try (Jedis j = pool.getResource()) {
        RedisDurabilityCheck.verify(j, configPath);
      }
    }
  }

  @Override
  public void postStop() {
    pool.close();
  }

  private record Stored(long sequenceNr, long timestamp, byte[] snapshot) {
    boolean matches(SnapshotSelectionCriteria c) {
      return sequenceNr <= c.maxSequenceNr() && sequenceNr >= c.minSequenceNr()
          && timestamp <= c.maxTimestamp() && timestamp >= c.minTimestamp();
    }
  }

  @Override
  public Future<Optional<SelectedSnapshot>> doLoadAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
    return async(() -> {
      Stored best = null;
      for (Stored s : all(persistenceId)) {
        if (s.matches(criteria) && (best == null || s.sequenceNr() > best.sequenceNr())) {
          best = s;
        }
      }
      if (best == null) {
        return Optional.empty();
      }
      Snapshot snap = serialization.deserialize(best.snapshot(), Snapshot.class).get();
      return Optional.of(SelectedSnapshot.create(
          SnapshotMetadata.apply(persistenceId, best.sequenceNr(), best.timestamp()), snap.data()));
    });
  }

  @Override
  public Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot) {
    return async(() -> {
      byte[] body = serialization.serialize(new Snapshot(snapshot)).get();
      byte[] value = ByteBuffer.allocate(8 + body.length).putLong(metadata.timestamp()).put(body).array();
      try (Jedis j = pool.getResource()) {
        j.hset(settings.snapshotKey(metadata.persistenceId()), field(metadata.sequenceNr()), value);
      }
      return null;
    });
  }

  @Override
  public Future<Void> doDeleteAsync(SnapshotMetadata metadata) {
    return async(() -> {
      try (Jedis j = pool.getResource()) {
        j.hdel(settings.snapshotKey(metadata.persistenceId()), field(metadata.sequenceNr()));
      }
      return null;
    });
  }

  @Override
  public Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
    return async(() -> {
      List<byte[]> doomed = new ArrayList<>();
      for (Stored s : all(persistenceId)) {
        if (s.matches(criteria)) {
          doomed.add(field(s.sequenceNr()));
        }
      }
      if (!doomed.isEmpty()) {
        try (Jedis j = pool.getResource()) {
          j.hdel(settings.snapshotKey(persistenceId), doomed.toArray(new byte[0][]));
        }
      }
      return null;
    });
  }

  private List<Stored> all(String persistenceId) {
    Map<byte[], byte[]> raw;
    try (Jedis j = pool.getResource()) {
      raw = j.hgetAll(settings.snapshotKey(persistenceId));
    }
    List<Stored> out = new ArrayList<>(raw.size());
    for (Map.Entry<byte[], byte[]> e : raw.entrySet()) {
      ByteBuffer buf = ByteBuffer.wrap(e.getValue());
      long ts = buf.getLong();
      byte[] body = new byte[buf.remaining()];
      buf.get(body);
      out.add(new Stored(Long.parseLong(new String(e.getKey(), StandardCharsets.UTF_8)), ts, body));
    }
    return out;
  }

  private static byte[] field(long sequenceNr) {
    return Long.toString(sequenceNr).getBytes(StandardCharsets.UTF_8);
  }

  private <T> Future<T> async(Callable<T> body) {
    return Futures.future(body, ec);
  }
}
