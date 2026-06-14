package org.agentic.flink.retrieve;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.memory.vector.ScoredItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed {@link HotVectorIndex} — the preferred hot tier for a distributed live-RAG stack
 * (the embedded default is {@link InMemoryHotVectorIndex}). A capped, TTL'd recent window shared
 * across processes: the ingest job and the query job both point at the same Redis and immediately
 * see each other's writes.
 *
 * <p>Layout (per index {@code name}, all TTL'd so the window self-cleans):
 *
 * <pre>
 *   hot:{name}:vecs   -&gt; Hash  (id -&gt; JSON {e:[float…], t:text, m:{meta}})
 *   hot:{name}:order  -&gt; List  (ids in insertion order; FIFO-evicted past maxEntries)
 * </pre>
 *
 * Search reads the whole (bounded) window with one {@code HGETALL} and brute-forces cosine locally —
 * accurate and fast while the window is small, and a single round trip per query. Eviction is FIFO:
 * pushing past {@code maxEntries} pops the oldest id and drops its vector. {@link java.io.Serializable}
 * (host/port config); the {@link JedisPool} + mapper are transient and built lazily on the task side.
 */
public final class RedisHotVectorIndex implements HotVectorIndex {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RedisHotVectorIndex.class);

  private final String name;
  private final String host;
  private final int port;
  private final int maxEntries;
  private final long ttlSeconds;

  private transient volatile JedisPool pool;
  private transient volatile ObjectMapper mapper;

  public RedisHotVectorIndex(String name, String host, int port) {
    this(name, host, port, 2000, 86_400L);
  }

  public RedisHotVectorIndex(String name, String host, int port, int maxEntries, long ttlSeconds) {
    this.name = java.util.Objects.requireNonNull(name, "name");
    this.host = host;
    this.port = port;
    this.maxEntries = Math.max(1, maxEntries);
    this.ttlSeconds = ttlSeconds;
  }

  private String vecsKey() {
    return "hot:" + name + ":vecs";
  }

  private String orderKey() {
    return "hot:" + name + ":order";
  }

  @Override
  public void upsert(String id, float[] embedding, String text, Map<String, String> metadata) {
    if (id == null || embedding == null) {
      return;
    }
    try (Jedis jedis = pool().getResource()) {
      Map<String, Object> doc = new LinkedHashMap<>();
      doc.put("e", embedding);
      doc.put("t", text == null ? "" : text);
      doc.put("m", metadata == null ? Map.of() : metadata);
      jedis.hset(vecsKey(), id, mapper().writeValueAsString(doc));
      jedis.rpush(orderKey(), id);
      // FIFO-evict anything past the window cap.
      long len = jedis.llen(orderKey());
      while (len > maxEntries) {
        String evicted = jedis.lpop(orderKey());
        if (evicted == null) {
          break;
        }
        jedis.hdel(vecsKey(), evicted);
        len--;
      }
      jedis.expire(vecsKey(), ttlSeconds);
      jedis.expire(orderKey(), ttlSeconds);
    } catch (Exception e) {
      LOG.warn("hot upsert failed for {}: {}", id, e.toString());
    }
  }

  @Override
  public List<ScoredItem> search(float[] query, int k) {
    if (query == null) {
      return new ArrayList<>();
    }
    int topK = Math.max(1, k);
    try (Jedis jedis = pool().getResource()) {
      Map<String, String> all = jedis.hgetAll(vecsKey());
      if (all == null || all.isEmpty()) {
        return new ArrayList<>();
      }
      double qNorm = InMemoryHotVectorIndex.norm(query);
      PriorityQueue<ScoredItem> heap =
          new PriorityQueue<>(topK, (a, b) -> Double.compare(a.getScore(), b.getScore()));
      for (Map.Entry<String, String> e : all.entrySet()) {
        Doc doc = decode(e.getValue());
        if (doc == null || doc.e == null) {
          continue;
        }
        double score = InMemoryHotVectorIndex.cosine(query, qNorm, doc.e);
        ScoredItem si = toScored(e.getKey(), doc, score);
        if (heap.size() < topK) {
          heap.offer(si);
        } else if (heap.peek() != null && score > heap.peek().getScore()) {
          heap.poll();
          heap.offer(si);
        }
      }
      List<ScoredItem> out = new ArrayList<>(heap);
      out.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));
      return out;
    } catch (Exception e) {
      LOG.warn("hot search failed: {}", e.toString());
      return new ArrayList<>();
    }
  }

  @Override
  public int size() {
    try (Jedis jedis = pool().getResource()) {
      return (int) jedis.hlen(vecsKey());
    } catch (Exception e) {
      LOG.warn("hot size failed: {}", e.toString());
      return 0;
    }
  }

  @Override
  public void clear() {
    try (Jedis jedis = pool().getResource()) {
      jedis.del(vecsKey(), orderKey());
    } catch (Exception e) {
      LOG.warn("hot clear failed: {}", e.toString());
    }
  }

  // ==================== plumbing ====================

  /** Decoded hot-window document. */
  private static final class Doc {
    public float[] e;
    public String t;
    public Map<String, String> m;
  }

  private Doc decode(String json) {
    try {
      return mapper().readValue(json, Doc.class);
    } catch (Exception e) {
      return null;
    }
  }

  private static ScoredItem toScored(String id, Doc doc, double score) {
    ContextItem item =
        new ContextItem(doc.t == null ? "" : doc.t, ContextPriority.SHOULD, MemoryType.SHORT_TERM);
    if (doc.m != null) {
      for (Map.Entry<String, String> m : doc.m.entrySet()) {
        item.addMetadata(m.getKey(), m.getValue());
      }
    }
    return new ScoredItem(id, score, item);
  }

  private JedisPool pool() {
    JedisPool p = pool;
    if (p == null) {
      synchronized (this) {
        if (pool == null) {
          pool = new JedisPool(host, port);
        }
        p = pool;
      }
    }
    return p;
  }

  private ObjectMapper mapper() {
    ObjectMapper m = mapper;
    if (m == null) {
      synchronized (this) {
        if (mapper == null) {
          mapper = new ObjectMapper();
        }
        m = mapper;
      }
    }
    return m;
  }
}
