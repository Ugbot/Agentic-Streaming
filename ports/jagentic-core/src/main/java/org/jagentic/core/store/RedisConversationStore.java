package org.jagentic.core.store;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;

import redis.clients.jedis.JedisPooled;

/** Real {@link ConversationStore} backed by Redis/Valkey (Jedis), using the same key
 * scheme as the Python/Go impls (transcript list + attrs hash + per-user set) so they
 * interoperate. */
public final class RedisConversationStore implements ConversationStore {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final JedisPooled jedis;
  private final int max;
  private final String prefix;

  public RedisConversationStore(String url, int maxMessages) {
    this.jedis = new JedisPooled(url);
    this.max = maxMessages <= 0 ? 200 : maxMessages;
    this.prefix = "agentic";
  }

  private String msgs(String cid) { return prefix + ":conv:" + cid + ":msgs"; }
  private String attrs(String cid) { return prefix + ":conv:" + cid + ":attrs"; }
  private String user(String uid) { return prefix + ":user:" + uid + ":convs"; }

  @Override
  public void append(String cid, ChatMessage m) {
    try {
      Map<String, String> d = new LinkedHashMap<>();
      d.put("role", m.role());
      d.put("content", m.content() == null ? "" : m.content());
      d.put("tool_name", m.toolName());
      d.put("tool_call_id", m.toolCallId());
      jedis.rpush(msgs(cid), MAPPER.writeValueAsString(d));
      jedis.ltrim(msgs(cid), -max, -1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ChatMessage> history(String cid) {
    List<ChatMessage> out = new ArrayList<>();
    for (String v : jedis.lrange(msgs(cid), 0, -1)) {
      try {
        Map<String, String> d = MAPPER.readValue(v, Map.class);
        out.add(new ChatMessage(d.get("role"), d.get("content"), d.get("tool_name"), d.get("tool_call_id")));
      } catch (Exception ignore) {
        // skip malformed
      }
    }
    return out;
  }

  @Override
  public int messageCount(String cid) {
    return (int) jedis.llen(msgs(cid));
  }

  @Override
  public void putAttribute(String cid, String key, String value) {
    jedis.hset(attrs(cid), key, value);
  }

  @Override
  public Optional<String> getAttribute(String cid, String key) {
    return Optional.ofNullable(jedis.hget(attrs(cid), key));
  }

  @Override
  public Map<String, String> attributes(String cid) {
    Map<String, String> out = new LinkedHashMap<>();
    jedis.hgetAll(attrs(cid)).forEach((k, v) -> {
      if (!k.startsWith("__")) out.put(k, v);
    });
    return out;
  }

  @Override
  public void associateUser(String cid, String userId) {
    String prior = jedis.hget(attrs(cid), "__owner__");
    if (prior != null && !prior.equals(userId)) {
      jedis.srem(user(prior), cid);
    }
    jedis.hset(attrs(cid), "__owner__", userId);
    jedis.sadd(user(userId), cid);
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    return new ArrayList<>(new TreeSet<>(jedis.smembers(user(userId))));
  }

  @Override
  public void clear(String cid) {
    String owner = jedis.hget(attrs(cid), "__owner__");
    if (owner != null) {
      jedis.srem(user(owner), cid);
    }
    jedis.del(msgs(cid), attrs(cid));
  }
}
