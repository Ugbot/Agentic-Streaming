package org.jagentic.ports.kafkastreams;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;

/**
 * A {@link ConversationStore} whose transcript, attributes and user index live in a Kafka
 * Streams {@link KeyValueStore} (persistent and changelog-backed when registered through
 * {@code Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(...))}, which is how
 * {@link BankingTopology} registers it). Nothing is cached on the heap: every read goes to the
 * store, so the transcript survives a processor restart and is restored from the changelog on
 * a new instance.
 *
 * <p>Layout, one logical record per key so no value has to be rewritten on append. Segments are
 * separated by {@code NUL}, which cannot occur in a conversation id, user id or attribute name
 * without breaking the layout (every write path rejects it):
 * <pre>
 *   c NUL cid                 first and next message sequence numbers, "first next"
 *   m NUL cid NUL seq         one message; seq is zero padded so byte order is append order
 *   a NUL cid NUL name        one scalar attribute
 *   o NUL cid                 owning userId
 *   u NUL userId NUL cid      user index entry (value unused)
 * </pre>
 * A message is encoded as four length-prefixed fields ({@code len:value}, {@code -:} for null)
 * so any content, including separators and newlines, round-trips.
 */
final class KeyValueConversationStore implements ConversationStore {

  static final int DEFAULT_MAX_MESSAGES = 200;

  private static final char SEP = '\u0000';
  private static final StringSerializer KEYS = new StringSerializer();

  private final KeyValueStore<String, String> kv;
  private final int maxMessages;

  KeyValueConversationStore(KeyValueStore<String, String> kv) {
    this(kv, DEFAULT_MAX_MESSAGES);
  }

  KeyValueConversationStore(KeyValueStore<String, String> kv, int maxMessages) {
    this.kv = kv;
    this.maxMessages = Math.max(1, maxMessages);
  }

  private static String segment(String value, String what) {
    if (value == null) {
      throw new IllegalArgumentException(what + " must not be null");
    }
    if (value.indexOf(SEP) >= 0) {
      throw new IllegalArgumentException(what + " must not contain NUL");
    }
    return value;
  }

  private static String cursorKey(String cid) {
    return "c" + SEP + cid;
  }

  private static String messagePrefix(String cid) {
    return "m" + SEP + cid + SEP;
  }

  private static String messageKey(String cid, long seq) {
    return messagePrefix(cid) + String.format("%019d", seq);
  }

  private static String attributePrefix(String cid) {
    return "a" + SEP + cid + SEP;
  }

  private static String ownerKey(String cid) {
    return "o" + SEP + cid;
  }

  private static String userPrefix(String userId) {
    return "u" + SEP + userId + SEP;
  }

  /** {first, next}: messages first..next-1 are live. */
  private long[] cursor(String cid) {
    String raw = kv.get(cursorKey(cid));
    if (raw == null) {
      return new long[] {0L, 0L};
    }
    int space = raw.indexOf(' ');
    return new long[] {Long.parseLong(raw.substring(0, space)), Long.parseLong(raw.substring(space + 1))};
  }

  @Override
  public void append(String conversationId, ChatMessage message) {
    String cid = segment(conversationId, "conversationId");
    long[] c = cursor(cid);
    kv.put(messageKey(cid, c[1]), encode(message));
    c[1]++;
    while (c[1] - c[0] > maxMessages) {
      kv.delete(messageKey(cid, c[0]));
      c[0]++;
    }
    kv.put(cursorKey(cid), c[0] + " " + c[1]);
  }

  @Override
  public List<ChatMessage> history(String conversationId) {
    String cid = segment(conversationId, "conversationId");
    List<ChatMessage> out = new ArrayList<>();
    try (KeyValueIterator<String, String> it = kv.prefixScan(messagePrefix(cid), KEYS)) {
      while (it.hasNext()) {
        out.add(decode(it.next().value));
      }
    }
    return out;
  }

  @Override
  public int messageCount(String conversationId) {
    long[] c = cursor(segment(conversationId, "conversationId"));
    return (int) (c[1] - c[0]);
  }

  @Override
  public void putAttribute(String conversationId, String key, String value) {
    kv.put(attributePrefix(segment(conversationId, "conversationId")) + segment(key, "attribute name"), value);
  }

  @Override
  public Optional<String> getAttribute(String conversationId, String key) {
    return Optional.ofNullable(
        kv.get(attributePrefix(segment(conversationId, "conversationId")) + segment(key, "attribute name")));
  }

  @Override
  public Map<String, String> attributes(String conversationId) {
    String prefix = attributePrefix(segment(conversationId, "conversationId"));
    Map<String, String> out = new LinkedHashMap<>();
    try (KeyValueIterator<String, String> it = kv.prefixScan(prefix, KEYS)) {
      while (it.hasNext()) {
        KeyValue<String, String> e = it.next();
        out.put(e.key.substring(prefix.length()), e.value);
      }
    }
    return out;
  }

  @Override
  public void associateUser(String conversationId, String userId) {
    String cid = segment(conversationId, "conversationId");
    String uid = segment(userId, "userId");
    String previous = kv.get(ownerKey(cid));
    if (previous != null && !previous.equals(uid)) {
      kv.delete(userPrefix(previous) + cid);
    }
    kv.put(ownerKey(cid), uid);
    kv.put(userPrefix(uid) + cid, "");
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    String prefix = userPrefix(segment(userId, "userId"));
    List<String> out = new ArrayList<>();
    try (KeyValueIterator<String, String> it = kv.prefixScan(prefix, KEYS)) {
      while (it.hasNext()) {
        out.add(it.next().key.substring(prefix.length()));
      }
    }
    return out;
  }

  @Override
  public void clear(String conversationId) {
    String cid = segment(conversationId, "conversationId");
    deletePrefix(messagePrefix(cid));
    deletePrefix(attributePrefix(cid));
    kv.delete(cursorKey(cid));
    String owner = kv.get(ownerKey(cid));
    if (owner != null) {
      kv.delete(userPrefix(owner) + cid);
      kv.delete(ownerKey(cid));
    }
  }

  private void deletePrefix(String prefix) {
    List<String> keys = new ArrayList<>();
    try (KeyValueIterator<String, String> it = kv.prefixScan(prefix, KEYS)) {
      while (it.hasNext()) {
        keys.add(it.next().key);
      }
    }
    for (String k : keys) {
      kv.delete(k);
    }
  }

  static String encode(ChatMessage m) {
    StringBuilder sb = new StringBuilder();
    for (String field : new String[] {m.role(), m.content(), m.toolName(), m.toolCallId()}) {
      if (field == null) {
        sb.append("-:");
      } else {
        sb.append(field.length()).append(':').append(field);
      }
    }
    return sb.toString();
  }

  static ChatMessage decode(String raw) {
    String[] fields = new String[4];
    int pos = 0;
    for (int i = 0; i < 4; i++) {
      int colon = raw.indexOf(':', pos);
      if (colon < 0) {
        throw new IllegalArgumentException("malformed message record: " + raw);
      }
      String len = raw.substring(pos, colon);
      pos = colon + 1;
      if (len.equals("-")) {
        fields[i] = null;
      } else {
        int n = Integer.parseInt(len);
        fields[i] = raw.substring(pos, pos + n);
        pos += n;
      }
    }
    if (pos != raw.length()) {
      throw new IllegalArgumentException("trailing bytes in message record: " + raw);
    }
    return new ChatMessage(fields[0], fields[1], fields[2], fields[3]);
  }
}
