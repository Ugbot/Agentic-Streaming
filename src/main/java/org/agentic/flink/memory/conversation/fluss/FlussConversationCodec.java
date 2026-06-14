package org.agentic.flink.memory.conversation.fluss;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;

/**
 * Pure (cluster-free) JSON codec for {@link FlussConversationStore}'s row payloads. A Fluss PK table
 * gives single-key upsert + lookup; this models a whole conversation as one {@code (key, payload)}
 * row whose payload is a JSON <em>envelope</em> ({@code msgs}/{@code attrs}/{@code owner}), plus
 * separate JSON-array <em>index</em> rows (all-conversations, per-user) so listing operations work
 * without a secondary-index scan.
 *
 * <p>Every mutation is a read-modify-write on the JSON string, so this logic is the correctness core
 * of the store and is unit-tested directly without a running Fluss cluster.
 */
final class FlussConversationCodec {

  private FlussConversationCodec() {}

  private static final String MSGS = "msgs";
  private static final String ATTRS = "attrs";
  private static final String OWNER = "owner";

  static final String EMPTY_ENVELOPE = "{}";
  static final String EMPTY_LIST = "[]";

  // ==================== envelope ====================

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parseEnvelope(ObjectMapper mapper, String json) {
    if (json == null || json.isEmpty()) {
      return new LinkedHashMap<>();
    }
    try {
      Map<String, Object> m = mapper.readValue(json, Map.class);
      return m == null ? new LinkedHashMap<>() : m;
    } catch (Exception e) {
      return new LinkedHashMap<>();
    }
  }

  private static String writeJson(ObjectMapper mapper, Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalStateException("fluss conversation codec: JSON encode failed", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, String>> msgList(Map<String, Object> env) {
    Object raw = env.get(MSGS);
    if (raw instanceof List) {
      return (List<Map<String, String>>) raw;
    }
    return new ArrayList<>();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> attrMap(Map<String, Object> env) {
    Object raw = env.get(ATTRS);
    if (raw instanceof Map) {
      return (Map<String, String>) raw;
    }
    return new LinkedHashMap<>();
  }

  /** Append a message to the envelope, keeping at most {@code maxMessages} (0 = unbounded). */
  static String appendMessage(ObjectMapper mapper, String envelopeJson, ChatMessage m, int maxMessages) {
    Map<String, Object> env = parseEnvelope(mapper, envelopeJson);
    List<Map<String, String>> msgs = new ArrayList<>(msgList(env));
    Map<String, String> encoded = new LinkedHashMap<>();
    encoded.put("role", m.getRole().name());
    encoded.put("content", m.getContent() == null ? "" : m.getContent());
    if (m.getToolCallId() != null) {
      encoded.put("toolCallId", m.getToolCallId());
    }
    if (m.getToolName() != null) {
      encoded.put("toolName", m.getToolName());
    }
    msgs.add(encoded);
    if (maxMessages > 0 && msgs.size() > maxMessages) {
      msgs = new ArrayList<>(msgs.subList(msgs.size() - maxMessages, msgs.size()));
    }
    env.put(MSGS, msgs);
    return writeJson(mapper, env);
  }

  static List<ChatMessage> messages(ObjectMapper mapper, String envelopeJson) {
    Map<String, Object> env = parseEnvelope(mapper, envelopeJson);
    List<ChatMessage> out = new ArrayList<>();
    for (Map<String, String> e : msgList(env)) {
      ChatMessage m = decode(e);
      if (m != null) {
        out.add(m);
      }
    }
    return out;
  }

  static int messageCount(ObjectMapper mapper, String envelopeJson) {
    return msgList(parseEnvelope(mapper, envelopeJson)).size();
  }

  static String putAttribute(ObjectMapper mapper, String envelopeJson, String key, String value) {
    Map<String, Object> env = parseEnvelope(mapper, envelopeJson);
    Map<String, String> attrs = new LinkedHashMap<>(attrMap(env));
    attrs.put(key, value == null ? "" : value);
    env.put(ATTRS, attrs);
    return writeJson(mapper, env);
  }

  static Optional<String> getAttribute(ObjectMapper mapper, String envelopeJson, String key) {
    return Optional.ofNullable(attrMap(parseEnvelope(mapper, envelopeJson)).get(key));
  }

  static Map<String, String> attributes(ObjectMapper mapper, String envelopeJson) {
    return new LinkedHashMap<>(attrMap(parseEnvelope(mapper, envelopeJson)));
  }

  static String setOwner(ObjectMapper mapper, String envelopeJson, String owner) {
    Map<String, Object> env = parseEnvelope(mapper, envelopeJson);
    env.put(OWNER, owner);
    return writeJson(mapper, env);
  }

  static Optional<String> owner(ObjectMapper mapper, String envelopeJson) {
    Object o = parseEnvelope(mapper, envelopeJson).get(OWNER);
    return o == null ? Optional.empty() : Optional.of(o.toString());
  }

  // ==================== index rows (JSON arrays, set semantics) ====================

  static List<String> decodeList(ObjectMapper mapper, String listJson) {
    if (listJson == null || listJson.isEmpty()) {
      return new ArrayList<>();
    }
    try {
      List<String> l = mapper.readValue(listJson, new TypeReference<List<String>>() {});
      return l == null ? new ArrayList<>() : l;
    } catch (Exception e) {
      return new ArrayList<>();
    }
  }

  static String addToList(ObjectMapper mapper, String listJson, String id) {
    LinkedHashSet<String> set = new LinkedHashSet<>(decodeList(mapper, listJson));
    set.add(id);
    return writeJson(mapper, new ArrayList<>(set));
  }

  static String removeFromList(ObjectMapper mapper, String listJson, String id) {
    LinkedHashSet<String> set = new LinkedHashSet<>(decodeList(mapper, listJson));
    set.remove(id);
    return writeJson(mapper, new ArrayList<>(set));
  }

  private static ChatMessage decode(Map<String, String> e) {
    try {
      String content = e.getOrDefault("content", "");
      switch (ChatRole.valueOf(e.get("role"))) {
        case SYSTEM:
          return ChatMessage.system(content);
        case USER:
          return ChatMessage.user(content);
        case ASSISTANT:
          return ChatMessage.assistant(content);
        case TOOL:
          return ChatMessage.tool(e.get("toolCallId"), e.get("toolName"), content);
        default:
          return ChatMessage.user(content);
      }
    } catch (Exception ex) {
      return null;
    }
  }
}
