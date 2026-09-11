package org.jagentic.pekko.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity;

/**
 * The JSON wire format shared by the HTTP and Kafka front doors. A request is one turn:
 * {@code conversation_id}, {@code turn_id} (the idempotency key; generated when the caller omits it
 * and always echoed back in the result), {@code user_id}, {@code text}, optional string
 * {@code metadata}, and an optional structured {@code signal} that resumes a suspended turn. A
 * response is the normalized result document of {@code spec/v1/result.schema.json}.
 */
public final class TurnWire {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
  private static final TypeReference<Map<String, String>> STRINGS = new TypeReference<>() {};

  private TurnWire() {}

  /** Thrown for a request that is not a JSON object with the fields above. */
  public static final class MalformedTurn extends RuntimeException {
    public MalformedTurn(String message, Throwable cause) {
      super(message, cause);
    }

    public MalformedTurn(String message) {
      super(message);
    }
  }

  public static Event parse(String json) {
    JsonNode n;
    try {
      n = JSON.readTree(json);
    } catch (IOException e) {
      throw new MalformedTurn("invalid json: " + e.getMessage(), e);
    }
    if (n == null || !n.isObject()) {
      throw new MalformedTurn("turn must be a json object");
    }
    String cid = text(n, "conversation_id", "conversationId");
    if (cid == null) {
      throw new MalformedTurn("conversation_id is required");
    }
    String turnId = text(n, "turn_id", "turnId");
    if (turnId == null) {
      turnId = UUID.randomUUID().toString();
    }
    String uid = text(n, "user_id", "userId");
    Map<String, Object> signal = n.hasNonNull("signal") ? convert(n.get("signal"), OBJECT, "signal") : null;
    if (signal != null) {
      return Event.resume(cid, turnId, signal);
    }
    String txt = text(n, "text");
    Map<String, String> metadata = n.hasNonNull("metadata")
        ? convert(n.get("metadata"), STRINGS, "metadata") : Map.of();
    return new Event(cid, turnId, uid == null ? "anonymous" : uid, txt == null ? "" : txt, metadata, null);
  }

  public static String write(ConversationEntity.TurnReply reply) {
    return write(reply.toMap());
  }

  public static String write(Map<String, Object> document) {
    try {
      return JSON.writeValueAsString(document);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Map<String, Object> readObject(String json) {
    try {
      return JSON.readValue(json, OBJECT);
    } catch (IOException e) {
      throw new MalformedTurn("invalid json: " + e.getMessage(), e);
    }
  }

  private static <T> T convert(JsonNode node, TypeReference<T> type, String field) {
    if (!node.isObject()) {
      throw new MalformedTurn(field + " must be a json object");
    }
    try {
      return JSON.convertValue(node, type);
    } catch (IllegalArgumentException e) {
      throw new MalformedTurn(field + " has the wrong shape: " + e.getMessage(), e);
    }
  }

  private static String text(JsonNode node, String... keys) {
    for (String k : keys) {
      JsonNode v = node.get(k);
      if (v != null && !v.isNull()) {
        return v.asText();
      }
    }
    return null;
  }
}
