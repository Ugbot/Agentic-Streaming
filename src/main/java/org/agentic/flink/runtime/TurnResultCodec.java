package org.agentic.flink.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.agentic.flink.typeinfo.FlinkJson;
import org.jagentic.core.LogEvent;
import org.jagentic.core.ToolCall;
import org.jagentic.core.TurnError;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;

/**
 * Encodes a {@link TurnResult} as its normalized {@code spec/v1/result.schema.json} document and
 * decodes it back. The wire form <em>is</em> the spec shape, so a Flink sink that writes these
 * bytes emits conformance-comparable results without a second mapping step.
 */
public final class TurnResultCodec {
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

  private TurnResultCodec() {}

  private static ObjectMapper json() {
    return FlinkJson.mapper();
  }

  public static byte[] encode(TurnResult r) throws IOException {
    return json().writeValueAsBytes(r.toMap());
  }

  public static TurnResult decode(byte[] bytes) throws IOException {
    return fromMap(json().readValue(bytes, MAP));
  }

  /** Inverse of {@link TurnResult#toMap()}. */
  @SuppressWarnings("unchecked")
  public static TurnResult fromMap(Map<String, Object> m) {
    String cid = (String) m.get("conversation_id");
    String turnId = (String) m.get("turn_id");
    TurnStatus status = TurnStatus.parse((String) m.get("status"));
    String path = (String) m.get("path");
    String reply = (String) m.get("reply");
    Map<String, Object> state = m.get("state") instanceof Map<?, ?> s
        ? new LinkedHashMap<>((Map<String, Object>) s) : Map.of();
    List<ToolCall> calls = new ArrayList<>();
    if (m.get("tool_calls") instanceof List<?> tc) {
      for (Object o : tc) {
        calls.add(toolCall((Map<String, Object>) o));
      }
    }
    List<LogEvent> events = new ArrayList<>();
    if (m.get("events") instanceof List<?> ev) {
      for (Object o : ev) {
        Map<String, Object> e = (Map<String, Object>) o;
        Map<String, Object> payload = e.get("payload") instanceof Map<?, ?> p
            ? (Map<String, Object>) p : Map.of();
        events.add(new LogEvent(cid, ((Number) e.get("sequence")).longValue(), turnId,
            (String) e.get("type"), payload));
      }
    }
    TurnError error = null;
    if (m.get("error") instanceof Map<?, ?> em) {
      Map<String, Object> e = (Map<String, Object>) em;
      error = new TurnError(TurnError.ErrorClass.parse((String) e.get("class")), (String) e.get("message"));
    }
    return new TurnResult(cid, turnId, status, path, reply, error, calls, events, state);
  }

  @SuppressWarnings("unchecked")
  private static ToolCall toolCall(Map<String, Object> c) {
    Map<String, Object> args = c.get("args") instanceof Map<?, ?> a ? (Map<String, Object>) a : Map.of();
    int index = ((Number) c.get("index")).intValue();
    int attempt = ((Number) c.get("attempt")).intValue();
    String tool = (String) c.get("tool");
    if (c.get("error") != null) {
      return ToolCall.failed(tool, index, args, String.valueOf(c.get("error")), attempt);
    }
    return ToolCall.succeeded(tool, index, args, c.get("result"), attempt);
  }
}
