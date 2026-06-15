package org.jagentic.tools.app.pubsub;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.ToolRegistry;

/**
 * Shared request/result mapping for the async pub-sub bridges (Kafka + Redis). A request is a
 * JSON object {@code {id, tool, args}}; the result is {@code {id, ok, result|error}}, correlated
 * by {@code id}. The same {@link ToolRegistry} that backs REST/MCP/gRPC runs the tool, so all
 * transports stay behaviourally identical.
 */
public final class ToolPubSub {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final ToolRegistry registry;
  private final ObjectMapper mapper;

  public ToolPubSub(ToolRegistry registry, ObjectMapper mapper) {
    this.registry = registry;
    this.mapper = mapper;
  }

  /** Handle one request payload, returning the result payload as a JSON string. Never throws —
   * tool failures and malformed requests are reported in the result envelope. */
  public String handle(String requestJson) {
    Object id = null;
    try {
      Map<String, Object> req = mapper.readValue(requestJson, MAP_TYPE);
      id = req.get("id");
      String tool = req.get("tool") == null ? null : String.valueOf(req.get("tool"));
      @SuppressWarnings("unchecked")
      Map<String, Object> args = req.get("args") instanceof Map
          ? (Map<String, Object>) req.get("args")
          : Map.of();
      if (tool == null || registry.get(tool) == null) {
        return error(id, "no such tool: " + tool);
      }
      Object result = registry.execute(tool, args);
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("id", id);
      out.put("ok", true);
      out.put("result", result);
      return mapper.writeValueAsString(out);
    } catch (Exception e) {
      return error(id, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }
  }

  private String error(Object id, String message) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("id", id);
    out.put("ok", false);
    out.put("error", message);
    try {
      return mapper.writeValueAsString(out);
    } catch (Exception fatal) {
      // Last-ditch hand-built JSON; id/message are simple scalars here.
      return "{\"id\":" + jsonScalar(id) + ",\"ok\":false,\"error\":" + jsonScalar(message) + "}";
    }
  }

  private static String jsonScalar(Object v) {
    if (v == null) {
      return "null";
    }
    if (v instanceof Number || v instanceof Boolean) {
      return v.toString();
    }
    return "\"" + v.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
