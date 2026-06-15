package org.jagentic.core.mcp.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jagentic.core.ToolRegistry;

/** Transport-agnostic MCP <b>server</b> engine — the mirror image of
 * {@link org.jagentic.core.store.McpStdioClient}: same JSON-RPC 2.0, same protocol version
 * ({@code 2024-11-05}), same response shapes, so our own MCP client interoperates with it
 * byte-for-byte. It exposes any {@link ToolRegistry} as MCP tools.
 *
 * <p>This class is pure: {@link #handle(JsonNode)} maps one JSON-RPC request to one response
 * node (or {@code null} for a notification, which must not be answered). It owns no I/O — a
 * transport (stdio loop, HTTP handler, gRPC service, pub-sub bridge) drives it.</p>
 */
public final class ToolServer {

  public static final String PROTOCOL_VERSION = "2024-11-05";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ToolRegistry registry;
  private final String serverName;
  private final String serverVersion;

  public ToolServer(ToolRegistry registry) {
    this(registry, "jagentic-tool-server", "0.1.0");
  }

  public ToolServer(ToolRegistry registry, String serverName, String serverVersion) {
    this.registry = registry;
    this.serverName = serverName;
    this.serverVersion = serverVersion;
  }

  public ToolRegistry registry() {
    return registry;
  }

  /** Handle one JSON-RPC request. Returns the response node, or {@code null} when the input
   * is a notification (no {@code id}) that must produce no reply. Never throws — protocol and
   * tool errors are mapped to JSON-RPC error / {@code isError} envelopes. */
  public JsonNode handle(JsonNode request) {
    if (request == null || !request.isObject()) {
      return error(null, -32600, "invalid request");
    }
    JsonNode idNode = request.get("id");
    boolean isNotification = idNode == null || idNode.isNull();
    String method = request.path("method").asText("");
    JsonNode params = request.path("params");

    try {
      switch (method) {
        case "initialize":
          return result(idNode, initializeResult());
        case "notifications/initialized":
        case "initialized":
          return null; // notification — no response
        case "ping":
          return result(idNode, MAPPER.createObjectNode());
        case "tools/list":
          return result(idNode, toolsListResult());
        case "tools/call":
          return result(idNode, toolsCallResult(params));
        default:
          if (isNotification) {
            return null; // ignore unknown notifications
          }
          return error(idNode, -32601, "method not found: " + method);
      }
    } catch (IllegalArgumentException e) {
      // unknown tool / bad params
      return error(idNode, -32602, e.getMessage());
    } catch (Exception e) {
      return error(idNode, -32603, "internal error: " + e.getMessage());
    }
  }

  private ObjectNode initializeResult() {
    ObjectNode res = MAPPER.createObjectNode();
    res.put("protocolVersion", PROTOCOL_VERSION);
    ObjectNode caps = res.putObject("capabilities");
    caps.putObject("tools");
    ObjectNode info = res.putObject("serverInfo");
    info.put("name", serverName);
    info.put("version", serverVersion);
    return res;
  }

  private ObjectNode toolsListResult() {
    ObjectNode res = MAPPER.createObjectNode();
    res.set("tools", MAPPER.valueToTree(registry.toolDescriptors()));
    return res;
  }

  @SuppressWarnings("unchecked")
  private ObjectNode toolsCallResult(JsonNode params) {
    String name = params.path("name").asText("");
    if (name.isEmpty()) {
      throw new IllegalArgumentException("tools/call requires a 'name'");
    }
    Map<String, Object> args = Map.of();
    JsonNode argsNode = params.get("arguments");
    if (argsNode != null && argsNode.isObject()) {
      args = MAPPER.convertValue(argsNode, Map.class);
    }
    ObjectNode res = MAPPER.createObjectNode();
    try {
      Object out = registry.execute(name, args);
      res.set("content", textContent(stringify(out)));
      res.put("isError", false);
    } catch (IllegalArgumentException e) {
      throw e; // unknown tool -> -32602 above
    } catch (Exception e) {
      // a tool that threw is reported as an MCP tool error (isError), not a protocol error
      res.set("content", textContent("tool error: " + e.getMessage()));
      res.put("isError", true);
    }
    return res;
  }

  private com.fasterxml.jackson.databind.node.ArrayNode textContent(String text) {
    com.fasterxml.jackson.databind.node.ArrayNode arr = MAPPER.createArrayNode();
    ObjectNode part = arr.addObject();
    part.put("type", "text");
    part.put("text", text);
    return arr;
  }

  private String stringify(Object out) {
    if (out == null) {
      return "";
    }
    if (out instanceof String s) {
      return s;
    }
    try {
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      return String.valueOf(out);
    }
  }

  private ObjectNode result(JsonNode id, JsonNode result) {
    ObjectNode resp = MAPPER.createObjectNode();
    resp.put("jsonrpc", "2.0");
    setId(resp, id);
    resp.set("result", result);
    return resp;
  }

  private ObjectNode error(JsonNode id, int code, String message) {
    ObjectNode resp = MAPPER.createObjectNode();
    resp.put("jsonrpc", "2.0");
    setId(resp, id);
    ObjectNode err = resp.putObject("error");
    err.put("code", code);
    err.put("message", message == null ? "error" : message);
    return resp;
  }

  private void setId(ObjectNode resp, JsonNode id) {
    if (id == null || id.isNull()) {
      resp.putNull("id");
    } else {
      resp.set("id", id);
    }
  }

  /** Convenience: list registered tool names (handy for diagnostics). */
  public List<String> toolNames() {
    List<String> names = new ArrayList<>();
    for (Map<String, Object> d : registry.toolDescriptors()) {
      names.add(String.valueOf(d.get("name")));
    }
    return names;
  }

  // exposed for transports that prefer to (de)serialize themselves
  public static ObjectMapper mapper() {
    return MAPPER;
  }
}
