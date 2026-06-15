package org.jagentic.core.mcp.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import org.jagentic.core.ToolRegistry;
import org.jagentic.core.store.McpStdioClient;

/** The MCP server engine: protocol unit tests + the headline interop proof — our
 * {@link StdioToolServer}, launched as a subprocess, served to the existing
 * {@link McpStdioClient}. */
class ToolServerTest {

  private static final ObjectMapper M = new ObjectMapper();

  private static ToolRegistry calcRegistry() {
    ToolRegistry reg = new ToolRegistry();
    reg.register("add", "Add two numbers.",
        Map.of("type", "object", "properties",
            Map.of("a", Map.of("type", "number"), "b", Map.of("type", "number")),
            "required", List.of("a", "b")),
        p -> ((Number) p.get("a")).intValue() + ((Number) p.get("b")).intValue());
    // schema-less, back-compatible registration
    reg.register("ping", "Reply pong.", p -> "pong");
    return reg;
  }

  @Test
  void initializeAdvertisesProtocolAndTools() {
    ToolServer server = new ToolServer(calcRegistry());
    JsonNode init = server.handle(req(1, "initialize", M.createObjectNode()));
    assertEquals("2024-11-05", init.path("result").path("protocolVersion").asText());
    assertTrue(init.path("result").path("capabilities").has("tools"));
    assertTrue(init.path("result").path("serverInfo").path("name").asText().length() > 0);
  }

  @Test
  void toolsListIncludesSchemaAndBackCompatDefault() {
    ToolServer server = new ToolServer(calcRegistry());
    JsonNode list = server.handle(req(2, "tools/list", M.createObjectNode()));
    JsonNode tools = list.path("result").path("tools");
    Map<String, JsonNode> byName = new java.util.HashMap<>();
    tools.forEach(t -> byName.put(t.path("name").asText(), t));
    assertTrue(byName.containsKey("add") && byName.containsKey("ping"));
    // declared schema preserved
    assertEquals("number", byName.get("add").path("inputSchema").path("properties").path("a").path("type").asText());
    // schema-less tool gets the permissive MCP default
    assertEquals("object", byName.get("ping").path("inputSchema").path("type").asText());
  }

  @Test
  void toolsCallReturnsTextContentAndToolErrorEnvelope() {
    ToolServer server = new ToolServer(calcRegistry());
    var args = M.createObjectNode();
    args.put("name", "add");
    args.set("arguments", M.valueToTree(Map.of("a", 5, "b", 3)));
    JsonNode call = server.handle(req(3, "tools/call", args));
    assertFalse(call.path("result").path("isError").asBoolean());
    assertEquals("8", call.path("result").path("content").get(0).path("text").asText());

    // unknown tool => JSON-RPC error (-32602)
    var bad = M.createObjectNode();
    bad.put("name", "nope");
    JsonNode err = server.handle(req(4, "tools/call", bad));
    assertEquals(-32602, err.path("error").path("code").asInt());
  }

  @Test
  void notificationsProduceNoResponse() {
    ToolServer server = new ToolServer(calcRegistry());
    var notif = M.createObjectNode();
    notif.put("jsonrpc", "2.0");
    notif.put("method", "notifications/initialized");
    assertEquals(null, server.handle(notif));
  }

  @Test
  void existingMcpClientInteroperatesWithOurServerSubprocess() throws Exception {
    String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    String cp = System.getProperty("java.class.path");
    List<String> command = List.of(javaBin, "-cp", cp,
        "org.jagentic.core.mcp.server.StdioServerTestMain");

    McpStdioClient client = new McpStdioClient(command);
    try {
      Set<String> names = client.tools().stream().map(s -> s.get("name")).collect(Collectors.toSet());
      assertTrue(names.containsAll(Set.of("add", "shout")), "discovered: " + names);

      ToolRegistry reg = new ToolRegistry();
      List<String> ids = client.register(reg, "srv_");
      assertTrue(ids.contains("srv_add"));
      assertEquals("8", ((String) reg.execute("srv_add", Map.of("a", 5, "b", 3))).trim());
      assertEquals("HI", ((String) reg.execute("srv_shout", Map.of("text", "hi"))).trim());
    } finally {
      client.close();
    }
  }

  private static JsonNode req(int id, String method, JsonNode params) {
    var n = M.createObjectNode();
    n.put("jsonrpc", "2.0");
    n.put("id", id);
    n.put("method", method);
    n.set("params", params);
    return n;
  }
}
