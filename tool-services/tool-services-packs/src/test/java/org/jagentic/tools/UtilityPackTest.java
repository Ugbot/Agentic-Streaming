package org.jagentic.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import org.jagentic.core.ToolRegistry;
import org.jagentic.core.mcp.server.ToolServer;
import org.jagentic.tools.util.UtilityPack;

/** The utility pack registers @Tool methods with real input schemas, executes them, and is
 * servable over MCP via ToolServer — proving the @Tool -> schema -> MCP path end to end. */
class UtilityPackTest {

  private static final ObjectMapper M = new ObjectMapper();

  @Test
  void registersCalculatorAndStringToolsWithSchemas() {
    ToolRegistry reg = new ToolRegistry();
    List<String> ids = new UtilityPack().register(reg);
    assertTrue(ids.contains("util_add"));
    assertTrue(ids.contains("util_toUpperCase"));

    // tools execute through the registry
    assertEquals(8.0, ((Number) reg.execute("util_add", Map.of("a", 5, "b", 3))).doubleValue());
    assertEquals("HELLO", reg.execute("util_toUpperCase", Map.of("text", "hello")));

    // the descriptor carries a real input schema (param names from -parameters)
    Map<String, Object> add = reg.toolDescriptors().stream()
        .filter(d -> d.get("name").equals("util_add")).findFirst().orElseThrow();
    @SuppressWarnings("unchecked")
    Map<String, Object> schema = (Map<String, Object>) add.get("inputSchema");
    @SuppressWarnings("unchecked")
    Map<String, Object> props = (Map<String, Object>) schema.get("properties");
    assertTrue(props.containsKey("a") && props.containsKey("b"), "schema props: " + props);
  }

  @Test
  void buildRegistryFromCsvSelectsPacks() {
    ToolRegistry reg = ToolPacks.buildRegistryFromCsv("util");
    assertTrue(reg.ids().contains("util_concat"));
    assertEquals("ab", reg.execute("util_concat", Map.of("first", "a", "second", "b")));
  }

  @Test
  void utilityPackIsServableOverMcp() {
    ToolServer server = new ToolServer(ToolPacks.buildRegistry(List.of("util")));
    // tools/list exposes the schemas
    JsonNode list = server.handle(req(1, "tools/list", M.createObjectNode()));
    boolean hasAdd = false;
    for (JsonNode t : list.path("result").path("tools")) {
      if (t.path("name").asText().equals("util_add")) {
        hasAdd = true;
        assertTrue(t.path("inputSchema").path("properties").has("a"));
      }
    }
    assertTrue(hasAdd, "util_add not advertised");

    // tools/call round-trips
    var params = M.createObjectNode();
    params.put("name", "util_add");
    params.set("arguments", M.valueToTree(Map.of("a", 40, "b", 2)));
    JsonNode call = server.handle(req(2, "tools/call", params));
    assertFalse(call.path("result").path("isError").asBoolean());
    assertEquals("42.0", call.path("result").path("content").get(0).path("text").asText());
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
