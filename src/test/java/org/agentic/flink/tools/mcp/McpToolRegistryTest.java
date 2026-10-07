package org.agentic.flink.tools.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.tools.ToolExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@link McpToolRegistry} and {@link McpToolExecutor} against an in-process fake MCP server that
 * speaks JSON-RPC over HTTP: {@code initialize}, {@code tools/list} and {@code tools/call}.
 */
class McpToolRegistryTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpServer server;
  private final List<String> toolNames = new ArrayList<>();
  private final ConcurrentLinkedQueue<JsonNode> calls = new ConcurrentLinkedQueue<>();
  private String failingTool;

  @BeforeEach
  void start() throws IOException {
    int n = ThreadLocalRandom.current().nextInt(2, 6);
    for (int i = 0; i < n; i++) {
      toolNames.add("tool_" + UUID.randomUUID().toString().replace('-', '_'));
    }
    failingTool = toolNames.get(ThreadLocalRandom.current().nextInt(n));

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/mcp",
        ex -> {
          JsonNode req = MAPPER.readTree(ex.getRequestBody());
          ObjectNode resp = MAPPER.createObjectNode();
          resp.put("jsonrpc", "2.0");
          resp.set("id", req.get("id"));
          String method = req.path("method").asText();
          ObjectNode result = MAPPER.createObjectNode();
          switch (method) {
            case "initialize":
              result.putObject("serverInfo").put("name", "fake-mcp");
              break;
            case "tools/list":
              ArrayNode tools = result.putArray("tools");
              for (String name : toolNames) {
                ObjectNode t = tools.addObject();
                t.put("name", name);
                t.put("description", "describes " + name);
                ObjectNode schema = t.putObject("inputSchema");
                schema.put("type", "object");
                schema.putObject("properties").putObject("text").put("type", "string");
              }
              break;
            case "tools/call":
              JsonNode params = req.path("params");
              calls.add(params);
              String name = params.path("name").asText();
              ArrayNode content = result.putArray("content");
              if (name.equals(failingTool)) {
                result.put("isError", true);
                content.addObject().put("type", "text").put("text", "boom:" + name);
              } else {
                String echoed = params.path("arguments").path("text").asText();
                content.addObject().put("type", "text").put("text", name + ":" + echoed);
              }
              break;
            default:
              resp.putObject("error").put("code", -32601).put("message", "unknown " + method);
          }
          if (!resp.has("error")) {
            resp.set("result", result);
          }
          byte[] body = MAPPER.writeValueAsBytes(resp);
          ex.getResponseHeaders().add("Content-Type", "application/json");
          ex.sendResponseHeaders(200, body.length);
          ex.getResponseBody().write(body);
          ex.close();
        });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private McpServerSpec spec(String serverName) {
    return McpServerSpec.http(
        serverName, "http://127.0.0.1:" + server.getAddress().getPort() + "/mcp");
  }

  @Test
  void discoverWrapsEveryAdvertisedToolAsANamespacedExecutor() throws Exception {
    String serverName = "srv-" + UUID.randomUUID().toString().substring(0, 8);
    List<McpToolExecutor> executors = McpToolRegistry.discover(spec(serverName));

    assertEquals(toolNames.size(), executors.size());
    for (int i = 0; i < toolNames.size(); i++) {
      McpToolExecutor executor = executors.get(i);
      assertInstanceOf(ToolExecutor.class, executor);
      assertEquals(serverName + ":" + toolNames.get(i), executor.getToolId());
      assertEquals("describes " + toolNames.get(i), executor.getDescription());
      assertEquals(toolNames.get(i), executor.getMetadata().getName());
      assertEquals("object", executor.getMetadata().getInputSchema().get("type"));
      assertEquals(serverName, executor.getServerSpec().getName());
    }
  }

  @Test
  void executorsShareOneClientAndForwardArgumentsToToolsCall() throws Exception {
    McpServerSpec spec = spec("shared");
    try (McpClient client = new McpClient(spec)) {
      client.initialize();
      List<McpToolExecutor> executors = McpToolRegistry.discover(spec, client);

      for (McpToolExecutor executor : executors) {
        String name = executor.getMetadata().getName();
        if (name.equals(failingTool)) continue;
        String text = "arg-" + UUID.randomUUID();
        Object out = executor.execute(Map.of("text", text)).get(10, TimeUnit.SECONDS);
        assertEquals(name + ":" + text, out);
      }
    }
    int expectedCalls = toolNames.size() - 1;
    assertEquals(expectedCalls, calls.size());
    for (JsonNode call : calls) {
      assertTrue(toolNames.contains(call.path("name").asText()));
      assertTrue(call.path("arguments").path("text").asText().startsWith("arg-"));
    }
  }

  @Test
  void serverSideToolErrorSurfacesAsAFailedFuture() throws Exception {
    List<McpToolExecutor> executors = McpToolRegistry.discover(spec("err"));
    McpToolExecutor failing =
        executors.stream()
            .filter(e -> e.getMetadata().getName().equals(failingTool))
            .findFirst()
            .orElseThrow();

    ExecutionException e =
        assertThrows(
            ExecutionException.class,
            () -> failing.execute(Map.of("text", "x")).get(10, TimeUnit.SECONDS));
    assertTrue(e.getCause().getMessage().contains(failingTool), e.getCause().getMessage());
    assertTrue(
        e.getCause().getMessage().contains("boom:" + failingTool), e.getCause().getMessage());
  }

  @Test
  void executorWithoutAPrebuiltClientOpensItsOwnOnFirstCall() throws Exception {
    McpServerSpec spec = spec("lazy");
    String name = toolNames.stream().filter(t -> !t.equals(failingTool)).findFirst().orElseThrow();
    McpToolExecutor executor = new McpToolExecutor(spec, new McpToolMetadata(name, "d", Map.of()));
    String text = UUID.randomUUID().toString();

    Object out = executor.execute(Map.of("text", text)).get(10, TimeUnit.SECONDS);

    assertEquals(name + ":" + text, out);
  }
}
