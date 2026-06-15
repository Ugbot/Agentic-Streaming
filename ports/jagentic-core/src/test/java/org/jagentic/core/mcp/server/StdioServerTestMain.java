package org.jagentic.core.mcp.server;

import java.util.List;
import java.util.Map;

import org.jagentic.core.ToolRegistry;

/** A tiny MCP stdio server launched as a subprocess by {@code ToolServerTest} to prove our
 * {@link StdioToolServer} interoperates with the existing {@link org.jagentic.core.store.McpStdioClient}. */
public final class StdioServerTestMain {

  private StdioServerTestMain() {}

  public static void main(String[] args) throws Exception {
    ToolRegistry reg = new ToolRegistry();
    Map<String, Object> addSchema = Map.of(
        "type", "object",
        "properties", Map.of(
            "a", Map.of("type", "number"),
            "b", Map.of("type", "number")),
        "required", List.of("a", "b"));
    reg.register("add", "Add two numbers.", addSchema, params -> {
      double a = ((Number) params.getOrDefault("a", 0)).doubleValue();
      double b = ((Number) params.getOrDefault("b", 0)).doubleValue();
      double sum = a + b;
      return sum == Math.rint(sum) ? Long.toString((long) sum) : Double.toString(sum);
    });
    reg.register("shout", "Uppercase the text.", params ->
        String.valueOf(params.getOrDefault("text", "")).toUpperCase());
    new StdioToolServer(reg).run();
  }
}
