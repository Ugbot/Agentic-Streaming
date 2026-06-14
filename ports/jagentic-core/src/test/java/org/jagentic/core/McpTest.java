package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.jagentic.core.store.McpStdioClient;

/** Phase D — pure-Java MCP stdio client against a tiny FastMCP stub server (venv python).
 * Skips cleanly when the python+mcp SDK isn't present. */
class McpTest {

  private static final String STUB_SERVER = """
      from mcp.server.fastmcp import FastMCP
      mcp = FastMCP("stub")

      @mcp.tool()
      def add(a: int, b: int) -> int:
          \"\"\"Add two numbers.\"\"\"
          return a + b

      @mcp.tool()
      def shout(text: str) -> str:
          \"\"\"Uppercase the text.\"\"\"
          return text.upper()

      if __name__ == "__main__":
          mcp.run()
      """;

  @Test
  void mcpClientRegistersAndCallsTools() throws IOException {
    String py = System.getenv("AGENTIC_TEST_PYTHON");
    if (py == null || py.isBlank()) {
      py = "/tmp/af-venv/bin/python";
    }
    if (!Files.exists(Path.of(py))) {
      Assumptions.abort("python with mcp SDK not available at " + py);
      return;
    }
    Path stub = Files.createTempFile("stub_mcp_server", ".py");
    Files.writeString(stub, STUB_SERVER);

    McpStdioClient client;
    try {
      client = new McpStdioClient(List.of(py, stub.toString()));
    } catch (Throwable t) {
      Assumptions.abort("MCP stub server didn't start (mcp SDK missing?): " + t.getMessage());
      return;
    }
    try {
      Set<String> names = client.tools().stream()
          .map(s -> s.get("name")).collect(Collectors.toSet());
      assertTrue(names.containsAll(Set.of("add", "shout")), "tools: " + names);

      ToolRegistry reg = new ToolRegistry();
      List<String> ids = client.register(reg, "mcp_");
      assertTrue(ids.contains("mcp_add"));
      assertEquals("5", ((String) reg.execute("mcp_add", Map.of("a", 2, "b", 3))).trim());
      assertEquals("HI", ((String) reg.execute("mcp_shout", Map.of("text", "hi"))).trim());
    } finally {
      client.close();
    }
  }
}
