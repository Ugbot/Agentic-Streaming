package org.jagentic.core.pipeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** A document starts only what the operator listed, and nothing starts before the check. */
class McpCommandsTest {

  private static final String KEY = "agentic.mcp.allowed_commands";

  @Test
  void noListMeansNothingStarts() {
    assertThrows(McpCommands.NotAllowed.class, () -> McpCommands.check(List.of("python3", "s.py"), k -> null));
    assertThrows(McpCommands.NotAllowed.class, () -> McpCommands.check(List.of("python3", "s.py"), k -> " ; "));
  }

  @Test
  void anEntryMatchesWordForWordFromTheStart() {
    Map<String, String> s = Map.of(KEY, "python3 server.py; /opt/tool --serve");
    assertDoesNotThrow(() -> McpCommands.check(List.of("python3", "server.py"), s::get));
    assertDoesNotThrow(() -> McpCommands.check(List.of("python3", "server.py", "--port", "1"), s::get));
    assertDoesNotThrow(() -> McpCommands.check(List.of("/opt/tool", "--serve"), s::get));
    assertThrows(McpCommands.NotAllowed.class, () -> McpCommands.check(List.of("python3", "-c", "x"), s::get));
    assertThrows(McpCommands.NotAllowed.class, () -> McpCommands.check(List.of("python3"), s::get));
    assertThrows(McpCommands.NotAllowed.class, () -> McpCommands.check(List.of("/opt/tool"), s::get));
    assertThrows(McpCommands.NotAllowed.class, () -> McpCommands.check(List.of("sh", "-c", "python3 server.py"), s::get));
  }

  @Test
  void aHostileDocumentRunsNothing() throws Exception {
    Path planted = Files.createTempFile("mcp-planted", ".txt");
    Files.delete(planted);
    Map<String, Object> doc = Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule")), "verifier", Map.of("kind", "none")),
        "mcp", List.of(Map.of("name", "x", "transport", "stdio",
            "command", List.of("touch", planted.toString()))));
    McpCommands.NotAllowed refused = assertThrows(McpCommands.NotAllowed.class, () -> GraphBuilder.build(doc, null));
    assertTrue(refused.getMessage().contains("AGENTIC_MCP_ALLOWED_COMMANDS"));
    assertTrue(Files.notExists(planted), "the document's command never ran");
  }
}
