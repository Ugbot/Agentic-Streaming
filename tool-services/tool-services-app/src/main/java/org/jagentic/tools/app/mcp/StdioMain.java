package org.jagentic.tools.app.mcp;

import org.jagentic.core.ToolRegistry;
import org.jagentic.core.mcp.server.StdioToolServer;
import org.jagentic.tools.ToolPacks;

/** MCP <b>stdio</b> entrypoint — a plain {@code main} (no Quarkus boot) so the service can be
 * launched as a subprocess by any MCP client that spawns a stdio server (the common default,
 * incl. our own {@code McpStdioClient} and the {@code mcp:} pipeline section). The pack
 * selection comes from {@code TOOL_PACKS} (env) or the first CLI arg; empty = all packs.
 *
 * <p>Usage: {@code java -cp tool-services-app.jar:... org.jagentic.tools.app.mcp.StdioMain util,web}</p>
 */
public final class StdioMain {

  private StdioMain() {}

  public static void main(String[] args) throws Exception {
    String packs = args.length > 0 ? args[0] : System.getenv().getOrDefault("TOOL_PACKS", "");
    ToolRegistry registry = ToolPacks.buildRegistryFromCsv(packs);
    new StdioToolServer(registry).run();
  }
}
