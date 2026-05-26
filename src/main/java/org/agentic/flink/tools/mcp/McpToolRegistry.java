package org.agentic.flink.tools.mcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers tools exposed by an MCP server and produces {@link McpToolExecutor}s for each.
 *
 * <p>Typical usage from an agent operator's {@code open()}:
 *
 * <pre>{@code
 * try (McpClient client = new McpClient(spec)) {
 *     client.initialize();
 *     List<McpToolExecutor> tools = McpToolRegistry.discover(spec, client);
 *     for (McpToolExecutor t : tools) {
 *         toolRegistry.register(t.getToolId(), t);
 *     }
 * }
 * }</pre>
 */
public final class McpToolRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(McpToolRegistry.class);

  private McpToolRegistry() {}

  /** Convenience: open a fresh client, list tools, return executors sharing that client. */
  public static List<McpToolExecutor> discover(McpServerSpec spec) throws IOException {
    McpClient client = new McpClient(spec);
    client.initialize();
    return discover(spec, client);
  }

  /**
   * List tools via the supplied (already-initialized) client and wrap each as an executor.
   * The client is reused by every returned executor.
   */
  public static List<McpToolExecutor> discover(McpServerSpec spec, McpClient client)
      throws IOException {
    List<McpToolMetadata> tools = client.listTools();
    LOG.info("Discovered {} MCP tools from server '{}'", tools.size(), spec.getName());
    List<McpToolExecutor> executors = new ArrayList<>(tools.size());
    for (McpToolMetadata t : tools) {
      executors.add(new McpToolExecutor(spec, t, client));
    }
    return executors;
  }
}
