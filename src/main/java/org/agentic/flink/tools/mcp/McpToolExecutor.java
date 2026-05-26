package org.agentic.flink.tools.mcp;

import org.agentic.flink.tools.ToolExecutor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Adapts a single tool exposed by an MCP server into our {@link ToolExecutor} interface.
 *
 * <p>One instance per MCP tool. The wrapping {@link McpClient} is supplied at construction —
 * typically by {@link McpToolRegistry#discover(McpServerSpec)}, which builds one client per
 * server and N executors per tool against it.
 *
 * <p>Calls are executed on the common pool through {@link CompletableFuture#supplyAsync(java.util.function.Supplier)}
 * to match the async contract of {@link ToolExecutor#execute(Map)} without blocking the calling
 * Flink task thread.
 */
public final class McpToolExecutor implements ToolExecutor {
  private static final long serialVersionUID = 1L;

  private final McpServerSpec serverSpec;
  private final McpToolMetadata metadata;
  // Transient — the live client is bound to its operator task; specs ride along in the graph.
  private transient McpClient client;

  public McpToolExecutor(McpServerSpec serverSpec, McpToolMetadata metadata) {
    this(serverSpec, metadata, null);
  }

  /** Construct with a pre-built client (typical path from {@link McpToolRegistry}). */
  public McpToolExecutor(McpServerSpec serverSpec, McpToolMetadata metadata, McpClient client) {
    this.serverSpec = Objects.requireNonNull(serverSpec, "serverSpec");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.client = client;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return client().callTool(metadata.getName(), parameters);
          } catch (Exception e) {
            throw new RuntimeException(
                "MCP tool '" + metadata.getName() + "' failed: " + e.getMessage(), e);
          }
        });
  }

  @Override
  public String getToolId() {
    // Namespace by server name so tools from different servers can coexist.
    return serverSpec.getName() + ":" + metadata.getName();
  }

  @Override
  public String getDescription() {
    return metadata.getDescription();
  }

  public McpToolMetadata getMetadata() {
    return metadata;
  }

  public McpServerSpec getServerSpec() {
    return serverSpec;
  }

  private synchronized McpClient client() throws java.io.IOException {
    if (client == null) {
      client = new McpClient(serverSpec);
    }
    client.initialize();
    return client;
  }
}
