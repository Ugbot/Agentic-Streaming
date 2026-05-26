package org.agentic.flink.tools.mcp;

import java.io.Serializable;
import java.util.Map;

/** Metadata returned by {@code tools/list} on an MCP server. */
public final class McpToolMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final String description;
  private final Map<String, Object> inputSchema;

  public McpToolMetadata(String name, String description, Map<String, Object> inputSchema) {
    this.name = name;
    this.description = description == null ? "" : description;
    this.inputSchema = inputSchema == null ? Map.of() : Map.copyOf(inputSchema);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, Object> getInputSchema() {
    return inputSchema;
  }
}
