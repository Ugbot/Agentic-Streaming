package org.agentic.flink.tool;

import org.agentic.flink.tools.ToolExecutor;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Registry of available tools that agents can execute.
 *
 * <p>The ToolRegistry manages tool discovery, registration, and execution. It acts as the
 * central repository for all tools available to agents during job execution.
 *
 * <p><b>Purpose:</b>
 * <ul>
 *   <li>Register tools (functions/APIs) that agents can call</li>
 *   <li>Validate that required tools are available</li>
 *   <li>Provide access to tool executors</li>
 *   <li>Handle tool metadata and schemas</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * ToolRegistry registry = ToolRegistry.builder()
 *     .registerTool("calculator", new CalculatorTool())
 *     .registerTool("web-search", new WebSearchTool())
 *     .registerTool("database-query", new DatabaseQueryTool())
 *     .build();
 *
 * AgentJob job = AgentJob.builder()
 *     .withAgent(myAgent)
 *     .withToolRegistry(registry)
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public class ToolRegistry implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, ToolDefinition> tools;

  private ToolRegistry(ToolRegistryBuilder builder) {
    this.tools = new HashMap<>(builder.tools);
  }

  /**
   * Checks if a tool is registered.
   *
   * @param toolName The tool name
   * @return true if tool is available
   */
  public boolean hasTool(String toolName) {
    return tools.containsKey(toolName);
  }

  /**
   * Gets a tool definition by name.
   *
   * @param toolName The tool name
   * @return Optional containing the tool, or empty if not found
   */
  public Optional<ToolDefinition> getTool(String toolName) {
    return Optional.ofNullable(tools.get(toolName));
  }

  /**
   * Gets all registered tool names.
   *
   * @return Set of tool names
   */
  public java.util.Set<String> getToolNames() {
    return tools.keySet();
  }

  /**
   * Validates that all required tools are available.
   *
   * @param requiredTools Set of required tool names
   * @throws IllegalStateException if any required tool is missing
   */
  public void validateRequiredTools(java.util.Set<String> requiredTools) {
    for (String requiredTool : requiredTools) {
      if (!hasTool(requiredTool)) {
        throw new IllegalStateException("Required tool not found: " + requiredTool);
      }
    }
  }

  /**
   * Gets the executor for a tool.
   *
   * @param toolName The tool name
   * @return Optional containing the executor, or empty if not found
   */
  public Optional<ToolExecutor> getExecutor(String toolName) {
    return Optional.ofNullable(tools.get(toolName))
        .map(ToolDefinition::getExecutor);
  }

  public static ToolRegistryBuilder builder() {
    return new ToolRegistryBuilder();
  }

  /**
   * Creates an empty registry (for testing/dev).
   */
  public static ToolRegistry empty() {
    return new ToolRegistryBuilder().build();
  }

  // ==================== Tool Definition ====================

  /**
   * Represents a tool available for agent use.
   */
  public static class ToolDefinition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String description;
    private final Map<String, Object> schema;
    private final ToolExecutor executor;

    public ToolDefinition(String name, String description, Map<String, Object> schema, ToolExecutor executor) {
      this.name = name;
      this.description = description;
      this.schema = schema;
      this.executor = executor;
    }

    public ToolDefinition(String name, String description, ToolExecutor executor) {
      this(name, description, new HashMap<>(), executor);
    }

    public String getName() { return name; }
    public String getDescription() { return description; }
    public Map<String, Object> getSchema() { return schema; }
    public ToolExecutor getExecutor() { return executor; }
  }

  // ==================== Builder ====================

  public static class ToolRegistryBuilder {
    private final Map<String, ToolDefinition> tools = new HashMap<>();

    /**
     * Registers a tool with its executor.
     *
     * @param toolName The tool name
     * @param executor The tool executor implementation
     * @return this builder
     */
    public ToolRegistryBuilder registerTool(String toolName, ToolExecutor executor) {
      String description = executor.getDescription();
      tools.put(toolName, new ToolDefinition(toolName, description, executor));
      return this;
    }

    /**
     * Registers a tool with name, description, and executor.
     *
     * @param toolName The tool name
     * @param description The tool description
     * @param executor The tool executor implementation
     * @return this builder
     */
    public ToolRegistryBuilder registerTool(String toolName, String description, ToolExecutor executor) {
      tools.put(toolName, new ToolDefinition(toolName, description, executor));
      return this;
    }

    /**
     * Registers a tool definition directly.
     *
     * @param toolName The tool name
     * @param tool The tool definition
     * @return this builder
     */
    public ToolRegistryBuilder registerTool(String toolName, ToolDefinition tool) {
      tools.put(toolName, tool);
      return this;
    }

    /**
     * Registers a tool with just name and description (for testing/placeholders).
     *
     * @param toolName The tool name
     * @param description The tool description
     * @return this builder
     */
    public ToolRegistryBuilder registerTool(String toolName, String description) {
      tools.put(toolName, new ToolDefinition(toolName, description, null));
      return this;
    }

    /**
     * Builds the registry.
     *
     * @return new ToolRegistry
     */
    public ToolRegistry build() {
      return new ToolRegistry(this);
    }
  }
}
