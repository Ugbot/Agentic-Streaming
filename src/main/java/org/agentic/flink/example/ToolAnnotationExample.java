package org.agentic.flink.example;

import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.langchain.LangChainToolAdapter;
import org.agentic.flink.langchain.ToolAnnotationRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Example demonstrating LangChain4j @Tool annotation integration.
 *
 * <p>This example shows how to:
 * <ul>
 *   <li>Scan for @Tool annotated methods using ToolAnnotationRegistry
 *   <li>Invoke tools using LangChainToolAdapter
 *   <li>Use annotation-based tools within the Flink agent framework
 * </ul>
 *
 * <p>The @Tool pattern enables automatic tool discovery and reduces boilerplate code.
 *
 * @author Agentic Flink Team
 */
public class ToolAnnotationExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=== Flink Agents @Tool Annotation Example ===\n");

    // Step 1: Create ToolAnnotationRegistry to scan for @Tool methods
    System.out.println("Step 1: Scanning for @Tool annotated methods...");
    ToolAnnotationRegistry registry =
        new ToolAnnotationRegistry("org.agentic.flink.tools.builtin");

    System.out.println("Found " + registry.getToolCount() + " tools\n");

    // Step 2: List discovered tools
    System.out.println("Step 2: Discovered tools:");
    Map<String, ToolDefinition> tools = registry.getToolDefinitions();
    for (Map.Entry<String, ToolDefinition> entry : tools.entrySet()) {
      ToolDefinition def = entry.getValue();
      System.out.println(
          "  - "
              + entry.getKey()
              + ": "
              + def.getName()
              + " - "
              + def.getDescription());
    }
    System.out.println();

    // Step 3: Execute a calculator tool
    System.out.println("Step 3: Executing calculator tool (add)...");
    executeCalculatorExample(registry);

    // Step 4: Execute a string tool
    System.out.println("\nStep 4: Executing string tool (toUpperCase)...");
    executeStringExample(registry);

    // Step 5: Show tool metadata
    System.out.println("\nStep 5: Tool metadata example:");
    showToolMetadata(registry, "calculator_add");

    System.out.println("\n=== Example Complete ===");
  }

  /**
   * Demonstrates executing a calculator tool.
   *
   * @param registry The tool registry
   */
  private static void executeCalculatorExample(ToolAnnotationRegistry registry) throws Exception {
    String toolId = "calculator_add";

    // Create adapter
    LangChainToolAdapter adapter = new LangChainToolAdapter(toolId, registry);

    // Prepare parameters
    Map<String, Object> params = new HashMap<>();
    params.put("a", 15.5);
    params.put("b", 7.3);

    // Execute
    System.out.println("  Calling: add(15.5, 7.3)");
    CompletableFuture<Object> result = adapter.execute(params);

    Object value = result.get();
    System.out.println("  Result: " + value);
  }

  /**
   * Demonstrates executing a string tool.
   *
   * @param registry The tool registry
   */
  private static void executeStringExample(ToolAnnotationRegistry registry) throws Exception {
    String toolId = "string_touppercase";

    // Create adapter
    LangChainToolAdapter adapter = new LangChainToolAdapter(toolId, registry);

    // Prepare parameters
    Map<String, Object> params = new HashMap<>();
    params.put("text", "hello, world!");

    // Execute
    System.out.println("  Calling: toUpperCase(\"hello, world!\")");
    CompletableFuture<Object> result = adapter.execute(params);

    Object value = result.get();
    System.out.println("  Result: " + value);
  }

  /**
   * Shows tool metadata from the registry.
   *
   * @param registry The tool registry
   * @param toolId The tool ID to inspect
   */
  private static void showToolMetadata(ToolAnnotationRegistry registry, String toolId) {
    ToolDefinition def = registry.getToolDefinition(toolId);
    if (def == null) {
      System.out.println("  Tool not found: " + toolId);
      return;
    }

    System.out.println("  Tool ID: " + def.getToolId());
    System.out.println("  Name: " + def.getName());
    System.out.println("  Description: " + def.getDescription());
    System.out.println("  Version: " + def.getVersion());
    System.out.println("  Executor: " + def.getExecutorClass());
    System.out.println("  Input Schema: " + def.getInputSchema());
    System.out.println("  Output Schema: " + def.getOutputSchema());
  }
}
