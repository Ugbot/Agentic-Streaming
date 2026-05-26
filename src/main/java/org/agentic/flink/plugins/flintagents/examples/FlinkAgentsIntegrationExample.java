package org.agentic.flink.plugins.flintagents.examples;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.plugins.flintagents.adapter.FlinkAgentsEventAdapter;
import org.agentic.flink.plugins.flintagents.adapter.FlinkAgentsToolAdapter;
import org.agentic.flink.tools.ToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.Event;

/**
 * Example demonstrating integration between our Agentic Flink framework and Apache Flink Agents.
 *
 * <p>This example shows:
 *
 * <ol>
 *   <li>Converting our AgentEvent to Flink Agents Event using FlinkAgentsEventAdapter
 *   <li>Wrapping our ToolExecutor as Flink Agents Agent using FlinkAgentsToolAdapter
 *   <li>Bidirectional event conversion (our events ↔ Flink Agents events)
 *   <li>Using our tools within Flink Agents architecture
 * </ol>
 *
 * <p><b>Hybrid Architecture Benefits:</b>
 *
 * <ul>
 *   <li>✅ Use official Apache Flink Agents framework (ReAct, MCP protocol, observability)
 *   <li>✅ Keep our innovations (context management, validation/correction, comprehensive RAG)
 *   <li>✅ Seamless interoperability through adapter layer
 *   <li>✅ Future-proof with Apache Flink roadmap
 * </ul>
 *
 * @author Agentic Flink Team
 */
public class FlinkAgentsIntegrationExample {

  public static void main(String[] args) {
    System.out.println("=".repeat(80));
    System.out.println("Apache Flink Agents Integration Example");
    System.out.println("Demonstrating Hybrid Architecture: Our Framework + Flink Agents");
    System.out.println("=".repeat(80));

    // Part 1: Event Conversion
    demonstrateEventConversion();

    // Part 2: Tool Wrapping
    demonstrateToolWrapping();

    // Part 3: Bidirectional Conversion
    demonstrateBidirectionalConversion();

    System.out.println("\n" + "=".repeat(80));
    System.out.println("Integration Example Complete!");
    System.out.println("=".repeat(80));
  }

  /** Demonstrates converting our events to Flink Agents events. */
  private static void demonstrateEventConversion() {
    System.out.println("\n--- Part 1: Event Conversion (Our Events → Flink Agents) ---\n");

    // Create our AgentEvent
    AgentEvent ourEvent =
        new AgentEvent(
            "flow-001", // flowId
            "user-123", // userId
            "agent-calculator", // agentId
            AgentEventType.TOOL_CALL_REQUESTED // eventType
            );

    Map<String, Object> data = new HashMap<>();
    data.put("operation", "add");
    data.put("operand1", 42);
    data.put("operand2", 58);
    ourEvent.setData(data);
    ourEvent.setCurrentStage("execution");
    ourEvent.setIterationNumber(1);
    ourEvent.setTimestamp(System.currentTimeMillis());

    System.out.println("Our AgentEvent:");
    System.out.println("  - Flow ID: " + ourEvent.getFlowId());
    System.out.println("  - User ID: " + ourEvent.getUserId());
    System.out.println("  - Agent ID: " + ourEvent.getAgentId());
    System.out.println("  - Event Type: " + ourEvent.getEventType());
    System.out.println("  - Stage: " + ourEvent.getCurrentStage());
    System.out.println("  - Data: " + ourEvent.getData());

    // Convert to Flink Agents Event
    Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(ourEvent);

    System.out.println("\nConverted to Flink Agents Event:");
    System.out.println("  - Event ID: " + flinkEvent.getId());
    System.out.println("  - Flow ID (attr): " + flinkEvent.getAttr("flowId"));
    System.out.println("  - User ID (attr): " + flinkEvent.getAttr("userId"));
    System.out.println("  - Agent ID (attr): " + flinkEvent.getAttr("agentId"));
    System.out.println("  - Event Type (attr): " + flinkEvent.getAttr("eventType"));
    System.out.println("  - Timestamp: " + flinkEvent.getSourceTimestamp());
    System.out.println("  - All Attributes: " + flinkEvent.getAttributes().size() + " attributes");

    System.out.println("\n✅ Event conversion successful!");
  }

  /** Demonstrates wrapping our tools as Flink Agents. */
  private static void demonstrateToolWrapping() {
    System.out.println("\n--- Part 2: Tool Wrapping (Our Tools → Flink Agents) ---\n");

    // Create a simple calculator tool
    ToolExecutor calculatorTool =
        new ToolExecutor() {
          @Override
          public CompletableFuture<Object> execute(Map<String, Object> parameters) {
            String operation = (String) parameters.get("operation");
            Number operand1 = (Number) parameters.get("operand1");
            Number operand2 = (Number) parameters.get("operand2");

            double result = 0;
            switch (operation) {
              case "add":
                result = operand1.doubleValue() + operand2.doubleValue();
                break;
              case "subtract":
                result = operand1.doubleValue() - operand2.doubleValue();
                break;
              case "multiply":
                result = operand1.doubleValue() * operand2.doubleValue();
                break;
              case "divide":
                result = operand1.doubleValue() / operand2.doubleValue();
                break;
            }

            return CompletableFuture.completedFuture(result);
          }

          @Override
          public String getToolId() {
            return "calculator";
          }

          @Override
          public String getDescription() {
            return "Performs basic arithmetic operations";
          }

          @Override
          public boolean validateParameters(Map<String, Object> parameters) {
            return parameters.containsKey("operation")
                && parameters.containsKey("operand1")
                && parameters.containsKey("operand2");
          }
        };

    // Create tool definition
    ToolDefinition calculatorDef = new ToolDefinition();
    calculatorDef.setToolId("calculator");
    calculatorDef.setName("Calculator");
    calculatorDef.setDescription("Performs basic arithmetic operations");
    calculatorDef.setInputSchema(
        Map.of(
            "operation", "string (add, subtract, multiply, divide)",
            "operand1", "number",
            "operand2", "number"));
    calculatorDef.setOutputSchema(Map.of("result", "number"));

    System.out.println("Our ToolExecutor:");
    System.out.println("  - Tool ID: " + calculatorDef.getToolId());
    System.out.println("  - Description: " + calculatorDef.getDescription());
    System.out.println("  - Input Schema: " + calculatorDef.getInputSchema());
    System.out.println("  - Output Schema: " + calculatorDef.getOutputSchema());

    // Wrap as Flink Agents Agent
    Agent toolAgent =
        FlinkAgentsToolAdapter.wrapSingleTool("calculator", calculatorTool, calculatorDef);

    System.out.println("\nWrapped as Flink Agents Agent:");
    System.out.println("  - Agent Class: " + toolAgent.getClass().getSimpleName());
    System.out.println(
        "  - Agent ID: "
            + ((FlinkAgentsToolAdapter.ToolWrapperAgent) toolAgent).getAgentId());

    // Test MCP schema conversion
    Map<String, Object> mcpSchema = FlinkAgentsToolAdapter.toMCPToolSchema(calculatorDef);
    System.out.println("\nMCP Tool Schema:");
    System.out.println("  - Name: " + mcpSchema.get("name"));
    System.out.println("  - Description: " + mcpSchema.get("description"));

    System.out.println("\n✅ Tool wrapping successful!");
  }

  /** Demonstrates bidirectional event conversion. */
  private static void demonstrateBidirectionalConversion() {
    System.out.println("\n--- Part 3: Bidirectional Conversion (Round-trip Test) ---\n");

    // Create original event
    AgentEvent original =
        new AgentEvent("flow-999", "user-999", "agent-test", AgentEventType.VALIDATION_PASSED);

    Map<String, Object> testData = new HashMap<>();
    testData.put("result", "success");
    testData.put("confidence", 0.95);
    original.setData(testData);
    original.setCurrentStage("validation");
    original.setIterationNumber(3);
    original.setTimestamp(System.currentTimeMillis());

    System.out.println("Original AgentEvent:");
    System.out.println(FlinkAgentsEventAdapter.debugSummary(original));

    // Convert to Flink Agents
    Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(original);

    // Convert back to our format
    AgentEvent converted = FlinkAgentsEventAdapter.fromFlinkAgentEvent(flinkEvent);

    System.out.println("\nAfter Round-trip Conversion:");
    System.out.println(FlinkAgentsEventAdapter.debugSummary(converted));

    // Validate conversion
    boolean isLossless = FlinkAgentsEventAdapter.validateConversion(original, converted);

    System.out.println("\nConversion Validation:");
    System.out.println("  - Flow ID preserved: " + original.getFlowId().equals(converted.getFlowId()));
    System.out.println("  - User ID preserved: " + original.getUserId().equals(converted.getUserId()));
    System.out.println("  - Agent ID preserved: " + original.getAgentId().equals(converted.getAgentId()));
    System.out.println("  - Event Type preserved: " + original.getEventType().equals(converted.getEventType()));
    System.out.println("  - Stage preserved: " + original.getCurrentStage().equals(converted.getCurrentStage()));
    System.out.println("  - Iteration preserved: " + original.getIterationNumber().equals(converted.getIterationNumber()));
    System.out.println("  - Overall lossless: " + isLossless);

    if (isLossless) {
      System.out.println("\n✅ Bidirectional conversion is lossless!");
    } else {
      System.out.println("\n⚠️  Some data was lost in conversion");
    }
  }

  /**
   * Example of how to use these adapters in a real Flink job.
   *
   * <pre>{@code
   * // Create Flink environment
   * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
   *
   * // Create our event stream
   * DataStream<AgentEvent> ourEvents = env.fromElements(
   *     new AgentEvent("flow-001", "user-001", "agent-001", AgentEventType.TOOL_CALL_REQUESTED),
   *     new AgentEvent("flow-002", "user-002", "agent-002", AgentEventType.TOOL_CALL_REQUESTED)
   * );
   *
   * // Convert to Flink Agents events
   * DataStream<Event> flinkEvents = ourEvents.map(
   *     event -> FlinkAgentsEventAdapter.toFlinkAgentEvent(event)
   * );
   *
   * // Create tool wrapper agent
   * Map<String, ToolExecutor> tools = Map.of("calculator", new CalculatorToolExecutor());
   * Map<String, ToolDefinition> defs = Map.of("calculator", calculatorDef);
   * Agent toolAgent = FlinkAgentsToolAdapter.createToolWrapperAgent("tools", tools, defs);
   *
   * // Use Flink Agents runtime (when available)
   * // AgentRuntime runtime = new AgentRuntime(env);
   * // DataStream<Event> results = runtime.execute(toolAgent, flinkEvents);
   *
   * // Convert results back to our format
   * // DataStream<AgentEvent> ourResults = results.map(
   * //     event -> FlinkAgentsEventAdapter.fromFlinkAgentEvent(event)
   * // );
   * }</pre>
   */
  public static class FlinkJobIntegrationExample {
    // Documentation placeholder
  }
}
