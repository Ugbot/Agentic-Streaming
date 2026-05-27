package org.agentic.flink.example;

import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.tool.ToolRegistry;
import java.util.concurrent.CompletableFuture;

/**
 * Tool Execution Example - Real LLM + Real Tool Execution
 *
 * <p>This example demonstrates the complete agentic loop with:
 * <ul>
 *   <li>Real LLM calls via LangChain4J/Ollama</li>
 *   <li>Real tool execution with ToolExecutor implementations</li>
 *   <li>Multi-step reasoning (LLM → Tool → LLM → Result)</li>
 *   <li>No Flink - just the agent engine</li>
 * </ul>
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 * # Start Ollama
 * docker compose up -d ollama
 *
 * # Pull model
 * docker compose exec ollama ollama pull qwen2.5:3b
 *
 * # Or if running locally:
 * ollama pull qwen2.5:3b
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.ToolExecutionExample"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class ToolExecutionExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=".repeat(80));
    System.out.println("  Tool Execution Example - Real LLM + Real Tools");
    System.out.println("=".repeat(80));
    System.out.println();

    // ==================== Step 1: Define Agent with Tools ====================

    System.out.println("📝 Step 1: Defining agent with tool support...\n");

    Agent agent = Agent.builder()
        .withId("calculator-assistant")
        .withName("Calculator Assistant")
        .withType(AgentType.EXECUTOR)
        .withSystemPrompt(
            "You are a calculator assistant. You have access to these tools:\n" +
            "- calculator-add: Adds two numbers (parameters: a, b)\n" +
            "- calculator-multiply: Multiplies two numbers (parameters: a, b)\n\n" +
            "When the user asks you to perform calculations, use the appropriate tools. " +
            "Call tools by outputting: TOOL_CALL: <tool_name> {\"a\": <number>, \"b\": <number>}\n\n" +
            "Example: To add 5 and 3, output: TOOL_CALL: calculator-add {\"a\": 5, \"b\": 3}")
        .withChatSetup(ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.3).build())
        .withMaxIterations(5)
        .build();

    System.out.println("✅ Agent created: " + agent.getAgentName());
    System.out.println();

    // ==================== Step 2: Setup Tool Registry ====================

    System.out.println("🔧 Step 2: Setting up tool registry with real executors...\n");

    ToolRegistry toolRegistry = ToolRegistry.builder()
        .registerTool("calculator-add", new SimpleCalculatorTool("add"))
        .registerTool("calculator-multiply", new SimpleCalculatorTool("multiply"))
        .build();

    System.out.println("✅ Tool registry created with real executors:");
    toolRegistry.getToolNames().forEach(name -> {
      toolRegistry.getExecutor(name).ifPresent(executor -> {
        System.out.println("   - " + name + ": " + executor.getDescription());
      });
    });
    System.out.println();

    // ==================== Step 3: Create LLM Client ====================

    System.out.println("🔗 Step 3: Creating LLM client...\n");

    LLMClient llmClient = LLMClient.builder()
        .withModel("qwen2.5:3b")
        .withTemperature(0.3)
        .withBaseUrl(ConfigKeys.DEFAULT_OLLAMA_BASE_URL)
        .build();

    System.out.println("✅ LLM client created (Ollama @ localhost:11434)");
    System.out.println();

    // ==================== Step 4: Create Agent Executor ====================

    System.out.println("⚙️  Step 4: Creating agent executor with tools...\n");

    AgentExecutor executor = AgentExecutor.builder()
        .withAgent(agent)
        .withToolRegistry(toolRegistry)
        .withLlmClient(llmClient)
        .build();

    System.out.println("✅ Agent executor ready with real tool execution");
    System.out.println();

    // ==================== Step 5: Execute with Tool Calls ====================

    System.out.println("🚀 Step 5: Executing agent with real LLM + real tools...\n");
    System.out.println("-".repeat(80));

    // Create input event
    AgentEvent inputEvent = new AgentEvent(
        "flow-001",
        "user-001",
        "calculator-assistant",
        AgentEventType.FLOW_STARTED
    );
    inputEvent.putData("user_message", "Calculate: (5 + 3) * 2. Show your work.");

    System.out.println("❓ Question: Calculate: (5 + 3) * 2. Show your work.");
    System.out.println();
    System.out.println("⏳ Calling LLM and executing tools...");
    System.out.println("   (This may take a few seconds)");
    System.out.println();

    // Execute!
    CompletableFuture<ExecutionResult> future = executor.execute(inputEvent);
    ExecutionResult result = future.get();

    // ==================== Step 6: Show Results ====================

    System.out.println("-".repeat(80));
    System.out.println();

    if (result.isSuccess()) {
      System.out.println("✅ SUCCESS!");
      System.out.println();
      System.out.println("📄 Response:");
      System.out.println(result.getOutput());
      System.out.println();
      System.out.println("📊 Execution Metrics:");
      System.out.println("   - Events generated: " + result.getEvents().size());
      System.out.println("   - Tool calls executed: " +
          (result.getToolCalls() != null ? result.getToolCalls().size() : 0));

      if (result.getToolCalls() != null && !result.getToolCalls().isEmpty()) {
        System.out.println();
        System.out.println("🔧 Tool Execution Details:");
        result.getToolCalls().forEach(toolCall -> {
          System.out.println("   - " + toolCall.getToolName() + ": " +
              (toolCall.isSuccess() ? "✓ " + toolCall.getResult() : "✗ " + toolCall.getError()));
        });
      }
    } else {
      System.out.println("❌ FAILED!");
      System.out.println("Error: " + result.getErrorMessage());
    }

    System.out.println();
    System.out.println("=".repeat(80));
    System.out.println("  Real tool execution working! 🎉");
    System.out.println("  - LLM made decisions");
    System.out.println("  - Tools executed real code");
    System.out.println("  - Multi-step reasoning completed");
    System.out.println("=".repeat(80));
  }
}
