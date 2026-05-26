package org.agentic.flink.example;

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
 * Quick Start Example - Real LangChain4J Integration
 *
 * <p>This example demonstrates the REAL working integration:
 * <ul>
 *   <li>Real LangChain4J Ollama calls</li>
 *   <li>Declarative agent definition</li>
 *   <li>Actual LLM-based execution</li>
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
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.QuickStartExample"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class QuickStartExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=".repeat(80));
    System.out.println("  Quick Start - Real LangChain4J Integration");
    System.out.println("=".repeat(80));
    System.out.println();

    // ==================== Step 1: Define Agent ====================

    System.out.println("📝 Step 1: Defining agent...\n");

    Agent agent = Agent.builder()
        .withId("assistant")
        .withName("Helpful Assistant")
        .withType(AgentType.EXECUTOR)
        .withSystemPrompt(
            "You are a helpful AI assistant. Answer questions clearly and concisely. " +
            "If you don't know something, say so.")
        .withLlmModel("qwen2.5:latest")
        .withTemperature(0.7)
        .withMaxIterations(3)
        .build();

    System.out.println("✅ Agent created: " + agent.getAgentName());
    System.out.println("   Model: " + agent.getLlmModel());
    System.out.println();

    // ==================== Step 2: Create LLM Client ====================

    System.out.println("🔗 Step 2: Creating LLM client...\n");

    LLMClient llmClient = LLMClient.builder()
        .withModel("qwen2.5:latest")
        .withTemperature(0.7)
        .withBaseUrl(ConfigKeys.DEFAULT_OLLAMA_BASE_URL)
        .build();

    System.out.println("✅ LLM client created (Ollama @ localhost:11434)");
    System.out.println();

    // ==================== Step 3: Create Agent Executor ====================

    System.out.println("⚙️  Step 3: Creating agent executor...\n");

    AgentExecutor executor = AgentExecutor.builder()
        .withAgent(agent)
        .withToolRegistry(ToolRegistry.empty())
        .withLlmClient(llmClient)
        .build();

    System.out.println("✅ Agent executor ready");
    System.out.println();

    // ==================== Step 4: Execute Agent ====================

    System.out.println("🚀 Step 4: Executing agent with real LLM...\n");
    System.out.println("-".repeat(80));

    // Create input event
    AgentEvent inputEvent = new AgentEvent(
        "flow-001",
        "user-001",
        "assistant",
        AgentEventType.FLOW_STARTED
    );
    inputEvent.putData("user_message", "What is Apache Flink and why is it useful for AI?");

    System.out.println("❓ Question: What is Apache Flink and why is it useful for AI?");
    System.out.println();
    System.out.println("⏳ Calling Ollama... (this may take a few seconds)");
    System.out.println();

    // Execute!
    CompletableFuture<ExecutionResult> future = executor.execute(inputEvent);
    ExecutionResult result = future.get();

    // ==================== Step 5: Show Results ====================

    System.out.println("-".repeat(80));
    System.out.println();

    if (result.isSuccess()) {
      System.out.println("✅ SUCCESS!");
      System.out.println();
      System.out.println("📄 Response:");
      System.out.println(result.getOutput());
      System.out.println();
      System.out.println("📊 Metrics:");
      System.out.println("   - Events generated: " + result.getEvents().size());
      System.out.println("   - Tool calls: " + (result.getToolCalls() != null ? result.getToolCalls().size() : 0));
    } else {
      System.out.println("❌ FAILED!");
      System.out.println("Error: " + result.getErrorMessage());
    }

    System.out.println();
    System.out.println("=".repeat(80));
    System.out.println("  Real LangChain4J integration working! 🎉");
    System.out.println("=".repeat(80));
  }
}
