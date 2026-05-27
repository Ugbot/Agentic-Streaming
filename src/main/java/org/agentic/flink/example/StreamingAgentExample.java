package org.agentic.flink.example;

import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.stream.AgentFlatMapFunction;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Complete Streaming Agent Example - Real Flink Integration
 *
 * <p>This example demonstrates the complete streaming integration:
 * <ul>
 *   <li>Flink DataStream processing with agents</li>
 *   <li>Real LLM calls in a streaming context</li>
 *   <li>Real tool execution</li>
 *   <li>Event-driven agent workflows</li>
 * </ul>
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 * # Start Ollama
 * docker compose up -d ollama
 *
 * # Pull model
 * docker compose exec ollama ollama pull qwen2.5:3b
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.StreamingAgentExample"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class StreamingAgentExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=".repeat(80));
    System.out.println("  Streaming Agent Example - Real Flink Integration");
    System.out.println("=".repeat(80));
    System.out.println();

    // ==================== Step 1: Setup Flink Environment ====================

    System.out.println("🔧 Step 1: Setting up Flink environment...\n");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    System.out.println("✅ Flink environment configured");
    System.out.println();

    // ==================== Step 2: Define Agent ====================

    System.out.println("📝 Step 2: Defining calculator agent...\n");

    Agent calculatorAgent = Agent.builder()
        .withId("calculator-agent")
        .withName("Calculator Agent")
        .withType(AgentType.EXECUTOR)
        .withSystemPrompt(
            "You are a calculator agent. When asked to perform calculations:\n" +
            "1. Use TOOL_CALL: calculator-add {\"a\": X, \"b\": Y} to add numbers\n" +
            "2. Use TOOL_CALL: calculator-multiply {\"a\": X, \"b\": Y} to multiply\n" +
            "3. Show your work step by step\n" +
            "4. Provide the final answer clearly")
        .withChatSetup(ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.3).build())
        .withMaxIterations(5)
        .build();

    System.out.println("✅ Agent created: " + calculatorAgent.getAgentName());
    System.out.println();

    // ==================== Step 3: Setup Tool Registry ====================

    System.out.println("🔧 Step 3: Setting up tools...\n");

    ToolRegistry toolRegistry = ToolRegistry.builder()
        .registerTool("calculator-add", new SimpleCalculatorTool("add"))
        .registerTool("calculator-multiply", new SimpleCalculatorTool("multiply"))
        .build();

    System.out.println("✅ Tools registered: " + toolRegistry.getToolNames());
    System.out.println();

    // ==================== Step 4: Create LLM Client ====================

    System.out.println("🔗 Step 4: Creating LLM client...\n");

    LLMClient llmClient = LLMClient.builder()
        .withModel("qwen2.5:3b")
        .withTemperature(0.3)
        .withBaseUrl(ConfigKeys.DEFAULT_OLLAMA_BASE_URL)
        .build();

    System.out.println("✅ LLM client created");
    System.out.println();

    // ==================== Step 5: Create Input Stream ====================

    System.out.println("📊 Step 5: Creating input event stream...\n");

    DataStream<AgentEvent> inputStream = env.fromElements(
        createCalcRequest("flow-001", "calculator-agent", "Calculate 5 + 3"),
        createCalcRequest("flow-002", "calculator-agent", "What is 10 * 4?"),
        createCalcRequest("flow-003", "calculator-agent", "Compute (7 + 3) * 2")
    );

    System.out.println("✅ Input stream created with 3 calculation requests");
    System.out.println();

    // ==================== Step 6: Apply Agent Function ====================

    System.out.println("⚙️  Step 6: Wiring agent execution into stream...\n");

    DataStream<AgentEvent> resultStream = inputStream
        .flatMap(new AgentFlatMapFunction(calculatorAgent, toolRegistry, llmClient))
        .name("Agent Execution");

    System.out.println("✅ Agent function applied to stream");
    System.out.println();

    // ==================== Step 7: Print Results ====================

    System.out.println("📤 Step 7: Setting up result sink...\n");

    resultStream
        .map(event -> {
          String status = event.getEventType() == AgentEventType.FLOW_COMPLETED ? "✅" : "❌";
          String result = event.getData("result") != null ? event.getData("result").toString() : "N/A";
          return String.format("%s Flow %s: %s",
              status, event.getFlowId(), result);
        })
        .print();

    System.out.println("✅ Result sink configured");
    System.out.println();

    // ==================== Step 8: Execute ====================

    System.out.println("🚀 Step 8: Executing Flink job...\n");
    System.out.println("=".repeat(80));
    System.out.println();

    env.execute("Streaming Agent Example");

    System.out.println();
    System.out.println("=".repeat(80));
    System.out.println("  Streaming integration complete! 🎉");
    System.out.println("  - Flink DataStream ✓");
    System.out.println("  - Real LLM execution ✓");
    System.out.println("  - Real tool execution ✓");
    System.out.println("  - Event-driven workflow ✓");
    System.out.println("=".repeat(80));
  }

  private static AgentEvent createCalcRequest(String flowId, String agentId, String question) {
    AgentEvent event = new AgentEvent(
        flowId,
        "user-001",
        agentId,
        AgentEventType.FLOW_STARTED
    );
    event.putData("user_message", question);
    event.setTimestamp(System.currentTimeMillis());
    return event;
  }
}
