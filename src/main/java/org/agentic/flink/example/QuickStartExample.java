package org.agentic.flink.example;

import java.io.PrintStream;
import org.agentic.flink.annotation.Internal;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.tool.ToolRegistry;

/**
 * Quick Start Example - Real LangChain4J Integration
 *
 * <p>This example demonstrates the REAL working integration:
 *
 * <ul>
 *   <li>Real LangChain4J Ollama calls
 *   <li>Declarative agent definition
 *   <li>Actual LLM-based execution
 *   <li>No Flink - just the agent engine
 * </ul>
 *
 * <p><b>Prerequisites:</b>
 *
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
 *
 * <pre>
 * ./mvnw -q test-compile exec:java -Dexec.mainClass=org.agentic.flink.example.QuickStartExample \
 *     -Dexec.classpathScope=test
 * </pre>
 *
 * <p>The process exits non-zero when the model call fails.
 *
 * @author Agentic Flink Team
 */
@Internal
public class QuickStartExample {

  static final String SUCCESS_BANNER = "Real LangChain4J integration working!";

  private static final String MODEL = "qwen2.5:3b";
  private static final String QUESTION = "What is Apache Flink and why is it useful for AI?";

  public static void main(String[] args) throws Exception {
    System.exit(run(ConfigKeys.DEFAULT_OLLAMA_BASE_URL, System.out));
  }

  /** Runs the example against the Ollama at {@code baseUrl}. Returns the process exit code. */
  static int run(String baseUrl, PrintStream out) throws Exception {
    out.println("=".repeat(80));
    out.println("  Quick Start - Real LangChain4J Integration");
    out.println("=".repeat(80));
    out.println();

    Agent agent =
        Agent.builder()
            .withId("assistant")
            .withName("Helpful Assistant")
            .withType(AgentType.EXECUTOR)
            .withSystemPrompt(
                "You are a helpful AI assistant. Answer questions clearly and concisely. "
                    + "If you don't know something, say so.")
            .withChatSetup(ChatSetup.builder().withModel(MODEL).withTemperature(0.7).build())
            .withMaxIterations(3)
            .build();
    out.println("Agent: " + agent.getAgentName() + ", model " + agent.getLlmModel());

    LLMClient llmClient =
        LLMClient.builder().withModel(MODEL).withTemperature(0.7).withBaseUrl(baseUrl).build();
    out.println("Ollama at " + baseUrl);
    out.println();

    AgentEvent inputEvent =
        new AgentEvent("flow-001", "user-001", "assistant", AgentEventType.FLOW_STARTED);
    inputEvent.putData("user_message", QUESTION);
    out.println("Question: " + QUESTION);
    out.println();

    ExecutionResult result;
    try (AgentExecutor executor =
        AgentExecutor.builder()
            .withAgent(agent)
            .withToolRegistry(ToolRegistry.empty())
            .withLlmClient(llmClient)
            .build()) {
      result = executor.execute(inputEvent).get();
    }

    if (!result.isSuccess()) {
      out.println("FAILED: " + result.getErrorMessage());
      return 1;
    }
    out.println("Response:");
    out.println(result.getOutput());
    out.println();
    out.println("Events generated: " + result.getEvents().size());
    out.println(
        "Tool calls: " + (result.getToolCalls() == null ? 0 : result.getToolCalls().size()));
    out.println();
    out.println("=".repeat(80));
    out.println("  " + SUCCESS_BANNER);
    out.println("=".repeat(80));
    return 0;
  }
}
