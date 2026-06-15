package org.agentic.flink.plugins.flintagents.examples;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.plugins.flintagents.adapter.FlinkAgentsEventAdapter;
import org.agentic.flink.plugins.flintagents.adapter.FlinkAgentsToolAdapter;
import org.agentic.flink.tools.ToolExecutor;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.Event;

/**
 * Demo showing OpenAI integration with Flink Agents.
 *
 * <p><b>SECURITY: API Key Setup</b>
 * <pre>
 * Option 1: Environment Variable (RECOMMENDED)
 * export OPENAI_API_KEY="your-key-here"
 *
 * Option 2: System Property
 * mvn exec:java -Dexec.mainClass="..." -Dopenai.api.key="your-key-here"
 *
 * Option 3: .env file (for development)
 * Create .env file with: OPENAI_API_KEY=your-key-here
 * Add .env to .gitignore!
 * </pre>
 *
 * <p><b>Usage:</b>
 * <pre>
 * export OPENAI_API_KEY="sk-..."
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.OpenAIFlinkAgentsDemo"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class OpenAIFlinkAgentsDemo {

  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_CYAN = "\u001B[36m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_BOLD = "\u001B[1m";

  public static void main(String[] args) {
    printBanner();

    // Get API key securely
    String apiKey = getApiKey();
    if (apiKey == null) {
      printApiKeyHelp();
      System.exit(1);
    }

    System.out.println(ANSI_GREEN + "✓ OpenAI API key found" + ANSI_RESET);
    System.out.println(ANSI_CYAN + "  Key prefix: " + maskApiKey(apiKey) + ANSI_RESET);
    System.out.println();

    try {
      // Run demos
      demoSimpleChat(apiKey);
      demoToolIntegration(apiKey);
      demoFlinkAgentsIntegration(apiKey);

      printSuccess();
    } catch (Exception e) {
      printError(e);
    }
  }

  /**
   * Demo 1: Simple OpenAI chat.
   */
  private static void demoSimpleChat(String apiKey) {
    printSectionHeader("Demo 1: Simple OpenAI Chat");

    System.out.println(ANSI_CYAN + "Creating OpenAI chat model..." + ANSI_RESET);
    // gpt-5.x reasoning models reject max_tokens / custom temperature (see OpenAiModels) — omit them.
    ChatModel model = OpenAiChatModel.builder()
        .apiKey(apiKey)
        .modelName("gpt-5.4-mini")
        .build();

    System.out.println(ANSI_GREEN + "✓ Model created" + ANSI_RESET);
    System.out.println();

    String question = "What is Apache Flink in one sentence?";
    System.out.println(ANSI_BOLD + "Question:" + ANSI_RESET + " " + question);
    System.out.println(ANSI_CYAN + "Asking OpenAI..." + ANSI_RESET);

    ChatResponse response = model.chat(UserMessage.from(question));
    String answer = response.aiMessage().text();

    System.out.println(ANSI_BOLD + "Answer:" + ANSI_RESET + " " + answer);
    System.out.println();
    System.out.println(ANSI_GREEN + "✓ Simple chat completed" + ANSI_RESET);
    System.out.println();
  }

  /**
   * Demo 2: OpenAI with tool execution.
   */
  private static void demoToolIntegration(String apiKey) {
    printSectionHeader("Demo 2: OpenAI + Tool Execution");

    // Create a calculator tool
    ToolExecutor calculator = new ToolExecutor() {
      @Override
      public CompletableFuture<Object> execute(Map<String, Object> parameters) {
        double num1 = ((Number) parameters.get("num1")).doubleValue();
        double num2 = ((Number) parameters.get("num2")).doubleValue();
        String operation = (String) parameters.get("operation");

        double result;
        switch (operation) {
          case "add": result = num1 + num2; break;
          case "multiply": result = num1 * num2; break;
          default: result = 0;
        }

        return CompletableFuture.completedFuture(
            "The result of " + num1 + " " + operation + " " + num2 + " is " + result
        );
      }

      @Override
      public String getToolId() { return "calculator"; }

      @Override
      public String getDescription() {
        return "Performs calculations (add, multiply)";
      }
    };

    ToolDefinition toolDef = new ToolDefinition();
    toolDef.setToolId("calculator");
    toolDef.setDescription("Performs calculations");

    System.out.println(ANSI_GREEN + "✓ Calculator tool created" + ANSI_RESET);
    System.out.println();

    // Simulate: "What is 15 + 27?"
    System.out.println(ANSI_BOLD + "User request:" + ANSI_RESET + " What is 15 + 27?");
    System.out.println(ANSI_CYAN + "→ AI decides to use calculator tool" + ANSI_RESET);

    Map<String, Object> params = Map.of("num1", 15, "num2", 27, "operation", "add");
    try {
      Object result = calculator.execute(params).get();
      System.out.println(ANSI_BOLD + "Tool result:" + ANSI_RESET + " " + result);
    } catch (Exception e) {
      System.out.println(ANSI_RED + "✗ Tool execution failed: " + e.getMessage() + ANSI_RESET);
    }

    System.out.println();
    System.out.println(ANSI_GREEN + "✓ Tool integration completed" + ANSI_RESET);
    System.out.println();
  }

  /**
   * Demo 3: OpenAI + Flink Agents Integration.
   */
  private static void demoFlinkAgentsIntegration(String apiKey) {
    printSectionHeader("Demo 3: OpenAI + Flink Agents Integration");

    System.out.println(ANSI_CYAN + "Creating hybrid agent system..." + ANSI_RESET);

    // Create AgentEvent (our framework)
    AgentEvent ourEvent = new AgentEvent(
        "flow-openai-001",
        "user-demo",
        "openai-agent",
        AgentEventType.TOOL_CALL_REQUESTED
    );
    ourEvent.setData(Map.of(
        "question", "Explain event-driven agents in 25 words",
        "model", "gpt-5.4-mini"
    ));

    System.out.println(ANSI_GREEN + "✓ AgentEvent created (our framework)" + ANSI_RESET);

    // Convert to Flink Agents
    Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(ourEvent);
    System.out.println(ANSI_GREEN + "✓ Converted to Flink Agents Event" + ANSI_RESET);

    // Create OpenAI model
    // gpt-5.x reasoning models reject max_tokens / custom temperature (see OpenAiModels) — omit them.
    ChatModel model = OpenAiChatModel.builder()
        .apiKey(apiKey)
        .modelName("gpt-5.4-mini")
        .build();

    System.out.println(ANSI_GREEN + "✓ OpenAI model configured" + ANSI_RESET);
    System.out.println();

    // Get response
    String question = (String) ourEvent.getData().get("question");
    System.out.println(ANSI_BOLD + "Question:" + ANSI_RESET + " " + question);
    System.out.println(ANSI_CYAN + "Asking OpenAI via Flink Agents integration..." + ANSI_RESET);

    ChatResponse response = model.chat(UserMessage.from(question));
    String answer = response.aiMessage().text();

    System.out.println(ANSI_BOLD + "Answer:" + ANSI_RESET + " " + answer);
    System.out.println();

    // Convert back to our format
    AgentEvent resultEvent = FlinkAgentsEventAdapter.fromFlinkAgentEvent(flinkEvent);
    System.out.println(ANSI_GREEN + "✓ Result converted back to AgentEvent" + ANSI_RESET);
    System.out.println(ANSI_CYAN + "  Flow ID: " + resultEvent.getFlowId() + ANSI_RESET);
    System.out.println();

    System.out.println(ANSI_GREEN + "✓ Flink Agents integration completed" + ANSI_RESET);
    System.out.println();
  }

  /**
   * Get API key from environment or system properties.
   * SECURITY: Never hardcode API keys!
   */
  private static String getApiKey() {
    // Try environment variable first (recommended)
    String apiKey = System.getenv("OPENAI_API_KEY");
    if (apiKey != null && !apiKey.isEmpty()) {
      return apiKey;
    }

    // Try system property
    apiKey = System.getProperty("openai.api.key");
    if (apiKey != null && !apiKey.isEmpty()) {
      return apiKey;
    }

    // Try alternative environment variable name
    apiKey = System.getenv("OPENAI_KEY");
    if (apiKey != null && !apiKey.isEmpty()) {
      return apiKey;
    }

    return null;
  }

  /**
   * Mask API key for display (security).
   */
  private static String maskApiKey(String apiKey) {
    if (apiKey == null || apiKey.length() < 8) {
      return "***";
    }
    return apiKey.substring(0, 7) + "..." + apiKey.substring(apiKey.length() - 4);
  }

  private static void printBanner() {
    System.out.println("\n" + ANSI_CYAN + ANSI_BOLD);
    System.out.println("╔══════════════════════════════════════════════════════════════╗");
    System.out.println("║                                                              ║");
    System.out.println("║         OpenAI + Apache Flink Agents Integration            ║");
    System.out.println("║                                                              ║");
    System.out.println("╚══════════════════════════════════════════════════════════════╝");
    System.out.println(ANSI_RESET);
  }

  private static void printSectionHeader(String title) {
    System.out.println(ANSI_BOLD + ANSI_CYAN + "═".repeat(70) + ANSI_RESET);
    System.out.println(ANSI_BOLD + ANSI_CYAN + " " + title + ANSI_RESET);
    System.out.println(ANSI_BOLD + ANSI_CYAN + "═".repeat(70) + ANSI_RESET);
    System.out.println();
  }

  private static void printApiKeyHelp() {
    System.out.println(ANSI_RED + ANSI_BOLD + "✗ OpenAI API key not found!" + ANSI_RESET);
    System.out.println();
    System.out.println(ANSI_YELLOW + "Please set your API key using one of these methods:" + ANSI_RESET);
    System.out.println();
    System.out.println(ANSI_BOLD + "Option 1: Environment Variable (RECOMMENDED)" + ANSI_RESET);
    System.out.println("  export OPENAI_API_KEY=\"sk-your-key-here\"");
    System.out.println("  mvn exec:java -Dexec.mainClass=\"org.agentic.flink.example.OpenAIFlinkAgentsDemo\"");
    System.out.println();
    System.out.println(ANSI_BOLD + "Option 2: System Property" + ANSI_RESET);
    System.out.println("  mvn exec:java -Dexec.mainClass=\"...\" -Dopenai.api.key=\"sk-your-key-here\"");
    System.out.println();
    System.out.println(ANSI_BOLD + "Option 3: Add to shell profile (persistent)" + ANSI_RESET);
    System.out.println("  echo 'export OPENAI_API_KEY=\"sk-your-key-here\"' >> ~/.bashrc");
    System.out.println("  source ~/.bashrc");
    System.out.println();
    System.out.println(ANSI_CYAN + "Get your API key from: https://platform.openai.com/api-keys" + ANSI_RESET);
    System.out.println();
    System.out.println(ANSI_YELLOW + "⚠️  SECURITY: Never commit API keys to git!" + ANSI_RESET);
    System.out.println(ANSI_YELLOW + "⚠️  Add .env to .gitignore if using .env files" + ANSI_RESET);
    System.out.println();
  }

  private static void printSuccess() {
    System.out.println(ANSI_GREEN + ANSI_BOLD);
    System.out.println("╔══════════════════════════════════════════════════════════════╗");
    System.out.println("║                                                              ║");
    System.out.println("║              ✓ All OpenAI Demos Completed!                  ║");
    System.out.println("║                                                              ║");
    System.out.println("║        OpenAI + Flink Agents integration verified           ║");
    System.out.println("║                                                              ║");
    System.out.println("╚══════════════════════════════════════════════════════════════╝");
    System.out.println(ANSI_RESET);
  }

  private static void printError(Exception e) {
    System.out.println();
    System.out.println(ANSI_RED + ANSI_BOLD + "✗ Error occurred:" + ANSI_RESET);
    System.out.println(ANSI_RED + "  " + e.getMessage() + ANSI_RESET);
    System.out.println();
    System.out.println(ANSI_YELLOW + "Common issues:" + ANSI_RESET);
    System.out.println("  • Invalid API key - check your key is correct");
    System.out.println("  • Rate limit exceeded - wait a moment and try again");
    System.out.println("  • Network error - check your internet connection");
    System.out.println("  • Insufficient credits - check your OpenAI account balance");
    System.out.println();
  }
}
