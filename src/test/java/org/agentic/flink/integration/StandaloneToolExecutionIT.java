package org.agentic.flink.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.example.SimpleCalculatorTool;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.execution.LLMResponse;
import org.agentic.flink.execution.ToolCall;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for LLM + Tool Execution.
 *
 * <p>Tests the complete agentic loop: LLM -> Tool Call -> Execution.
 * Requires a running Ollama instance with qwen2.5:latest.
 */
@Tag("integration")
@Disabled("Requires running Ollama instance with qwen2.5:latest")
class StandaloneToolExecutionIT {

  private LLMClient llmClient;
  private ToolRegistry toolRegistry;

  @BeforeEach
  void setUp() {
    llmClient = LLMClient.builder()
        .withModel("qwen2.5:latest")
        .withTemperature(0.3)
        .withBaseUrl("http://localhost:11434")
        .build();

    toolRegistry = ToolRegistry.builder()
        .registerTool("calculator-add", new SimpleCalculatorTool("add"))
        .registerTool("calculator-multiply", new SimpleCalculatorTool("multiply"))
        .build();
  }

  @Test
  void multiStepCalculationShouldProduceToolCalls() throws Exception {
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(Map.of("role", "system", "content",
        "You are a calculator assistant. You have these tools:\n"
            + "- calculator-add: Add two numbers using {\"a\": X, \"b\": Y}\n"
            + "- calculator-multiply: Multiply two numbers using {\"a\": X, \"b\": Y}\n\n"
            + "When you need to calculate, use:\n"
            + "TOOL_CALL: calculator-add {\"a\": 5, \"b\": 3}\n\n"
            + "Show your reasoning."));

    messages.add(Map.of("role", "user", "content",
        "Calculate: (5 + 3) * 2\nDo this step by step using the tools."));

    LLMResponse response1 = llmClient.chat(messages);
    assertNotNull(response1.getText(), "LLM should produce a text response");
    assertTrue(response1.hasToolCalls(), "LLM should request a tool call for addition");

    ToolCall toolCall = response1.getToolCalls().get(0);
    assertNotNull(toolCall.getToolName(), "Tool call should have a name");

    ToolExecutor executor = toolRegistry.getExecutor(toolCall.getToolName()).get();
    Object result = executor.execute(toolCall.getParameters()).get();
    assertNotNull(result, "Tool execution should produce a result");

    messages.add(Map.of("role", "assistant", "content", response1.getText()));
    messages.add(Map.of("role", "tool", "content", "Result: " + result));

    LLMResponse response2 = llmClient.chat(messages);
    assertNotNull(response2.getText(), "LLM should continue after tool result");
  }
}
