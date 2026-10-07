package org.agentic.flink.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/**
 * The text tool-call protocol as implemented by {@link LLMClient#parseToolCallsFromText}: {@code
 * TOOL_CALL: name {json}} and {@code TOOL_CALL: name(k=v, ...)} embedded in prose. Every case goes
 * through the production parser, not a copy of its patterns.
 */
@SuppressWarnings("deprecation")
class ToolCallParsingTest {

  @Test
  void shouldParseJsonFormatToolCall() {
    int a = ThreadLocalRandom.current().nextInt(1, 1_000);
    int b = ThreadLocalRandom.current().nextInt(1, 1_000);
    String llmText =
        "I need to add two numbers.\n\n"
            + "TOOL_CALL: calculator-add {\"a\": "
            + a
            + ", \"b\": "
            + b
            + "}\n\n"
            + "Let me call the calculator tool.";

    List<ToolCall> calls = LLMClient.parseToolCallsFromText(llmText);

    assertEquals(1, calls.size(), calls.toString());
    assertEquals("calculator-add", calls.get(0).getToolName());
    assertEquals("call_0", calls.get(0).getToolCallId());
    assertEquals(Map.of("a", a, "b", b), calls.get(0).getParameters());
  }

  @Test
  void shouldParseFunctionCallFormat() {
    int a = ThreadLocalRandom.current().nextInt(1, 1_000);
    int b = ThreadLocalRandom.current().nextInt(1, 1_000);
    String llmText =
        "To multiply these numbers:\n\n"
            + "TOOL_CALL: calculator-multiply(a="
            + a
            + ", b="
            + b
            + ")\n\n"
            + "This will give us the result.";

    List<ToolCall> calls = LLMClient.parseToolCallsFromText(llmText);

    assertEquals(1, calls.size(), calls.toString());
    assertEquals("calculator-multiply", calls.get(0).getToolName());
    assertEquals(Map.of("a", a, "b", b), calls.get(0).getParameters());
  }

  @Test
  void shouldParseMultipleToolCallsInOneResponse() {
    int a = ThreadLocalRandom.current().nextInt(1, 1_000);
    int b = ThreadLocalRandom.current().nextInt(1, 1_000);
    String llmText =
        "I'll solve this step by step:\n\n"
            + "First, let me add the numbers:\n"
            + "TOOL_CALL: calculator-add {\"a\": "
            + a
            + ", \"b\": "
            + b
            + "}\n\n"
            + "Then, multiply the result by 2:\n"
            + "TOOL_CALL: calculator-multiply {\"a\": "
            + (a + b)
            + ", \"b\": 2}\n\n"
            + "That's how we solve it.";

    List<ToolCall> calls = LLMClient.parseToolCallsFromText(llmText);

    assertEquals(2, calls.size(), calls.toString());
    assertEquals("calculator-add", calls.get(0).getToolName());
    assertEquals("call_0", calls.get(0).getToolCallId());
    assertEquals(Map.of("a", a, "b", b), calls.get(0).getParameters());
    assertEquals("calculator-multiply", calls.get(1).getToolName());
    assertEquals("call_1", calls.get(1).getToolCallId());
    assertEquals(Map.of("a", a + b, "b", 2), calls.get(1).getParameters());
  }

  @Test
  void jsonFormTakesPrecedenceOverFunctionFormInTheSameResponse() {
    String llmText = "TOOL_CALL: first {\"x\": 1}\nTOOL_CALL: second(y=2)";

    List<ToolCall> calls = LLMClient.parseToolCallsFromText(llmText);

    assertEquals(1, calls.size(), calls.toString());
    assertEquals("first", calls.get(0).getToolName());
  }

  @Test
  void shouldNotMatchWhenNoToolCallPresent() {
    int sum = ThreadLocalRandom.current().nextInt(1, 1_000);
    String llmText = "The answer is " + sum + ". No tools needed. TOOL_CALL is just a word here.";

    assertTrue(LLMClient.parseToolCallsFromText(llmText).isEmpty());
    assertTrue(LLMClient.parseToolCallsFromText("").isEmpty());
    assertTrue(LLMClient.parseToolCallsFromText(null).isEmpty());
  }
}
