package org.agentic.flink.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Tests for tool call parsing from LLM responses.
 *
 * <p>Verifies that tool call patterns are correctly extracted from various LLM output formats.
 */
class ToolCallParsingTest {

  private static final Pattern JSON_TOOL_CALL_PATTERN = Pattern.compile(
      "TOOL_CALL:\\s*([a-zA-Z0-9_-]+)\\s*\\{([^}]+)\\}");

  private static final Pattern FUNCTION_TOOL_CALL_PATTERN = Pattern.compile(
      "TOOL_CALL:\\s*([a-zA-Z0-9_-]+)\\s*\\(([^)]+)\\)");

  @Test
  void shouldParseJsonFormatToolCall() {
    String llmText = "I need to add two numbers.\n\n"
        + "TOOL_CALL: calculator-add {\"a\": 5, \"b\": 3}\n\n"
        + "Let me call the calculator tool.";

    Matcher matcher = JSON_TOOL_CALL_PATTERN.matcher(llmText);
    assertTrue(matcher.find(), "Should detect JSON-format tool call");
    assertEquals("calculator-add", matcher.group(1));
    assertTrue(matcher.group(2).contains("\"a\": 5"));
    assertTrue(matcher.group(2).contains("\"b\": 3"));
  }

  @Test
  void shouldParseFunctionCallFormat() {
    String llmText = "To multiply these numbers:\n\n"
        + "TOOL_CALL: calculator-multiply(a=10, b=5)\n\n"
        + "This will give us the result.";

    Matcher matcher = FUNCTION_TOOL_CALL_PATTERN.matcher(llmText);
    assertTrue(matcher.find(), "Should detect function-call format");
    assertEquals("calculator-multiply", matcher.group(1));
    assertTrue(matcher.group(2).contains("a=10"));
    assertTrue(matcher.group(2).contains("b=5"));
  }

  @Test
  void shouldParseMultipleToolCallsInOneResponse() {
    String llmText = "I'll solve this step by step:\n\n"
        + "First, let me add 5 and 3:\n"
        + "TOOL_CALL: calculator-add {\"a\": 5, \"b\": 3}\n\n"
        + "Then, multiply the result by 2:\n"
        + "TOOL_CALL: calculator-multiply {\"a\": 8, \"b\": 2}\n\n"
        + "That's how we solve (5 + 3) * 2.";

    Matcher matcher = JSON_TOOL_CALL_PATTERN.matcher(llmText);
    List<String> toolNames = new ArrayList<>();
    while (matcher.find()) {
      toolNames.add(matcher.group(1));
    }

    assertEquals(2, toolNames.size(), "Should detect two tool calls");
    assertEquals("calculator-add", toolNames.get(0));
    assertEquals("calculator-multiply", toolNames.get(1));
  }

  @Test
  void shouldNotMatchWhenNoToolCallPresent() {
    String llmText = "The answer to 5 plus 3 is 8. No tools needed.";

    Matcher jsonMatcher = JSON_TOOL_CALL_PATTERN.matcher(llmText);
    Matcher funcMatcher = FUNCTION_TOOL_CALL_PATTERN.matcher(llmText);

    assertFalse(jsonMatcher.find(), "Should not detect JSON tool call in plain text");
    assertFalse(funcMatcher.find(), "Should not detect function tool call in plain text");
  }
}
