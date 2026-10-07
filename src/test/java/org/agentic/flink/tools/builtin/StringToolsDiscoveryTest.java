package org.agentic.flink.tools.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.langchain.LangChainToolAdapter;
import org.agentic.flink.langchain.ToolAnnotationRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@link StringTools} is reached only through classpath scanning: {@link ToolAnnotationRegistry}
 * finds its {@code @Tool} methods and {@link LangChainToolAdapter} invokes them by name.
 */
class StringToolsDiscoveryTest {

  private static ToolAnnotationRegistry registry;

  @BeforeAll
  static void scan() {
    registry = new ToolAnnotationRegistry(StringTools.class.getPackageName());
  }

  @Test
  void scanningTheBuiltinPackageRegistersEveryStringTool() {
    for (String method :
        new String[] {
          "touppercase",
          "tolowercase",
          "length",
          "concat",
          "contains",
          "replace",
          "trim",
          "substring",
          "split",
          "startswith",
          "endswith"
        }) {
      assertTrue(registry.hasTool("string_" + method), "string_" + method);
    }
    assertInstanceOf(StringTools.class, registry.getToolInstance("string_length"));
    assertEquals(
        "Concatenates two strings", registry.getToolDefinition("string_concat").getDescription());
  }

  @Test
  void discoveredStringToolsExecuteThroughTheAdapter() throws Exception {
    String text = "text-" + UUID.randomUUID();
    String other = "other-" + UUID.randomUUID();

    Object upper =
        new LangChainToolAdapter("string_touppercase", registry)
            .execute(Map.of("text", text))
            .get(10, TimeUnit.SECONDS);
    Object length =
        new LangChainToolAdapter("string_length", registry)
            .execute(Map.of("text", text))
            .get(10, TimeUnit.SECONDS);
    Object joined =
        new LangChainToolAdapter("string_concat", registry)
            .execute(Map.of("first", text, "second", other))
            .get(10, TimeUnit.SECONDS);
    int from = ThreadLocalRandom.current().nextInt(0, text.length() / 2);
    int to = ThreadLocalRandom.current().nextInt(from, text.length());
    Object sub =
        new LangChainToolAdapter("string_substring", registry)
            .execute(Map.of("text", text, "startIndex", from, "endIndex", to))
            .get(10, TimeUnit.SECONDS);

    assertEquals(text.toUpperCase(), upper);
    assertEquals(text.length(), length);
    assertEquals(text + other, joined);
    assertEquals(text.substring(from, to), sub);
  }
}
