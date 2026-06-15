package org.jagentic.tools.util;

import java.util.ArrayList;
import java.util.List;

import org.jagentic.core.ToolRegistry;
import org.jagentic.tools.AnnotatedToolBinder;
import org.jagentic.tools.ToolPack;

/** Utility pack — calculator + string operations, exposed from {@code @Tool}-annotated
 * methods via {@link AnnotatedToolBinder}. The smallest reference pack: it proves the
 * {@code @Tool} → schema → MCP path end-to-end. Tools are id-prefixed {@code util_}. */
public final class UtilityPack implements ToolPack {

  @Override
  public String name() {
    return "util";
  }

  @Override
  public List<String> register(ToolRegistry registry) {
    List<String> ids = new ArrayList<>();
    ids.addAll(AnnotatedToolBinder.bind(registry, new CalculatorTools(), "util_"));
    ids.addAll(AnnotatedToolBinder.bind(registry, new StringTools(), "util_"));
    return ids;
  }
}
