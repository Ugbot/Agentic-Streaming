package org.jagentic.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Tools — named {@code params -> result} functions. Mirror of {@code ToolExecutor}.
 * A tool may carry an optional JSON-schema describing its input parameters; this is what
 * lets a {@code ToolRegistry} be exposed as an MCP server (its {@code tools/list} needs a
 * real {@code inputSchema}). The schema is optional and back-compatible — the original
 * 3-arg {@code register} still works and yields the permissive {@code {"type":"object"}}. */
public final class ToolRegistry {

  /** A single tool. {@code inputSchema} is a JSON-schema map for the parameters, or null. */
  public record Tool(String id, String description, Map<String, Object> inputSchema,
                     Function<Map<String, Object>, Object> fn) {
    /** Schema-less convenience constructor (back-compatible). */
    public Tool(String id, String description, Function<Map<String, Object>, Object> fn) {
      this(id, description, null, fn);
    }

    public Object execute(Map<String, Object> params) {
      return fn.apply(params == null ? Map.of() : params);
    }

    /** The input schema, or the permissive MCP default when none was declared. */
    public Map<String, Object> inputSchemaOrDefault() {
      return inputSchema == null ? Map.of("type", "object") : inputSchema;
    }
  }

  private final Map<String, Tool> tools = new ConcurrentHashMap<>();

  public ToolRegistry register(String id, String description, Function<Map<String, Object>, Object> fn) {
    tools.put(id, new Tool(id, description, fn));
    return this;
  }

  /** Register a tool with an explicit input JSON-schema (used for MCP {@code tools/list}). */
  public ToolRegistry register(String id, String description, Map<String, Object> inputSchema,
                               Function<Map<String, Object>, Object> fn) {
    tools.put(id, new Tool(id, description, inputSchema, fn));
    return this;
  }

  public Tool get(String id) {
    return tools.get(id);
  }

  public List<String> ids() {
    return List.copyOf(tools.keySet());
  }

  /** {@code [{name, description}]} — what an LLM brain shows the model so it can pick a
   * tool by name. */
  public List<Map<String, String>> specs() {
    return tools.values().stream()
        .map(t -> Map.of("name", t.id(), "description", t.description()))
        .toList();
  }

  /** {@code [{name, description, inputSchema}]} — the richer descriptor an MCP server
   * returns from {@code tools/list}. {@code inputSchema} is always present (defaulting to
   * {@code {"type":"object"}}). */
  public List<Map<String, Object>> toolDescriptors() {
    return tools.values().stream()
        .map(t -> {
          Map<String, Object> d = new LinkedHashMap<>();
          d.put("name", t.id());
          d.put("description", t.description());
          d.put("inputSchema", t.inputSchemaOrDefault());
          return d;
        })
        .toList();
  }

  public Object execute(String id, Map<String, Object> params) {
    Tool t = tools.get(id);
    if (t == null) {
      throw new IllegalArgumentException("no such tool: " + id);
    }
    return t.execute(params);
  }
}
