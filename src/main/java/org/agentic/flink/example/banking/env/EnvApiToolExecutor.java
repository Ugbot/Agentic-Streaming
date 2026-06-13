package org.agentic.flink.example.banking.env;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes a single A2A-harness environment tool as a framework {@link ToolExecutor}.
 *
 * <p>The harness grants each agent a scoped set of env tools (the user's banking actions for the
 * personal agent; the bank's operations for the CS agent). This executor calls one of them via
 * {@link EnvApiClient}, keyed by the turn's A2A {@code contextId} read from {@link EnvSession} — so
 * the session id in the URL path is always the real one, satisfying the harness's contextId
 * discipline.
 *
 * <p>Use a per-tool instance ({@link #EnvApiToolExecutor(EnvApiClient, String, String)}) for each
 * fetched tool schema, plus one {@link #fallback(EnvApiClient)} for the generic {@code call_env_tool}
 * escape hatch (covers tools granted mid-conversation that aren't in the agent's list yet).
 */
public final class EnvApiToolExecutor implements ToolExecutor {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(EnvApiToolExecutor.class);

  /** Generic fallback tool name (mirrors the template's call_env_tool). */
  public static final String FALLBACK_TOOL = "call_env_tool";

  private final EnvApiClient client;
  private final String toolName; // null => generic fallback
  private final String description;

  private transient volatile ObjectMapper mapper;

  public EnvApiToolExecutor(EnvApiClient client, String toolName, String description) {
    this.client = java.util.Objects.requireNonNull(client, "client");
    this.toolName = toolName;
    this.description = description == null ? "" : description;
  }

  /** The generic {@code call_env_tool(tool_name, arguments_json)} fallback executor. */
  public static EnvApiToolExecutor fallback(EnvApiClient client) {
    return new EnvApiToolExecutor(
        client,
        null,
        "Call any environment tool by name. arguments_json is a JSON object string, e.g."
            + " '{\"user_id\": \"abc123\"}'. Use for tools not in your tool list yet.");
  }

  @Override
  public String getToolId() {
    return toolName != null ? toolName : FALLBACK_TOOL;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    String contextId = EnvSession.contextId();
    if (contextId == null || contextId.isBlank()) {
      // A wiring bug — never fabricate a session id; fail loudly with a tool error.
      return CompletableFuture.completedFuture(
          error("No A2A contextId bound for this turn; cannot call env tool " + getToolId()));
    }

    String name;
    Map<String, Object> arguments;
    if (toolName != null) {
      name = toolName;
      arguments = parameters == null ? Map.of() : parameters;
    } else {
      // Generic fallback: {tool_name, arguments_json}.
      Object tn = parameters == null ? null : parameters.get("tool_name");
      if (!(tn instanceof String) || ((String) tn).isBlank()) {
        return CompletableFuture.completedFuture(error("call_env_tool requires a 'tool_name'"));
      }
      name = (String) tn;
      arguments = parseArgs(parameters.get("arguments_json"));
    }

    // Run synchronously on the calling (operator) thread so the EnvSession ThreadLocal is valid;
    // env calls are quick and bounded by the client timeout + the turn's routing budget.
    Map<String, Object> result = client.callTool(contextId, name, arguments);
    LOG.debug("env tool {} (ctx {}) -> error={}", name, contextId, result.get("error"));
    return CompletableFuture.completedFuture(result);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> parseArgs(Object argumentsJson) {
    if (argumentsJson == null) {
      return Map.of();
    }
    if (argumentsJson instanceof Map) {
      return (Map<String, Object>) argumentsJson;
    }
    String s = argumentsJson.toString();
    if (s.isBlank()) {
      return Map.of();
    }
    try {
      return mapper().readValue(s, Map.class);
    } catch (Exception e) {
      return Map.of();
    }
  }

  private Map<String, Object> error(String message) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("error", true);
    m.put("content", message);
    return m;
  }

  private ObjectMapper mapper() {
    ObjectMapper m = mapper;
    if (m == null) {
      synchronized (this) {
        if (mapper == null) {
          mapper = new ObjectMapper();
        }
        m = mapper;
      }
    }
    return m;
  }
}
