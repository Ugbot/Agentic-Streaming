package org.agentic.flink.example.banking.env;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.agentic.flink.tools.ToolExecutor;

/**
 * Lets the agent discover its session-scoped environment tools at runtime:
 * {@code list_env_tools()} returns the OpenAI-style schemas the harness granted for the current
 * A2A {@code contextId}. Paired with {@link EnvApiToolExecutor#fallback} ({@code call_env_tool}),
 * this exposes the dynamic, per-session env toolset to the LLM through two stable tools — no need
 * to pre-register every env tool on the agent.
 */
public final class ListEnvToolsExecutor implements ToolExecutor {
  private static final long serialVersionUID = 1L;

  private final EnvApiClient client;

  public ListEnvToolsExecutor(EnvApiClient client) {
    this.client = java.util.Objects.requireNonNull(client, "client");
  }

  @Override
  public String getToolId() {
    return "list_env_tools";
  }

  @Override
  public String getDescription() {
    return "List the environment tools available to you this session (name, description, parameters)."
        + " Call this first to discover what actions you can take, then use call_env_tool.";
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    String contextId = EnvSession.contextId();
    if (contextId == null || contextId.isBlank()) {
      Map<String, Object> err = new LinkedHashMap<>();
      err.put("error", true);
      err.put("content", "No session context; cannot list env tools.");
      return CompletableFuture.completedFuture(err);
    }
    List<Map<String, Object>> tools = client.listTools(contextId);
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("error", false);
    result.put("tools", tools);
    return CompletableFuture.completedFuture(result);
  }
}
