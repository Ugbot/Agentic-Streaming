package org.agentic.flink.python;

import org.agentic.flink.tools.ToolExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ToolExecutor} that delegates to a cloudpickled Python callable running inside the
 * embedded {@link PythonExecutor}. The Python function is registered lazily on first invocation
 * (so the operator's {@code open()} doesn't pay deserialization cost up-front for tools that may
 * never be called).
 *
 * <p>Argument binding: when {@code paramNames} is non-empty, the executor extracts values from
 * the {@code parameters} map in declared order and passes them positionally; remaining
 * parameters are passed as kwargs. When {@code paramNames} is empty, the entire map is passed as
 * kwargs.
 */
public final class PythonToolExecutor implements ToolExecutor {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PythonToolExecutor.class);

  private final String toolId;
  private final String description;
  private final String cloudpickleB64;
  private final List<String> paramNames;

  private transient PythonExecutor python;
  private transient String handle;

  public PythonToolExecutor(
      String toolId, String description, String cloudpickleB64, List<String> paramNames) {
    if (toolId == null || toolId.isEmpty()) {
      throw new IllegalArgumentException("toolId must be non-empty");
    }
    if (cloudpickleB64 == null || cloudpickleB64.isEmpty()) {
      throw new IllegalArgumentException("cloudpickleB64 must be non-empty");
    }
    this.toolId = toolId;
    this.description = description == null ? toolId : description;
    this.cloudpickleB64 = cloudpickleB64;
    this.paramNames = paramNames == null ? List.of() : List.copyOf(paramNames);
  }

  /** Bind to a shared {@link PythonExecutor}. Called from {@code AgentPlanProcessFunction.open}. */
  public synchronized void bind(PythonExecutor executor) {
    if (executor == null) {
      throw new IllegalArgumentException("PythonExecutor must not be null");
    }
    this.python = executor;
    // Defer register() until first invocation — cheaper if the tool is never called.
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(() -> invokeSync(parameters));
  }

  private synchronized Object invokeSync(Map<String, Object> parameters) {
    if (python == null) {
      throw new IllegalStateException(
          "PythonToolExecutor '" + toolId + "' not bound; call bind(PythonExecutor) first");
    }
    if (handle == null) {
      handle = python.register(cloudpickleB64);
      LOG.debug("Registered Python tool '{}' → handle {}", toolId, handle);
    }
    List<Object> args = new ArrayList<>(paramNames.size());
    java.util.Map<String, Object> remaining =
        parameters == null
            ? new java.util.LinkedHashMap<>()
            : new java.util.LinkedHashMap<>(parameters);
    for (String name : paramNames) {
      args.add(remaining.remove(name));
    }
    return python.invoke(handle, args, remaining);
  }

  @Override
  public String getToolId() {
    return toolId;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public List<String> getParamNames() {
    return paramNames;
  }
}
