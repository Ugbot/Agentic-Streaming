package org.agentic.flink.a2a;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes a remote A2A agent as a {@link ToolExecutor}, so calling a peer agent chains into an
 * Agentic-Flink workflow exactly like any other tool the LLM may select.
 *
 * <p>Registered under {@code "a2a:" + spec.name()} (see {@link RemoteAgentSpec#toolId()}). On {@link
 * #execute(Map)} it builds an {@link A2AMessage} from the call parameters, sends it to the peer, and
 * — for long-running tasks — awaits a {@linkplain A2ATaskState#isFinal() final} state (streaming
 * where the spec/peer support it, otherwise {@code message/send} + {@code tasks/get} polling),
 * returning the peer's artifacts flattened into the result map the agent loop consumes.
 *
 * <p>Serializable (ships in the job graph); the live {@link A2AClient} and the blocking-call thread
 * pool are {@code transient} and built lazily on the task side, per the project's convention for
 * non-serializable runtime resources.
 *
 * <p>Parameter conventions for {@link #execute(Map)}:
 *
 * <ul>
 *   <li>{@code input} or {@code prompt} (String) → a text part.
 *   <li>{@code data} (Map) → a structured data part.
 *   <li>{@code contextId} (String) → continues an existing A2A conversation.
 *   <li>otherwise the whole parameter map is sent as a single data part.
 * </ul>
 */
public final class A2AToolExecutor implements ToolExecutor {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(A2AToolExecutor.class);

  private final RemoteAgentSpec spec;
  private final A2AClientFactory clientFactory;

  private transient volatile A2AClient client;
  private transient volatile ExecutorService blockingPool;

  public A2AToolExecutor(RemoteAgentSpec spec, A2AClientFactory clientFactory) {
    this.spec = java.util.Objects.requireNonNull(spec, "spec");
    this.clientFactory =
        clientFactory == null ? A2AClientFactory.discovering() : clientFactory;
  }

  @Override
  public String getToolId() {
    return spec.toolId();
  }

  @Override
  public String getDescription() {
    if (spec.description() != null && !spec.description().isEmpty()) {
      return spec.description();
    }
    return "Delegate to remote A2A agent '" + spec.name() + "'"
        + (spec.skillId() != null ? " (skill: " + spec.skillId() + ")" : "");
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    return parameters != null;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    final Map<String, Object> params = parameters == null ? Map.of() : parameters;
    return CompletableFuture.supplyAsync(() -> invoke(params), pool());
  }

  private Object invoke(Map<String, Object> params) {
    A2AMessage message = buildMessage(params);
    A2AClient c = client();
    A2ATask task =
        spec.streaming()
            ? c.stream(message, t -> LOG.debug("A2A {} update: {}", spec.name(), t.getState()))
            : c.sendAndAwait(message, spec.pollIntervalMs(), spec.requestTimeoutMs());
    return toResult(task);
  }

  private A2AMessage buildMessage(Map<String, Object> params) {
    String contextId = asString(params.get("contextId"));
    String taskId = asString(params.get("taskId"));
    List<A2APart> parts = new ArrayList<>();

    Object input = params.containsKey("input") ? params.get("input") : params.get("prompt");
    if (input instanceof String && !((String) input).isEmpty()) {
      parts.add(A2APart.text((String) input));
    }
    Object data = params.get("data");
    if (data instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> dataMap = (Map<String, Object>) data;
      parts.add(A2APart.data(dataMap));
    }
    if (parts.isEmpty()) {
      // No recognized field — forward the whole call payload as structured data.
      Map<String, Object> payload = new LinkedHashMap<>(params);
      payload.remove("contextId");
      payload.remove("taskId");
      if (payload.isEmpty()) {
        parts.add(A2APart.text(""));
      } else {
        parts.add(A2APart.data(payload));
      }
    }
    return new A2AMessage(
        A2AMessage.Role.USER, UUID.randomUUID().toString(), parts, contextId, taskId, null);
  }

  private Map<String, Object> toResult(A2ATask task) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("tool", getToolId());
    result.put("agent", spec.name());
    result.put("taskId", task.getId());
    result.put("contextId", task.getContextId());
    result.put("state", task.getState().wire());
    result.put("success", task.getState() == A2ATaskState.COMPLETED);
    if (task.getStatusMessage() != null) {
      result.put("statusMessage", task.getStatusMessage());
    }
    if (task.getState() == A2ATaskState.INPUT_REQUIRED) {
      result.put("needsInput", true);
    }
    if (task.getState() == A2ATaskState.AUTH_REQUIRED) {
      result.put("needsAuth", true);
    }

    StringBuilder text = new StringBuilder();
    List<Map<String, Object>> artifacts = new ArrayList<>();
    for (A2AArtifact artifact : task.getArtifacts()) {
      Map<String, Object> a = new LinkedHashMap<>();
      a.put("artifactId", artifact.getArtifactId());
      a.put("name", artifact.getName());
      String t = artifact.textContent();
      if (!t.isEmpty()) {
        a.put("text", t);
        if (text.length() > 0) {
          text.append('\n');
        }
        text.append(t);
      }
      List<Map<String, Object>> dataParts = new ArrayList<>();
      for (A2APart part : artifact.getParts()) {
        if (part.getKind() == A2APart.Kind.DATA && part.getData() != null) {
          dataParts.add(part.getData());
        }
      }
      if (!dataParts.isEmpty()) {
        a.put("data", dataParts.size() == 1 ? dataParts.get(0) : dataParts);
      }
      artifacts.add(a);
    }
    result.put("text", text.toString());
    result.put("artifacts", artifacts);
    return result;
  }

  private static String asString(Object o) {
    return o == null ? null : o.toString();
  }

  private A2AClient client() {
    A2AClient c = client;
    if (c == null) {
      synchronized (this) {
        if (client == null) {
          client = clientFactory.create(spec);
        }
        c = client;
      }
    }
    return c;
  }

  private ExecutorService pool() {
    ExecutorService p = blockingPool;
    if (p == null) {
      synchronized (this) {
        if (blockingPool == null) {
          // Cached pool of daemon threads: A2A await/poll calls block, so they must not run on
          // the operator's async-IO callback thread. Idle threads are reaped after 60s; daemon
          // status lets the JVM exit cleanly.
          ThreadPoolExecutor tp =
              new ThreadPoolExecutor(
                  0,
                  Integer.MAX_VALUE,
                  60L,
                  TimeUnit.SECONDS,
                  new SynchronousQueue<>(),
                  r -> {
                    Thread t = new Thread(r, "a2a-tool-" + spec.name());
                    t.setDaemon(true);
                    return t;
                  });
          blockingPool = tp;
        }
        p = blockingPool;
      }
    }
    return p;
  }

  /** Spec this executor delegates to. */
  public RemoteAgentSpec spec() {
    return spec;
  }
}
