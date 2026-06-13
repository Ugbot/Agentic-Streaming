package org.agentic.flink.a2a.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;

/**
 * In-memory {@link A2ATaskStore} for tests and single-JVM / embedded gateway deployments.
 *
 * <p>No persistence across restarts. Backed by concurrent maps; safe for the gateway's
 * multi-threaded request handlers. Push configs are keyed per task and per config id.
 */
public final class InMemoryA2ATaskStore implements A2ATaskStore {

  private final Map<String, A2ATask> tasks = new ConcurrentHashMap<>();
  // taskId -> (configId -> config), insertion-ordered.
  private final Map<String, Map<String, A2APushConfig>> pushConfigs = new ConcurrentHashMap<>();

  @Override
  public void initialize(Map<String, String> config) {
    // Nothing to configure.
  }

  @Override
  public void saveTask(A2ATask task) {
    tasks.put(task.getId(), task);
  }

  @Override
  public Optional<A2ATask> loadTask(String taskId) {
    return Optional.ofNullable(tasks.get(taskId));
  }

  @Override
  public List<A2ATask> listTasksByContext(String contextId) {
    List<A2ATask> out = new ArrayList<>();
    for (A2ATask t : tasks.values()) {
      if (contextId != null && contextId.equals(t.getContextId())) {
        out.add(t);
      }
    }
    out.sort((a, b) -> Long.compare(a.getCreatedAtEpochMs(), b.getCreatedAtEpochMs()));
    return out;
  }

  @Override
  public List<A2ATask> listTasksByState(A2ATaskState state) {
    List<A2ATask> out = new ArrayList<>();
    for (A2ATask t : tasks.values()) {
      if (t.getState() == state) {
        out.add(t);
      }
    }
    return out;
  }

  @Override
  public void deleteTask(String taskId) {
    tasks.remove(taskId);
    pushConfigs.remove(taskId);
  }

  @Override
  public void savePushConfig(String taskId, A2APushConfig config) {
    pushConfigs
        .computeIfAbsent(taskId, k -> new ConcurrentHashMap<>())
        .put(config.getId(), config);
  }

  @Override
  public List<A2APushConfig> listPushConfigs(String taskId) {
    Map<String, A2APushConfig> m = pushConfigs.get(taskId);
    return m == null ? List.of() : new ArrayList<>(m.values());
  }

  @Override
  public Optional<A2APushConfig> getPushConfig(String taskId, String configId) {
    Map<String, A2APushConfig> m = pushConfigs.get(taskId);
    return m == null ? Optional.empty() : Optional.ofNullable(m.get(configId));
  }

  @Override
  public void deletePushConfig(String taskId, String configId) {
    Map<String, A2APushConfig> m = pushConfigs.get(taskId);
    if (m != null) {
      m.remove(configId);
    }
  }

  @Override
  public String getProviderName() {
    return "memory";
  }
}
