package org.agentic.flink.a2a.storage;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;

/**
 * Service-provider interface for persisting A2A task lifecycle state and push-notification configs
 * on the gateway side.
 *
 * <p>The Quarkus gateway creates an {@link A2ATask} per inbound request and updates it as the Flink
 * job reports progress, serving {@code tasks/get}, resubscribe, and push notifications from this
 * store. Mirrors the {@link org.agentic.flink.storage.LongTermMemoryStore} convention: backends are
 * {@code initialize(config)}-d and discovered via {@link java.util.ServiceLoader} /
 * {@link A2ATaskStoreFactory}. Built-ins: {@code memory}, {@code postgres}, {@code redis}.
 */
public interface A2ATaskStore extends AutoCloseable {

  /** Initialize the backend from string config (connection URLs, credentials, TTLs, …). */
  void initialize(Map<String, String> config) throws Exception;

  /** Insert or replace a task by its id. */
  void saveTask(A2ATask task) throws Exception;

  /** Load a task by id. */
  Optional<A2ATask> loadTask(String taskId) throws Exception;

  /** All tasks belonging to a conversation context, in save order. */
  List<A2ATask> listTasksByContext(String contextId) throws Exception;

  /** All tasks currently in the given lifecycle state. */
  List<A2ATask> listTasksByState(A2ATaskState state) throws Exception;

  /** Delete a task and any associated push configs. */
  void deleteTask(String taskId) throws Exception;

  /** Register (or replace, by config id) a push-notification config for a task. */
  void savePushConfig(String taskId, A2APushConfig config) throws Exception;

  /** All push configs registered for a task (A2A permits more than one). */
  List<A2APushConfig> listPushConfigs(String taskId) throws Exception;

  /** A specific push config by its id, if present. */
  Optional<A2APushConfig> getPushConfig(String taskId, String configId) throws Exception;

  /** Remove a push config by id. */
  void deletePushConfig(String taskId, String configId) throws Exception;

  /** Provider name used by {@link A2ATaskStoreFactory} for ServiceLoader matching. */
  default String getProviderName() {
    return getClass().getSimpleName();
  }

  @Override
  default void close() throws Exception {}
}
