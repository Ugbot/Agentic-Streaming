package org.agentic.flink.a2a.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.AuthSpec;
import org.agentic.flink.config.ConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for {@link RedisA2ATaskStore} against a real Redis (Testcontainers). Runs only
 * under {@code -P integration-tests}. Exercises the same contract as {@link A2ATaskStoreTest}.
 */
@Tag("integration")
class A2ARedisTaskStoreIT {

  private static GenericContainer<?> redis;

  @BeforeAll
  static void startRedis() {
    redis =
        new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    redis.start();
  }

  @AfterAll
  static void stopRedis() {
    if (redis != null) {
      redis.stop();
    }
  }

  private A2ATaskStore store() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(ConfigKeys.REDIS_HOST, redis.getHost());
    config.put(ConfigKeys.REDIS_PORT, String.valueOf(redis.getMappedPort(6379)));
    return A2ATaskStoreFactory.create("redis", config);
  }

  @Test
  @DisplayName("redis: full task + push-config lifecycle round-trips")
  void lifecycle() throws Exception {
    try (A2ATaskStore store = store()) {
      String ctx = "conv-" + UUID.randomUUID();
      A2AMessage msg = A2AMessage.userText(UUID.randomUUID().toString(), "compute");
      A2ATask task =
          A2ATask.submitted(UUID.randomUUID().toString(), ctx, msg, 1000L)
              .withState(A2ATaskState.WORKING, "s", 1000L)
              .withArtifact(A2AArtifact.text(UUID.randomUUID().toString(), "r", "out"), 1001L);
      store.saveTask(task);

      assertEquals(task, store.loadTask(task.getId()).orElseThrow());
      assertEquals(1, store.listTasksByContext(ctx).size());
      assertEquals(1, store.listTasksByState(A2ATaskState.WORKING).size());

      // State transition re-indexes.
      store.saveTask(task.withState(A2ATaskState.COMPLETED, "done", 1002L));
      assertTrue(store.listTasksByState(A2ATaskState.WORKING).isEmpty());
      assertEquals(1, store.listTasksByState(A2ATaskState.COMPLETED).size());

      // Push configs.
      store.savePushConfig(task.getId(), new A2APushConfig("c1", "https://cb", "t", AuthSpec.bearer("x")));
      assertEquals(1, store.listPushConfigs(task.getId()).size());

      store.deleteTask(task.getId());
      assertTrue(store.loadTask(task.getId()).isEmpty());
      assertTrue(store.listPushConfigs(task.getId()).isEmpty());
    }
  }
}
