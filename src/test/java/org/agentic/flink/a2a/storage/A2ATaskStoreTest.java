package org.agentic.flink.a2a.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.AuthSpec;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Backend-agnostic contract for {@link A2ATaskStore}, run against the in-memory and Postgres (H2)
 * implementations with randomized task graphs. Redis is covered by an integration test.
 */
class A2ATaskStoreTest {

  private final Random random = new Random();

  private A2ATaskStore store(String backend) throws Exception {
    Map<String, String> config = new HashMap<>();
    if (!"memory".equals(backend)) {
      config.put("postgres.url", "jdbc:h2:mem:a2a_" + UUID.randomUUID().toString().replace('-', '_')
          + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
      config.put("postgres.user", "sa");
      config.put("postgres.auto.create.tables", "true");
    }
    return A2ATaskStoreFactory.create(backend, config);
  }

  private A2ATask randomTask(String contextId, A2ATaskState state) {
    String id = UUID.randomUUID().toString();
    long now = Math.abs(random.nextLong() % 1_000_000_000L);
    A2AMessage msg = A2AMessage.userText(UUID.randomUUID().toString(), "in-" + random.nextInt());
    A2ATask task = A2ATask.submitted(id, contextId, msg, now).withState(state, "s", now);
    int n = random.nextInt(3);
    for (int i = 0; i < n; i++) {
      task = task.withArtifact(
          A2AArtifact.text(UUID.randomUUID().toString(), "a" + i, "out-" + random.nextInt()), now + i);
    }
    return task;
  }

  @ParameterizedTest(name = "[{0}] save/load round-trips a task")
  @ValueSource(strings = {"memory", "postgres"})
  void saveLoadRoundTrip(String backend) throws Exception {
    try (A2ATaskStore store = store(backend)) {
      A2ATask task = randomTask(UUID.randomUUID().toString(), A2ATaskState.WORKING);
      store.saveTask(task);
      A2ATask loaded = store.loadTask(task.getId()).orElseThrow();
      assertEquals(task, loaded);
      assertTrue(store.loadTask("nope").isEmpty());
    }
  }

  @ParameterizedTest(name = "[{0}] list by context and by state")
  @ValueSource(strings = {"memory", "postgres"})
  void listing(String backend) throws Exception {
    try (A2ATaskStore store = store(backend)) {
      String ctx = "conv-" + UUID.randomUUID();
      A2ATask a = randomTask(ctx, A2ATaskState.WORKING);
      A2ATask b = randomTask(ctx, A2ATaskState.COMPLETED);
      A2ATask other = randomTask("conv-other", A2ATaskState.WORKING);
      store.saveTask(a);
      store.saveTask(b);
      store.saveTask(other);

      List<A2ATask> byCtx = store.listTasksByContext(ctx);
      assertEquals(2, byCtx.size());
      assertTrue(byCtx.stream().anyMatch(t -> t.getId().equals(a.getId())));
      assertTrue(byCtx.stream().anyMatch(t -> t.getId().equals(b.getId())));

      List<A2ATask> working = store.listTasksByState(A2ATaskState.WORKING);
      assertTrue(working.stream().anyMatch(t -> t.getId().equals(a.getId())));
      assertTrue(working.stream().anyMatch(t -> t.getId().equals(other.getId())));
      assertFalse(working.stream().anyMatch(t -> t.getId().equals(b.getId())));
    }
  }

  @ParameterizedTest(name = "[{0}] state transition updates state index")
  @ValueSource(strings = {"memory", "postgres"})
  void stateTransitionReindexes(String backend) throws Exception {
    try (A2ATaskStore store = store(backend)) {
      A2ATask task = randomTask(UUID.randomUUID().toString(), A2ATaskState.WORKING);
      store.saveTask(task);
      assertEquals(1, store.listTasksByState(A2ATaskState.WORKING).size());

      A2ATask completed = task.withState(A2ATaskState.COMPLETED, "done", task.getUpdatedAtEpochMs() + 1);
      store.saveTask(completed);
      assertTrue(store.listTasksByState(A2ATaskState.WORKING).stream()
          .noneMatch(t -> t.getId().equals(task.getId())));
      assertTrue(store.listTasksByState(A2ATaskState.COMPLETED).stream()
          .anyMatch(t -> t.getId().equals(task.getId())));
    }
  }

  @ParameterizedTest(name = "[{0}] push config CRUD")
  @ValueSource(strings = {"memory", "postgres"})
  void pushConfigCrud(String backend) throws Exception {
    try (A2ATaskStore store = store(backend)) {
      A2ATask task = randomTask(UUID.randomUUID().toString(), A2ATaskState.WORKING);
      store.saveTask(task);
      A2APushConfig c1 =
          new A2APushConfig("cfg-1", "https://cb/1", "tok1", AuthSpec.bearer("secret"));
      A2APushConfig c2 = new A2APushConfig("cfg-2", "https://cb/2", null, AuthSpec.none());
      store.savePushConfig(task.getId(), c1);
      store.savePushConfig(task.getId(), c2);

      assertEquals(2, store.listPushConfigs(task.getId()).size());
      assertEquals(c1, store.getPushConfig(task.getId(), "cfg-1").orElseThrow());

      store.deletePushConfig(task.getId(), "cfg-1");
      assertTrue(store.getPushConfig(task.getId(), "cfg-1").isEmpty());
      assertEquals(1, store.listPushConfigs(task.getId()).size());
    }
  }

  @ParameterizedTest(name = "[{0}] delete removes task and its push configs")
  @ValueSource(strings = {"memory", "postgres"})
  void deleteCascades(String backend) throws Exception {
    try (A2ATaskStore store = store(backend)) {
      A2ATask task = randomTask("ctx", A2ATaskState.WORKING);
      store.saveTask(task);
      store.savePushConfig(task.getId(), new A2APushConfig("c", "https://cb", null, null));
      store.deleteTask(task.getId());
      assertTrue(store.loadTask(task.getId()).isEmpty());
      assertTrue(store.listPushConfigs(task.getId()).isEmpty());
      assertTrue(store.listTasksByContext("ctx").isEmpty());
    }
  }
}
