package org.agentic.flink.a2a.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APushConfig;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.AuthSpec;
import org.agentic.flink.storage.postgres.PostgresTestDatabase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link PostgresA2ATaskStore} against a real PostgreSQL (Testcontainers on Podman): the {@link
 * A2ATaskStore} contract, {@code ON CONFLICT} upserts under concurrent writers for the same
 * task/context, and use after Flink serialization.
 */
@Tag("integration")
class PostgresA2ATaskStoreTest {

  private static PostgresTestDatabase database;

  private A2ATaskStore store;

  @BeforeAll
  static void startDatabase() {
    database = PostgresTestDatabase.start();
  }

  @AfterAll
  static void stopDatabase() {
    if (database != null) {
      database.close();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    store = A2ATaskStoreFactory.create("postgres", database.storeConfig());
    assertTrue(store instanceof PostgresA2ATaskStore);
  }

  @AfterEach
  void tearDown() throws Exception {
    store.close();
  }

  @Test
  @DisplayName("save/load round-trips a randomized task")
  void saveLoadRoundTrip() throws Exception {
    A2ATask task = randomTask("conv-" + UUID.randomUUID(), A2ATaskState.WORKING);
    store.saveTask(task);
    assertEquals(task, store.loadTask(task.getId()).orElseThrow());
    assertTrue(store.loadTask(UUID.randomUUID().toString()).isEmpty());
  }

  @Test
  @DisplayName("re-saving a task upserts the row and re-indexes its state")
  void stateTransitionUpserts() throws Exception {
    A2ATask task = randomTask("conv-" + UUID.randomUUID(), A2ATaskState.WORKING);
    store.saveTask(task);
    A2ATask completed =
        task.withState(A2ATaskState.COMPLETED, "done", task.getUpdatedAtEpochMs() + 1);
    store.saveTask(completed);

    assertEquals(completed, store.loadTask(task.getId()).orElseThrow());
    assertTrue(
        store.listTasksByState(A2ATaskState.WORKING).stream()
            .noneMatch(t -> t.getId().equals(task.getId())));
    assertTrue(
        store.listTasksByState(A2ATaskState.COMPLETED).stream()
            .anyMatch(t -> t.getId().equals(task.getId())));
    assertEquals(1, store.listTasksByContext(task.getContextId()).size());
  }

  @Test
  @DisplayName("push configs upsert by (task_id, config_id)")
  void pushConfigUpsert() throws Exception {
    A2ATask task = randomTask("conv-" + UUID.randomUUID(), A2ATaskState.WORKING);
    store.saveTask(task);
    String cfgId = "cfg-" + UUID.randomUUID();
    A2APushConfig first =
        new A2APushConfig(cfgId, "https://cb/" + UUID.randomUUID(), "t1", AuthSpec.bearer("s1"));
    A2APushConfig second =
        new A2APushConfig(cfgId, "https://cb/" + UUID.randomUUID(), null, AuthSpec.none());
    store.savePushConfig(task.getId(), first);
    store.savePushConfig(task.getId(), second);

    assertEquals(1, store.listPushConfigs(task.getId()).size());
    assertEquals(second, store.getPushConfig(task.getId(), cfgId).orElseThrow());

    store.deletePushConfig(task.getId(), cfgId);
    assertTrue(store.getPushConfig(task.getId(), cfgId).isEmpty());
  }

  @Test
  @DisplayName("delete removes the task and its push configs")
  void deleteCascades() throws Exception {
    String ctx = "conv-" + UUID.randomUUID();
    A2ATask task = randomTask(ctx, A2ATaskState.WORKING);
    store.saveTask(task);
    store.savePushConfig(
        task.getId(), new A2APushConfig("c-" + UUID.randomUUID(), "https://cb", null, null));
    store.deleteTask(task.getId());
    assertTrue(store.loadTask(task.getId()).isEmpty());
    assertTrue(store.listPushConfigs(task.getId()).isEmpty());
    assertTrue(store.listTasksByContext(ctx).isEmpty());
  }

  @Test
  @DisplayName("concurrent writers on the same task and context converge to one row each")
  void concurrentWritersSameConversation() throws Exception {
    String ctx = "conv-" + UUID.randomUUID();
    A2ATask base = randomTask(ctx, A2ATaskState.SUBMITTED);
    store.saveTask(base);
    int writers = 8 + ThreadLocalRandom.current().nextInt(8);
    int writesPerWriter = 10 + ThreadLocalRandom.current().nextInt(10);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Integer>> results = new ArrayList<>();
    try {
      for (int w = 0; w < writers; w++) {
        int writerIndex = w;
        results.add(
            pool.submit(
                (Callable<Integer>)
                    () -> {
                      start.await();
                      for (int i = 0; i < writesPerWriter; i++) {
                        long now = base.getUpdatedAtEpochMs() + 1 + writerIndex * 1000L + i;
                        A2ATask update =
                            base.withArtifact(
                                A2AArtifact.text(
                                    "art-" + writerIndex + "-" + i,
                                    "w" + writerIndex,
                                    UUID.randomUUID().toString()),
                                now);
                        store.saveTask(update);
                        store.savePushConfig(
                            base.getId(),
                            new A2APushConfig(
                                "writer-" + writerIndex,
                                "https://cb/" + writerIndex + "/" + i,
                                null,
                                AuthSpec.none()));
                      }
                      return writesPerWriter;
                    }));
      }
      start.countDown();
      int total = 0;
      for (Future<Integer> f : results) {
        total += f.get(2, TimeUnit.MINUTES);
      }
      assertEquals(writers * writesPerWriter, total);
    } finally {
      pool.shutdownNow();
    }

    List<A2ATask> byContext = store.listTasksByContext(ctx);
    assertEquals(1, byContext.size());
    A2ATask finalTask = store.loadTask(base.getId()).orElseThrow();
    assertEquals(base.getId(), finalTask.getId());
    assertEquals(1, finalTask.getArtifacts().size() - base.getArtifacts().size());
    assertEquals(writers, store.listPushConfigs(base.getId()).size());
  }

  @Test
  @DisplayName("store serialized through Flink's InstantiationUtil reopens and keeps working")
  void usableAfterFlinkSerialization() throws Exception {
    A2ATask task = randomTask("conv-" + UUID.randomUUID(), A2ATaskState.WORKING);
    store.saveTask(task);
    byte[] bytes = InstantiationUtil.serializeObject(store);
    A2ATaskStore copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
    try {
      assertEquals(task, copy.loadTask(task.getId()).orElseThrow());
      A2ATask another = randomTask(task.getContextId(), A2ATaskState.WORKING);
      copy.saveTask(another);
      assertEquals(another, store.loadTask(another.getId()).orElseThrow());
    } finally {
      copy.close();
    }
  }

  @Test
  @DisplayName("an unreachable configured PostgreSQL fails initialize loudly")
  void unreachableDatabaseFailsLoudly() {
    Map<String, String> config = database.storeConfig();
    config.put("postgres.url", "jdbc:postgresql://127.0.0.1:1/" + UUID.randomUUID());
    assertThrows(Exception.class, () -> A2ATaskStoreFactory.create("postgres", config));
  }

  private static A2ATask randomTask(String contextId, A2ATaskState state) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    String id = UUID.randomUUID().toString();
    long now = random.nextLong(1, 1_000_000_000L);
    A2AMessage msg = A2AMessage.userText(UUID.randomUUID().toString(), "in-" + random.nextInt());
    A2ATask task = A2ATask.submitted(id, contextId, msg, now).withState(state, "s", now);
    int n = random.nextInt(3);
    for (int i = 0; i < n; i++) {
      task =
          task.withArtifact(
              A2AArtifact.text(UUID.randomUUID().toString(), "a" + i, "out-" + random.nextInt()),
              now + i);
    }
    return task;
  }
}
