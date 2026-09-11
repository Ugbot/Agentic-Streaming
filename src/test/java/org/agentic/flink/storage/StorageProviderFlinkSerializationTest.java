package org.agentic.flink.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATaskState;
import org.agentic.flink.a2a.storage.InMemoryA2ATaskStore;
import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import org.agentic.flink.storage.memory.InMemoryShortTermStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.apache.flink.util.function.SerializableFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Every in-process storage provider must survive the serialization Flink applies when it ships an
 * operator to a task: transient resources are rebuilt on first use after deserialization and the
 * copy is fully usable. The external providers get the same treatment against real backends in
 * the integration group (PostgresConversationStoreTest, PostgresA2ATaskStoreTest).
 */
class StorageProviderFlinkSerializationTest {

  private static ContextItem item(String content) {
    ContextItem i = new ContextItem(content, ContextPriority.SHOULD, MemoryType.SHORT_TERM);
    i.setItemId(UUID.randomUUID().toString());
    return i;
  }

  private static <S extends Serializable> void assertAllRoutes(
      S store, SerializableFunction<S, String> use, String expected) throws Exception {
    assertEquals(expected, use.apply(FlinkSerializationHarness.viaInstantiationUtil(store)));
    assertEquals(expected, use.apply(FlinkSerializationHarness.viaTypeSerializer(store)));
    String input = "in-" + UUID.randomUUID();
    assertEquals(
        List.of(input + "=" + expected),
        FlinkSerializationHarness.viaJobGraph(store, use, List.of(input)));
  }

  @Test
  @DisplayName("InMemoryLongTermStore: transient maps are rebuilt and the copy is usable")
  void inMemoryLongTermStore() throws Exception {
    InMemoryLongTermStore store = new InMemoryLongTermStore();
    store.initialize(new HashMap<>());
    String flowId = "flow-" + UUID.randomUUID();
    String userId = "user-" + UUID.randomUUID();
    String content = "c-" + UUID.randomUUID();
    assertAllRoutes(
        store,
        s -> {
          try {
            AgentContext ctx = new AgentContext("agent", flowId, userId, 1000, 10);
            ctx.addContext(item(content));
            s.saveContext(flowId, ctx);
            s.addFact(flowId, "f", item(content));
            return s.loadContext(flowId).orElseThrow().getUserId()
                + "/"
                + s.loadFacts(flowId).get("f").getContent()
                + "/"
                + s.listConversationsForUser(userId).size();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        userId + "/" + content + "/1");
  }

  @Test
  @DisplayName("InMemoryShortTermStore: transient maps are rebuilt and the copy is usable")
  void inMemoryShortTermStore() throws Exception {
    InMemoryShortTermStore store = new InMemoryShortTermStore();
    store.initialize(new HashMap<>());
    String flowId = "flow-" + UUID.randomUUID();
    int n = 1 + ThreadLocalRandom.current().nextInt(5);
    assertAllRoutes(
        store,
        s -> {
          try {
            for (int i = 0; i < n; i++) {
              s.addItem(flowId, item("i-" + UUID.randomUUID()));
            }
            int count = s.getItemCount(flowId);
            s.clearItems(flowId);
            return count + "/" + s.getItemCount(flowId);
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        n + "/0");
  }

  @Test
  @DisplayName("InMemoryVectorStore: stored entries and the index survive serialization")
  void inMemoryVectorStore() throws Exception {
    InMemoryVectorStore store = new InMemoryVectorStore();
    int dim = 2 + ThreadLocalRandom.current().nextInt(6);
    Map<String, String> cfg = new HashMap<>();
    cfg.put("vector.dimension", Integer.toString(dim));
    store.initialize(cfg);
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = ThreadLocalRandom.current().nextFloat() + 0.1f;
    }
    String id = "vec-" + UUID.randomUUID();
    Map<String, Object> meta = new HashMap<>();
    meta.put("flowId", "flow-" + UUID.randomUUID());
    store.storeEmbedding(id, v, meta);
    assertAllRoutes(
        store,
        s -> {
          try {
            String top = s.searchSimilar(v, 1).get(0).getId();
            String another = "vec-" + UUID.randomUUID();
            s.storeEmbedding(another, v, new HashMap<>(meta));
            return top + "/" + s.searchSimilar(v, 5).size() + "/" + s.getEmbeddingDimension();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        id + "/2/" + dim);
  }

  @Test
  @DisplayName("InMemoryA2ATaskStore: tasks survive serialization and the copy accepts writes")
  void inMemoryA2ATaskStore() throws Exception {
    InMemoryA2ATaskStore store = new InMemoryA2ATaskStore();
    store.initialize(new HashMap<>());
    String ctx = "conv-" + UUID.randomUUID();
    A2ATask task =
        A2ATask.submitted(
            UUID.randomUUID().toString(),
            ctx,
            A2AMessage.userText(UUID.randomUUID().toString(), "hi-" + UUID.randomUUID()),
            ThreadLocalRandom.current().nextLong(1, 1_000_000L));
    assertAllRoutes(
        store,
        s -> {
          s.saveTask(task);
          A2ATask loaded = s.loadTask(task.getId()).orElseThrow();
          s.saveTask(loaded.withState(A2ATaskState.COMPLETED, "done", loaded.getUpdatedAtEpochMs() + 1));
          return s.listTasksByContext(ctx).size() + "/" + s.listTasksByState(A2ATaskState.COMPLETED).size();
        },
        "1/1");
  }

  @Test
  @DisplayName("a provider that was never initialized fails loudly instead of NPE-ing after deserialization")
  void uninitializedProviderFailsLoudly() throws Exception {
    InMemoryShortTermStore copy =
        FlinkSerializationHarness.viaInstantiationUtil(new InMemoryShortTermStore());
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> copy.addItem("flow", item("x")));
    assertTrue(e.getMessage().contains("initialize"), e.getMessage());
  }
}
