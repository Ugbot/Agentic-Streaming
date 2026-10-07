package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.storage.postgres.PostgresConversationStore;
import org.agentic.flink.storage.postgres.PostgresTestDatabase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@link PostgresChangeChannel} against a real PostgreSQL (Testcontainers on Podman). The channel
 * tails the {@code agent_facts} table that {@link PostgresConversationStore} writes, so the test
 * writes facts through the store and reads them back through the channel's poll function.
 */
@Tag("integration")
class PostgresChangeChannelIT {

  private static PostgresTestDatabase database;
  private static PostgresConversationStore store;

  @BeforeAll
  static void start() throws Exception {
    database = PostgresTestDatabase.start();
    store = new PostgresConversationStore();
    store.initialize(database.storeConfig());
  }

  @AfterAll
  static void stop() throws Exception {
    if (store != null) store.close();
    if (database != null) database.close();
  }

  private static PostgresChangeChannel.PostgresPollFn openPollFn() throws Exception {
    Map<String, String> cfg = database.storeConfig();
    String user = cfg.get("postgres.user");
    String password = cfg.get("postgres.password");
    PostgresChangeChannel channel =
        new PostgresChangeChannel(database.jdbcUrl(), user, password, Duration.ofMillis(1));
    PostgresChangeChannel.PostgresPollFn fn =
        new PostgresChangeChannel.PostgresPollFn(database.jdbcUrl(), user, password, 1L);
    fn =
        InstantiationUtil.deserializeObject(
            InstantiationUtil.serializeObject(fn), PostgresChangeChannelIT.class.getClassLoader());
    fn.open(0);
    assertEquals("postgres-change", channel.providerName());
    return fn;
  }

  private static List<KeyedContextItem> drain(PostgresChangeChannel.PostgresPollFn fn, int expected)
      throws Exception {
    List<KeyedContextItem> out = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 30_000;
    while (out.size() < expected && System.currentTimeMillis() < deadline) {
      KeyedContextItem item = fn.poll(100);
      if (item == null) {
        Thread.sleep(20);
      } else {
        out.add(item);
      }
    }
    return out;
  }

  @Test
  void emitsEveryStoredFactOnceKeyedByFlowIdAndThenGoesQuiet() throws Exception {
    String flowId = "flow-" + UUID.randomUUID();
    int n = ThreadLocalRandom.current().nextInt(2, 8);
    Set<String> contents = new HashSet<>();
    for (int i = 0; i < n; i++) {
      String content = "fact-" + UUID.randomUUID();
      contents.add(content);
      store.addFact(
          flowId,
          "id-" + UUID.randomUUID(),
          new ContextItem(content, ContextPriority.SHOULD, MemoryType.LONG_TERM));
    }

    PostgresChangeChannel.PostgresPollFn fn = openPollFn();
    List<KeyedContextItem> seen = drain(fn, n);

    assertEquals(n, seen.size());
    Set<String> seenContents = new HashSet<>();
    for (KeyedContextItem item : seen) {
      assertEquals(flowId, item.getFlowId());
      assertTrue(seenContents.add(item.getItem().getContent()), "duplicate emission");
    }
    assertEquals(contents, seenContents);

    Thread.sleep(50);
    assertNull(fn.poll(100), "watermark advanced past every stored row");

    String late = "late-" + UUID.randomUUID();
    store.addFact(
        flowId, "late", new ContextItem(late, ContextPriority.SHOULD, MemoryType.LONG_TERM));
    List<KeyedContextItem> tail = drain(fn, 1);
    assertEquals(1, tail.size());
    assertEquals(late, tail.get(0).getItem().getContent());
  }
}
