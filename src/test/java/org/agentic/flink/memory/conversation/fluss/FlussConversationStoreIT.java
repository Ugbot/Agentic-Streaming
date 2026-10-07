package org.agentic.flink.memory.conversation.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.testkit.FlussTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link FlussConversationStore} against a real Fluss cluster. Runs under
 * {@code ./mvnw verify -P integration-tests}. The cluster comes from {@link FlussTestCluster}:
 * {@code FLUSS_BOOTSTRAP_SERVERS} if set (for example the compose stack in {@code
 * docker-compose-fluss.yml}), otherwise Testcontainers on Podman. An unreachable cluster fails the
 * test. Exercises the same {@link ConversationStore} contract as the in-JVM unit test, with
 * randomized data.
 */
@Tag("integration")
class FlussConversationStoreIT {

  private static FlussTestCluster cluster;
  private static String bootstrap;

  @BeforeAll
  static void startCluster() {
    cluster = FlussTestCluster.start();
    bootstrap = cluster.bootstrapServers();
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  /** Fresh table per run so concurrent/repeat runs stay isolated. */
  private ConversationStore store() {
    return new FlussConversationStore(
        bootstrap, "agentic_it", "conv_" + UUID.randomUUID().toString().replace("-", ""));
  }

  @Test
  @DisplayName("transcript append + bounded history round-trips through the PK table")
  void transcriptRoundTrips() {
    ConversationStore store = store();
    String conv = "ctx-" + UUID.randomUUID();
    store.append(conv, ChatMessage.user("hello " + UUID.randomUUID()));
    store.append(conv, ChatMessage.assistant("hi " + UUID.randomUUID()));
    store.append(conv, ChatMessage.tool(UUID.randomUUID().toString(), "kb_search", "result"));

    List<ChatMessage> history = store.history(conv);
    assertEquals(3, history.size());
    assertEquals(3, store.messageCount(conv));
    assertEquals("kb_search", history.get(2).getToolName());
    assertTrue(store.conversations().contains(conv));
  }

  @Test
  @DisplayName("attributes (e.g. the A2A contextId) survive across reads — the continuity spine")
  void attributesRoundTrip() {
    ConversationStore store = store();
    String conv = "ctx-" + UUID.randomUUID();
    String remoteCtx = "remote-" + UUID.randomUUID();
    store.putAttribute(conv, "a2a.cs.contextId", remoteCtx);
    store.putAttribute(conv, "banking.phase", "router");

    assertEquals(remoteCtx, store.getAttribute(conv, "a2a.cs.contextId").orElseThrow());
    assertEquals("router", store.getAttribute(conv, "banking.phase").orElseThrow());
    assertEquals(2, store.attributes(conv).size());

    store.putAttribute(conv, "banking.phase", "verify"); // overwrite, not duplicate
    assertEquals("verify", store.getAttribute(conv, "banking.phase").orElseThrow());
    assertEquals(2, store.attributes(conv).size());
  }

  @Test
  @DisplayName("user association + reverse index + clear behave correctly")
  void userIndexAndClear() {
    ConversationStore store = store();
    String user = "user-" + UUID.randomUUID();
    String c1 = "ctx-" + UUID.randomUUID();
    String c2 = "ctx-" + UUID.randomUUID();
    store.append(c1, ChatMessage.user("a"));
    store.append(c2, ChatMessage.user("b"));
    store.associateUser(c1, user);
    store.associateUser(c2, user);

    assertEquals(user, store.userOf(c1).orElseThrow());
    List<String> forUser = store.conversationsForUser(user);
    assertTrue(forUser.contains(c1) && forUser.contains(c2));

    store.clear(c1);
    assertTrue(store.userOf(c1).isEmpty());
    assertFalse(store.conversationsForUser(user).contains(c1));
    assertFalse(store.conversations().contains(c1));
    assertTrue(store.conversationsForUser(user).contains(c2), "clearing c1 must not affect c2");
  }
}
