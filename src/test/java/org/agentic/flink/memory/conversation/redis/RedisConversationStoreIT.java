package org.agentic.flink.memory.conversation.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for {@link RedisConversationStore} against a real Redis (Testcontainers). Runs
 * only under {@code -P integration-tests}. Exercises the same {@link ConversationStore} contract as
 * the in-JVM store's unit test, with randomized data.
 */
@Tag("integration")
class RedisConversationStoreIT {

  private static GenericContainer<?> redis;

  @BeforeAll
  static void startRedis() {
    redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    redis.start();
  }

  @AfterAll
  static void stopRedis() {
    if (redis != null) {
      redis.stop();
    }
  }

  private ConversationStore store() {
    return new RedisConversationStore(redis.getHost(), redis.getMappedPort(6379));
  }

  private static String randomId() {
    return "ctx-" + UUID.randomUUID();
  }

  private static List<ChatMessage> randomDialogue(int turns) {
    List<ChatMessage> out = new ArrayList<>();
    for (int i = 0; i < turns; i++) {
      String text = UUID.randomUUID().toString();
      out.add(i % 2 == 0 ? ChatMessage.user(text) : ChatMessage.assistant(text));
    }
    return out;
  }

  @Test
  @DisplayName("redis: transcript preserves order and survives a fresh store instance")
  void transcriptRoundTrips() {
    ConversationStore store = store();
    String id = randomId();
    int turns = ThreadLocalRandom.current().nextInt(3, 20);
    List<ChatMessage> dialogue = randomDialogue(turns);
    dialogue.forEach(m -> store.append(id, m));

    // A *separate* store instance reads the same Redis state (cross-process spine).
    List<ChatMessage> history = store().history(id);
    assertEquals(turns, history.size());
    assertEquals(turns, store().messageCount(id));
    for (int i = 0; i < turns; i++) {
      assertEquals(dialogue.get(i).getContent(), history.get(i).getContent());
      assertEquals(dialogue.get(i).getRole(), history.get(i).getRole());
    }
    store.clear(id);
  }

  @Test
  @DisplayName("redis: tool messages keep their identity through encode/decode")
  void toolRolePreserved() {
    ConversationStore store = store();
    String id = randomId();
    String callId = UUID.randomUUID().toString();
    store.append(id, ChatMessage.tool(callId, "kb_search", "result-" + UUID.randomUUID()));

    ChatMessage m = store.history(id).get(0);
    assertEquals(ChatRole.TOOL, m.getRole());
    assertEquals(callId, m.getToolCallId());
    assertEquals("kb_search", m.getToolName());
    store.clear(id);
  }

  @Test
  @DisplayName("redis: attributes round-trip and recent() returns the tail")
  void attributesAndRecent() {
    ConversationStore store = store();
    String id = randomId();
    String value = "READY_TO_ACT_" + ThreadLocalRandom.current().nextInt(1000);
    store.putAttribute(id, "phase", value);
    assertEquals(value, store.getAttribute(id, "phase").orElseThrow());
    assertEquals(value, store.attributes(id).get("phase"));

    int turns = ThreadLocalRandom.current().nextInt(8, 20);
    List<ChatMessage> dialogue = randomDialogue(turns);
    dialogue.forEach(m -> store.append(id, m));
    int n = ThreadLocalRandom.current().nextInt(1, turns);
    List<ChatMessage> recent = store.recent(id, n);
    assertEquals(n, recent.size());
    for (int i = 0; i < n; i++) {
      assertEquals(dialogue.get(turns - n + i).getContent(), recent.get(i).getContent());
    }
    store.clear(id);
  }

  @Test
  @DisplayName("redis: conversations retrievable by owning user; reassociation moves them")
  void byUser() {
    ConversationStore store = store();
    String user = "user-" + UUID.randomUUID();
    String id = randomId();
    store.associateUser(id, user);
    assertEquals(user, store.userOf(id).orElseThrow());
    assertTrue(store.conversationsForUser(user).contains(id));

    String user2 = "user-" + UUID.randomUUID();
    store.associateUser(id, user2);
    assertEquals(user2, store.userOf(id).orElseThrow());
    assertFalse(store.conversationsForUser(user).contains(id));
    assertTrue(store.conversationsForUser(user2).contains(id));
    store.clear(id);
  }

  @Test
  @DisplayName("redis: bounded transcript keeps only the most recent messages")
  void boundedEviction() {
    // Default cap is 200; push more and assert we never exceed it.
    ConversationStore store = store();
    String id = randomId();
    int cap = 200;
    int total = cap + ThreadLocalRandom.current().nextInt(10, 40);
    List<ChatMessage> dialogue = randomDialogue(total);
    dialogue.forEach(m -> store.append(id, m));

    assertEquals(cap, store.messageCount(id));
    List<ChatMessage> history = store.history(id);
    // The retained window is the most-recent `cap` messages, in order.
    for (int i = 0; i < cap; i++) {
      assertEquals(dialogue.get(total - cap + i).getContent(), history.get(i).getContent());
    }
    store.clear(id);
  }

  @Test
  @DisplayName("redis: clear() forgets transcript, attributes, and user association")
  void clearForgets() {
    ConversationStore store = store();
    String id = randomId();
    String user = "user-" + UUID.randomUUID();
    store.append(id, ChatMessage.user("x"));
    store.putAttribute(id, "phase", "NEW");
    store.associateUser(id, user);

    store.clear(id);

    assertEquals(0, store.messageCount(id));
    assertTrue(store.getAttribute(id, "phase").isEmpty());
    assertTrue(store.userOf(id).isEmpty());
    assertFalse(store.conversationsForUser(user).contains(id));
    assertFalse(store.conversations().contains(id));
  }
}
