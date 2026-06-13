package org.agentic.flink.memory.conversation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The per-conversation memory store is the cross-operator transcript a routed multi-turn agent
 * relies on. Tests use randomized conversation ids and turn counts (not hardcoded happy paths).
 */
class ConversationStoreTest {

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
  @DisplayName("Transcript preserves append order across reads")
  void preservesOrder() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    int turns = ThreadLocalRandom.current().nextInt(3, 25);
    List<ChatMessage> dialogue = randomDialogue(turns);

    dialogue.forEach(m -> store.append(id, m));

    List<ChatMessage> history = store.history(id);
    assertEquals(turns, history.size());
    assertEquals(turns, store.messageCount(id));
    for (int i = 0; i < turns; i++) {
      assertEquals(dialogue.get(i).getContent(), history.get(i).getContent());
      assertEquals(dialogue.get(i).getRole(), history.get(i).getRole());
    }
  }

  @Test
  @DisplayName("history() returns a snapshot copy that does not mutate the store")
  void historyIsSnapshot() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    store.append(id, ChatMessage.user("hello"));

    List<ChatMessage> snap = store.history(id);
    snap.add(ChatMessage.assistant("injected"));
    snap.clear();

    assertEquals(1, store.messageCount(id), "mutating the snapshot must not affect the store");
  }

  @Test
  @DisplayName("Bounded transcript evicts oldest messages past the cap, keeping the newest")
  void boundedEviction() {
    int cap = ThreadLocalRandom.current().nextInt(5, 15);
    ConversationStore store = new InMemoryConversationStore(cap);
    String id = randomId();
    int total = cap + ThreadLocalRandom.current().nextInt(5, 20);

    List<ChatMessage> dialogue = randomDialogue(total);
    dialogue.forEach(m -> store.append(id, m));

    assertEquals(cap, store.messageCount(id));
    List<ChatMessage> history = store.history(id);
    // The retained window must be the most-recent `cap` messages, in order.
    List<ChatMessage> expectedTail = dialogue.subList(total - cap, total);
    for (int i = 0; i < cap; i++) {
      assertEquals(expectedTail.get(i).getContent(), history.get(i).getContent());
    }
  }

  @Test
  @DisplayName("recent() returns the last N messages oldest-first")
  void recentTail() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    int turns = ThreadLocalRandom.current().nextInt(10, 30);
    List<ChatMessage> dialogue = randomDialogue(turns);
    dialogue.forEach(m -> store.append(id, m));

    int n = ThreadLocalRandom.current().nextInt(1, turns);
    List<ChatMessage> recent = store.recent(id, n);
    assertEquals(n, recent.size());
    for (int i = 0; i < n; i++) {
      assertEquals(dialogue.get(turns - n + i).getContent(), recent.get(i).getContent());
    }
    // Non-positive max returns the full history.
    assertEquals(turns, store.recent(id, 0).size());
  }

  @Test
  @DisplayName("Conversations are isolated by id")
  void isolatedByConversation() {
    ConversationStore store = new InMemoryConversationStore();
    String a = randomId();
    String b = randomId();
    int aTurns = ThreadLocalRandom.current().nextInt(2, 10);
    int bTurns = ThreadLocalRandom.current().nextInt(2, 10);

    randomDialogue(aTurns).forEach(m -> store.append(a, m));
    randomDialogue(bTurns).forEach(m -> store.append(b, m));

    assertEquals(aTurns, store.messageCount(a));
    assertEquals(bTurns, store.messageCount(b));
    assertTrue(store.conversations().contains(a));
    assertTrue(store.conversations().contains(b));
  }

  @Test
  @DisplayName("Workflow attributes round-trip and are isolated from the transcript")
  void attributes() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    String key = "phase";
    String value = "READY_TO_ACT_" + ThreadLocalRandom.current().nextInt(1000);

    assertTrue(store.getAttribute(id, key).isEmpty());
    store.putAttribute(id, key, value);
    assertEquals(value, store.getAttribute(id, key).orElseThrow());
    assertEquals(value, store.attributes(id).get(key));
    // Attributes don't add to the transcript.
    assertEquals(0, store.messageCount(id));
  }

  @Test
  @DisplayName("clear() forgets transcript, attributes, and user association for a conversation")
  void clearForgets() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    String user = "user-" + UUID.randomUUID();
    store.append(id, ChatMessage.user("x"));
    store.putAttribute(id, "phase", "NEW");
    store.associateUser(id, user);

    store.clear(id);

    assertEquals(0, store.messageCount(id));
    assertTrue(store.getAttribute(id, "phase").isEmpty());
    assertFalse(store.conversations().contains(id));
    assertTrue(store.userOf(id).isEmpty());
    assertFalse(store.conversationsForUser(user).contains(id));
  }

  @Test
  @DisplayName("Conversations are retrievable both by id and by owning user")
  void byUserAndByConvoId() {
    ConversationStore store = new InMemoryConversationStore();
    String user = "user-" + UUID.randomUUID();
    int convos = ThreadLocalRandom.current().nextInt(2, 6);
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < convos; i++) {
      String id = randomId();
      ids.add(id);
      store.associateUser(id, user);
      randomDialogue(ThreadLocalRandom.current().nextInt(1, 5)).forEach(m -> store.append(id, m));
    }
    // A conversation owned by a different user must not leak into this user's list.
    String otherId = randomId();
    store.associateUser(otherId, "user-" + UUID.randomUUID());

    List<String> forUser = store.conversationsForUser(user);
    assertEquals(convos, forUser.size());
    assertTrue(forUser.containsAll(ids));
    assertFalse(forUser.contains(otherId));
    // ...and each is still addressable directly by its conversation id.
    for (String id : ids) {
      assertEquals(user, store.userOf(id).orElseThrow());
      assertTrue(store.messageCount(id) > 0);
    }
  }

  @Test
  @DisplayName("Re-associating a conversation moves it to the new user")
  void reassociateMovesConversation() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    String first = "user-" + UUID.randomUUID();
    String second = "user-" + UUID.randomUUID();

    store.associateUser(id, first);
    store.associateUser(id, second);

    assertEquals(second, store.userOf(id).orElseThrow());
    assertFalse(store.conversationsForUser(first).contains(id));
    assertTrue(store.conversationsForUser(second).contains(id));
  }

  @Test
  @DisplayName("Null ids/messages are no-ops, never throw")
  void nullSafety() {
    ConversationStore store = new InMemoryConversationStore();
    store.append(null, ChatMessage.user("x"));
    store.append("id", null);
    store.putAttribute(null, "k", "v");
    assertTrue(store.history(null).isEmpty());
    assertEquals(0, store.messageCount(null));
    assertTrue(store.getAttribute(null, "k").isEmpty());
  }

  @Test
  @DisplayName("Shared singleton resolves to the same instance after serialization")
  void sharedSingletonRoundTrips() throws Exception {
    InMemoryConversationStore shared = InMemoryConversationStore.shared();
    String id = randomId();
    shared.append(id, ChatMessage.user("seed-" + UUID.randomUUID()));

    Object restored = roundTrip(shared);
    assertSame(shared, restored, "the singleton handle must resolve back to the one instance");
    // The restored reference sees the state written before serialization (same backing maps).
    assertEquals(1, ((ConversationStore) restored).messageCount(id));
    shared.clear(id);
  }

  @Test
  @DisplayName("Isolated instance deserializes to a fresh, empty store (no shared backing)")
  void isolatedInstanceRoundTripsEmpty() throws Exception {
    InMemoryConversationStore isolated = new InMemoryConversationStore(50);
    String id = randomId();
    isolated.append(id, ChatMessage.user("only-here"));

    ConversationStore restored = (ConversationStore) roundTrip(isolated);
    assertEquals(0, restored.messageCount(id), "isolated stores do not share backing state");
  }

  @Test
  @DisplayName("ConversationStores.discover() returns the shared in-JVM store by default")
  void discoverDefaultsToShared() {
    assertSame(InMemoryConversationStore.shared(), ConversationStores.discover());
  }

  @Test
  @DisplayName("ChatRole survives a transcript round-trip (tool messages keep their identity)")
  void toolRolePreserved() {
    ConversationStore store = new InMemoryConversationStore();
    String id = randomId();
    String callId = UUID.randomUUID().toString();
    store.append(id, ChatMessage.tool(callId, "kb_search", "result"));

    ChatMessage restored = store.history(id).get(0);
    assertEquals(ChatRole.TOOL, restored.getRole());
    assertEquals(callId, restored.getToolCallId());
    assertEquals("kb_search", restored.getToolName());
  }

  private static Object roundTrip(Object o) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(o);
    }
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }
}
