package org.jagentic.ports.kafkastreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.jagentic.core.ChatMessage;

/** Exercises the store layout against the real persistent store the topology registers. */
class KeyValueConversationStoreTest {

  private TopologyTestDriver driver;
  private KeyValueStore<String, String> kv;

  @BeforeEach
  void open(@TempDir Path stateDir) {
    Properties p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "agentic-ks-store-" + UUID.randomUUID());
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    driver = new TopologyTestDriver(BankingTopology.build(), p);
    kv = driver.getKeyValueStore(BankingTopology.CONVERSATION_STORE);
  }

  @AfterEach
  void close() {
    driver.close();
  }

  private static String randomText() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    StringBuilder sb = new StringBuilder();
    int n = rnd.nextInt(0, 40);
    for (int i = 0; i < n; i++) {
      sb.append((char) rnd.nextInt(1, 0x2FFF));
    }
    return sb.toString();
  }

  @Test
  void messagesRoundTripArbitraryContentInAppendOrder() {
    KeyValueConversationStore store = new KeyValueConversationStore(kv);
    String cid = "c-" + UUID.randomUUID();
    List<ChatMessage> expected = new ArrayList<>();
    int n = ThreadLocalRandom.current().nextInt(3, 12);
    for (int i = 0; i < n; i++) {
      ChatMessage m = switch (i % 4) {
        case 0 -> ChatMessage.user(randomText() + ":" + randomText());
        case 1 -> ChatMessage.assistant(randomText() + "\n" + randomText());
        case 2 -> ChatMessage.tool("call-" + i, "get_balance", randomText());
        default -> ChatMessage.system(randomText() + '\u0000' + randomText());
      };
      expected.add(m);
      store.append(cid, m);
    }
    assertEquals(expected, store.history(cid));
    assertEquals(n, store.messageCount(cid));
    assertEquals(List.of(), store.history("c-" + UUID.randomUUID()));
  }

  @Test
  void historyIsBoundedToTheNewestMessages() {
    int max = ThreadLocalRandom.current().nextInt(2, 6);
    KeyValueConversationStore store = new KeyValueConversationStore(kv, max);
    String cid = "c-" + UUID.randomUUID();
    int total = max + ThreadLocalRandom.current().nextInt(1, 8);
    for (int i = 0; i < total; i++) {
      store.append(cid, ChatMessage.user("m" + i));
    }
    List<ChatMessage> history = store.history(cid);
    assertEquals(max, history.size());
    assertEquals(max, store.messageCount(cid));
    assertEquals("m" + (total - max), history.get(0).content());
    assertEquals("m" + (total - 1), history.get(max - 1).content());
  }

  @Test
  void attributesAndUserIndexAreKeyedPerConversation() {
    KeyValueConversationStore store = new KeyValueConversationStore(kv);
    String cid = "c-" + UUID.randomUUID();
    String sibling = cid + "-2";
    String user = "u-" + UUID.randomUUID();
    String other = "u-" + UUID.randomUUID();

    store.putAttribute(cid, "stage", "verify");
    store.putAttribute(cid, "route", "cards");
    store.putAttribute(sibling, "stage", "other");
    assertEquals(Optional.of("verify"), store.getAttribute(cid, "stage"));
    assertEquals(Optional.empty(), store.getAttribute(cid, "missing"));
    assertEquals(Map.of("stage", "verify", "route", "cards"), store.attributes(cid));
    assertEquals(Map.of("stage", "other"), store.attributes(sibling));

    store.associateUser(cid, user);
    store.associateUser(sibling, user);
    assertEquals(List.of(cid, sibling), store.conversationsForUser(user));
    store.associateUser(sibling, other);
    assertEquals(List.of(cid), store.conversationsForUser(user));
    assertEquals(List.of(sibling), store.conversationsForUser(other));
  }

  @Test
  void clearRemovesOnlyThatConversation() {
    KeyValueConversationStore store = new KeyValueConversationStore(kv);
    String cid = "c-" + UUID.randomUUID();
    String sibling = cid + "-2";
    String user = "u-" + UUID.randomUUID();
    for (String c : List.of(cid, sibling)) {
      store.append(c, ChatMessage.user(randomText()));
      store.putAttribute(c, "k", "v");
      store.associateUser(c, user);
    }

    store.clear(cid);

    assertEquals(List.of(), store.history(cid));
    assertEquals(0, store.messageCount(cid));
    assertEquals(Map.of(), store.attributes(cid));
    assertEquals(List.of(sibling), store.conversationsForUser(user));
    assertEquals(1, store.messageCount(sibling));
    assertEquals(Map.of("k", "v"), store.attributes(sibling));
  }

  @Test
  void identifiersMustNotContainTheSeparator() {
    KeyValueConversationStore store = new KeyValueConversationStore(kv);
    String bad = "c" + '\u0000' + UUID.randomUUID();
    assertThrows(IllegalArgumentException.class, () -> store.append(bad, ChatMessage.user("x")));
    assertThrows(IllegalArgumentException.class, () -> store.putAttribute("c", bad, "v"));
    assertThrows(IllegalArgumentException.class, () -> store.associateUser("c", bad));
  }

  @Test
  void scalarStateClearRemovesOnlyThatKeysSlots() {
    KeyValueStore<String, String> scalars = driver.getKeyValueStore(BankingTopology.STATE_STORE);
    BankingTopology.KeyValueBackedState state = new BankingTopology.KeyValueBackedState(scalars);
    String key = "k-" + UUID.randomUUID();
    String other = key + "x";
    state.put(key, "a", "1");
    state.put(key, "b", "2");
    state.put(other, "a", "3");

    state.clear(key);

    assertTrue(state.get(key, "a").isEmpty());
    assertTrue(state.get(key, "b").isEmpty());
    assertEquals(Optional.of("3"), state.get(other, "a"));
  }
}
