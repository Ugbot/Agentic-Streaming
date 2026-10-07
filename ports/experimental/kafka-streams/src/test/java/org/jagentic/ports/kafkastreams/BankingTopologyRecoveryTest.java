package org.jagentic.ports.kafkastreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.jagentic.core.ChatMessage;

/**
 * The conversation transcript is a persistent, changelog-backed Kafka Streams state store, not a
 * heap map. {@link TopologyTestDriver#close()} wipes the local state directory, so recovery is
 * exercised the way a real instance recovers: the topology is closed, a new driver is opened
 * with empty local state, and the changelog records the first instance produced are replayed
 * into the store before the next turn is processed.
 */
class BankingTopologyRecoveryTest {

  private static final String APP_ID = "agentic-ks-recovery-" + UUID.randomUUID();
  private static final String CHANGELOG =
      APP_ID + "-" + BankingTopology.CONVERSATION_STORE + "-changelog";

  private static final String[] TURNS = {
      "what is my balance?", "show my balance please", "did my payment go through?",
      "I lost my card", "can you block my card", "hello there"
  };

  private static Properties props(Path stateDir) {
    Properties p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    p.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    return p;
  }

  private static TestInputTopic<String, String> requests(TopologyTestDriver driver) {
    return driver.createInputTopic(
        BankingTopology.REQUESTS_TOPIC, new StringSerializer(), new StringSerializer());
  }

  private static TestOutputTopic<String, String> responses(TopologyTestDriver driver) {
    return driver.createOutputTopic(
        BankingTopology.RESPONSES_TOPIC, new StringDeserializer(), new StringDeserializer());
  }

  private static KeyValueConversationStore transcript(TopologyTestDriver driver) {
    KeyValueStore<String, String> kv = driver.getKeyValueStore(BankingTopology.CONVERSATION_STORE);
    return new KeyValueConversationStore(kv);
  }

  private static TestRecord<String, String> turn(String cid, String userId, String text) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(BankingTopology.USER_ID_HEADER, userId.getBytes(StandardCharsets.UTF_8));
    return new TestRecord<>(cid, text, headers);
  }

  private static List<String> randomTurns() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    int n = rnd.nextInt(2, 6);
    List<String> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(TURNS[rnd.nextInt(TURNS.length)] + " " + UUID.randomUUID());
    }
    return out;
  }

  private static void assertTranscriptMatches(List<String> turns, List<ChatMessage> history) {
    assertEquals(2 * turns.size(), history.size(), history.toString());
    for (int i = 0; i < turns.size(); i++) {
      ChatMessage user = history.get(2 * i);
      ChatMessage assistant = history.get(2 * i + 1);
      assertEquals("user", user.role());
      assertEquals(turns.get(i), user.content());
      assertEquals("assistant", assistant.role());
      assertTrue(assistant.content().startsWith("["), assistant.content());
    }
  }

  @Test
  void transcriptSurvivesCloseAndReopenViaTheChangelog(@TempDir Path firstDir, @TempDir Path secondDir) {
    String cid = "c-" + UUID.randomUUID();
    String userId = "u-" + UUID.randomUUID();
    List<String> turns = randomTurns();
    List<TestRecord<String, String>> changelog;

    try (TopologyTestDriver first = new TopologyTestDriver(BankingTopology.build(), props(firstDir))) {
      TestInputTopic<String, String> in = requests(first);
      TestOutputTopic<String, String> out = responses(first);
      for (String text : turns) {
        in.pipeInput(turn(cid, userId, text));
        out.readValue();
      }
      changelog = first
          .createOutputTopic(CHANGELOG, new StringDeserializer(), new StringDeserializer())
          .readRecordsToList();
    }
    assertFalse(changelog.isEmpty(), "the conversation store must write a changelog");

    try (TopologyTestDriver second = new TopologyTestDriver(BankingTopology.build(), props(secondDir))) {
      KeyValueStore<String, String> kv = second.getKeyValueStore(BankingTopology.CONVERSATION_STORE);
      assertEquals(0, new KeyValueConversationStore(kv).messageCount(cid), "fresh instance starts empty");

      for (TestRecord<String, String> record : changelog) {
        if (record.value() == null) {
          kv.delete(record.key());
        } else {
          kv.put(record.key(), record.value());
        }
      }

      KeyValueConversationStore store = new KeyValueConversationStore(kv);
      assertTranscriptMatches(turns, store.history(cid));
      assertEquals(List.of(cid), store.conversationsForUser(userId));

      String next = "what is my balance after all that? " + UUID.randomUUID();
      requests(second).pipeInput(turn(cid, userId, next));
      assertTrue(responses(second).readValue().contains("1234.56"));
      assertEquals(2 * turns.size() + 2, store.messageCount(cid));
    }
  }

  @Test
  void userIdComesFromTheHeaderAndFallsBackToTheKey(@TempDir Path stateDir) {
    String cid = "c-" + UUID.randomUUID();
    String userId = "u-" + UUID.randomUUID();
    try (TopologyTestDriver driver = new TopologyTestDriver(BankingTopology.build(), props(stateDir))) {
      TestInputTopic<String, String> in = requests(driver);
      TestOutputTopic<String, String> out = responses(driver);

      in.pipeInput(turn(cid, userId, "what is my balance?"));
      out.readValue();
      String other = "c-" + UUID.randomUUID();
      in.pipeInput(other, "what is my balance?");
      out.readValue();

      KeyValueConversationStore store = transcript(driver);
      assertEquals(List.of(cid), store.conversationsForUser(userId));
      assertEquals(List.of(other), store.conversationsForUser("user-" + other));
    }
  }
}
