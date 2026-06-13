package org.agentic.flink.memory.conversation.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Cluster-free unit test of {@link FlussConversationCodec} — the read-modify-write JSON logic that is
 * the correctness core of {@link FlussConversationStore}. Uses randomized data; a real round trip
 * against a Fluss cluster is covered by the integration test.
 */
class FlussConversationCodecTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private static String randomText() {
    return UUID.randomUUID().toString();
  }

  @Test
  @DisplayName("messages round-trip through the envelope with role + tool identity preserved")
  void messagesRoundTrip() {
    String env = FlussConversationCodec.EMPTY_ENVELOPE;
    List<ChatMessage> expected = new ArrayList<>();

    int turns = ThreadLocalRandom.current().nextInt(3, 12);
    for (int i = 0; i < turns; i++) {
      ChatMessage m;
      switch (i % 4) {
        case 0:
          m = ChatMessage.user(randomText());
          break;
        case 1:
          m = ChatMessage.assistant(randomText());
          break;
        case 2:
          m = ChatMessage.system(randomText());
          break;
        default:
          m = ChatMessage.tool(UUID.randomUUID().toString(), "kb_" + randomText(), randomText());
      }
      expected.add(m);
      env = FlussConversationCodec.appendMessage(mapper, env, m, 0);
    }

    List<ChatMessage> got = FlussConversationCodec.messages(mapper, env);
    assertEquals(expected.size(), got.size());
    assertEquals(turns, FlussConversationCodec.messageCount(mapper, env));
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getRole(), got.get(i).getRole());
      assertEquals(expected.get(i).getContent(), got.get(i).getContent());
      if (expected.get(i).getRole() == ChatRole.TOOL) {
        assertEquals(expected.get(i).getToolCallId(), got.get(i).getToolCallId());
        assertEquals(expected.get(i).getToolName(), got.get(i).getToolName());
      }
    }
  }

  @Test
  @DisplayName("appendMessage enforces the maxMessages bound, keeping the most recent")
  void boundedTranscript() {
    String env = FlussConversationCodec.EMPTY_ENVELOPE;
    int cap = 5;
    int total = 20;
    String lastContent = null;
    for (int i = 0; i < total; i++) {
      lastContent = "m-" + i + "-" + randomText();
      env = FlussConversationCodec.appendMessage(mapper, env, ChatMessage.user(lastContent), cap);
    }
    List<ChatMessage> got = FlussConversationCodec.messages(mapper, env);
    assertEquals(cap, got.size(), "transcript must be bounded to cap");
    assertEquals(lastContent, got.get(got.size() - 1).getContent(), "newest message retained");
  }

  @Test
  @DisplayName("attributes set/get/list survive the JSON round trip, independent of messages")
  void attributesRoundTrip() {
    String env = FlussConversationCodec.EMPTY_ENVELOPE;
    env = FlussConversationCodec.appendMessage(mapper, env, ChatMessage.user(randomText()), 0);
    String phase = "phase-" + randomText();
    String ctx = "ctx-" + UUID.randomUUID();
    env = FlussConversationCodec.putAttribute(mapper, env, "banking.phase", phase);
    env = FlussConversationCodec.putAttribute(mapper, env, "a2a.cs.contextId", ctx);

    assertEquals(phase, FlussConversationCodec.getAttribute(mapper, env, "banking.phase").orElseThrow());
    assertEquals(ctx, FlussConversationCodec.getAttribute(mapper, env, "a2a.cs.contextId").orElseThrow());
    assertTrue(FlussConversationCodec.getAttribute(mapper, env, "missing").isEmpty());
    assertEquals(2, FlussConversationCodec.attributes(mapper, env).size());
    // Overwriting an attribute replaces, not duplicates.
    env = FlussConversationCodec.putAttribute(mapper, env, "banking.phase", "verify");
    assertEquals("verify", FlussConversationCodec.getAttribute(mapper, env, "banking.phase").orElseThrow());
    assertEquals(2, FlussConversationCodec.attributes(mapper, env).size());
    // Messages untouched by attribute writes.
    assertEquals(1, FlussConversationCodec.messageCount(mapper, env));
  }

  @Test
  @DisplayName("owner round-trips and does not disturb messages/attributes")
  void ownerRoundTrip() {
    String env = FlussConversationCodec.EMPTY_ENVELOPE;
    env = FlussConversationCodec.putAttribute(mapper, env, "k", "v");
    env = FlussConversationCodec.appendMessage(mapper, env, ChatMessage.assistant(randomText()), 0);
    assertTrue(FlussConversationCodec.owner(mapper, env).isEmpty());
    String user = "user-" + UUID.randomUUID();
    env = FlussConversationCodec.setOwner(mapper, env, user);
    assertEquals(user, FlussConversationCodec.owner(mapper, env).orElseThrow());
    assertEquals("v", FlussConversationCodec.getAttribute(mapper, env, "k").orElseThrow());
    assertEquals(1, FlussConversationCodec.messageCount(mapper, env));
  }

  @Test
  @DisplayName("index rows behave as a set: add is idempotent, remove deletes, order stable")
  void indexSetSemantics() {
    String list = FlussConversationCodec.EMPTY_LIST;
    String a = "conv-" + UUID.randomUUID();
    String b = "conv-" + UUID.randomUUID();
    list = FlussConversationCodec.addToList(mapper, list, a);
    list = FlussConversationCodec.addToList(mapper, list, a); // idempotent
    list = FlussConversationCodec.addToList(mapper, list, b);
    List<String> ids = FlussConversationCodec.decodeList(mapper, list);
    assertEquals(2, ids.size());
    assertTrue(ids.contains(a) && ids.contains(b));

    list = FlussConversationCodec.removeFromList(mapper, list, a);
    ids = FlussConversationCodec.decodeList(mapper, list);
    assertEquals(1, ids.size());
    assertFalse(ids.contains(a));
    assertTrue(ids.contains(b));
  }

  @Test
  @DisplayName("malformed / empty payloads degrade to empty rather than throwing")
  void robustToGarbage() {
    assertTrue(FlussConversationCodec.messages(mapper, null).isEmpty());
    assertTrue(FlussConversationCodec.messages(mapper, "not json").isEmpty());
    assertEquals(0, FlussConversationCodec.messageCount(mapper, ""));
    assertTrue(FlussConversationCodec.attributes(mapper, "{bad").isEmpty());
    assertTrue(FlussConversationCodec.decodeList(mapper, "nope").isEmpty());
    assertTrue(FlussConversationCodec.owner(mapper, null).isEmpty());
  }
}
