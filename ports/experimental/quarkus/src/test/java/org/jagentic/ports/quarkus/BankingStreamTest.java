package org.jagentic.ports.quarkus;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
import io.smallrye.reactive.messaging.memory.InMemorySource;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;
import org.jagentic.ports.quarkus.AgentMessages.AgentReply;
import org.jagentic.ports.quarkus.AgentMessages.AgentRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the streaming edge: turn requests are pushed into the {@code requests} channel and the
 * verified replies are read from the {@code replies} channel. The {@code %test} profile binds both
 * channels to the in-memory connector, so the same {@link BankingStream} bean runs without Kafka.
 */
@QuarkusTest
class BankingStreamTest {

  @Inject
  @Connector("smallrye-in-memory")
  InMemoryConnector connector;

  @Inject
  ConversationStore store;

  private InMemorySource<AgentRequest> requests;
  private InMemorySink<AgentReply> replies;

  @BeforeEach
  void channels() {
    requests = connector.source("requests");
    replies = connector.sink("replies");
    replies.clear();
  }

  private List<AgentReply> received() {
    List<AgentReply> out = new ArrayList<>();
    for (Message<AgentReply> m : replies.received()) {
      out.add(m.getPayload());
    }
    return out;
  }

  @Test
  void balanceRequestProducesAPaymentsReplyAndOneTranscriptPair() {
    String conversationId = AgentResourceTest.randomId("c");
    String userId = AgentResourceTest.randomId("u");
    String text = AgentResourceTest.randomBalanceQuestion();

    requests.send(new AgentRequest(conversationId, userId, text));

    await().atMost(Duration.ofSeconds(10)).until(() -> replies.received().size() == 1);
    AgentReply reply = received().get(0);
    assertEquals(conversationId, reply.conversationId());
    assertEquals("payments", reply.path());
    assertTrue(reply.ok(), reply.toString());
    assertTrue(reply.reply().contains("1234.56"), reply.reply());

    List<ChatMessage> history = store.history(conversationId);
    assertEquals(List.of("user", "assistant"), history.stream().map(ChatMessage::role).toList());
    assertEquals(text, history.get(0).content());
    assertEquals(reply.reply(), history.get(1).content());
    assertEquals(List.of(conversationId), store.conversationsForUser(userId));
  }

  @Test
  void repliesFollowRequestsPerConversation() {
    String conversationId = AgentResourceTest.randomId("c");
    String userId = AgentResourceTest.randomId("u");
    int turns = ThreadLocalRandom.current().nextInt(8, 17);
    List<String> texts = new ArrayList<>();
    for (int i = 0; i < turns; i++) {
      String text = (i % 2 == 0 ? "raise my transfer limit " : "tell me about your cards ") + UUID.randomUUID();
      texts.add(text);
      requests.send(new AgentRequest(conversationId, userId, text));
    }

    await().atMost(Duration.ofSeconds(10)).until(() -> replies.received().size() == turns);
    List<AgentReply> out = received();
    for (int i = 0; i < turns; i++) {
      assertEquals(conversationId, out.get(i).conversationId());
      assertEquals(i % 2 == 0 ? "payments" : "cards", out.get(i).path(), out.get(i).toString());
      assertTrue(out.get(i).ok());
    }
    List<ChatMessage> history = store.history(conversationId);
    assertEquals(2 * turns, history.size(), history.toString());
    assertEquals(2 * turns, store.messageCount(conversationId));
    for (int i = 0; i < turns; i++) {
      assertEquals(texts.get(i), history.get(2 * i).content());
      assertEquals("assistant", history.get(2 * i + 1).role());
    }
  }
}
