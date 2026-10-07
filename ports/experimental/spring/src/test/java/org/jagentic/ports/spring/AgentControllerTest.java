package org.jagentic.ports.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;
import org.jagentic.ports.spring.AgentController.AgentRequest;
import org.jagentic.ports.spring.AgentController.AgentResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.messaging.MessageChannel;

/**
 * Drives banking turns through the Spring adapter: the REST edge ({@code POST /agent}) and
 * the Spring Integration topology ({@link RoutedFlow}), over the real application context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AgentControllerTest {

  @Autowired
  private TestRestTemplate rest;

  @Autowired
  private ConversationStore store;

  @Autowired
  private KeyedStateStore state;

  @Autowired
  private ToolRegistry tools;

  @Autowired
  private Retrieval.TwoTierRetriever retriever;

  @Autowired
  private MessageChannel requestsIn;

  private static String randomId(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  /** A balance question with arbitrary casing and surrounding words; the router only needs the keyword. */
  private static String randomBalanceQuestion() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    String[] prefixes = {"hi, ", "please tell me ", "quick question: ", ""};
    String core = rnd.nextBoolean() ? "what is my balance" : "show me my current BALANCE";
    return prefixes[rnd.nextInt(prefixes.length)] + core + (rnd.nextBoolean() ? "?" : "");
  }

  private AgentResponse post(AgentRequest request) {
    AgentResponse response = rest.postForObject("/agent", request, AgentResponse.class);
    assertNotNull(response, "controller returned no body");
    return response;
  }

  @Test
  void balanceTurnRoutesToPaymentsAndCallsGetBalance() {
    String conversationId = randomId("c");
    String userId = randomId("u");
    AgentResponse response = post(new AgentRequest(conversationId, userId, randomBalanceQuestion()));

    assertEquals(conversationId, response.conversationId());
    assertEquals("payments", response.path());
    assertTrue(response.ok(), response.toString());
    assertEquals(List.of("get_balance"), response.toolCalls());
    assertTrue(response.reply().startsWith("[payments]"), response.reply());
    assertTrue(response.reply().contains("1234.56"), response.reply());
    assertEquals(List.of(conversationId), store.conversationsForUser(userId));
  }

  @Test
  void cardTurnRoutesToCardsAndAnswersFromRetrieval() {
    AgentResponse response = post(new AgentRequest(randomId("c"), randomId("u"),
        "what crypto cash-back do your cards offer"));
    assertEquals("cards", response.path());
    assertTrue(response.toolCalls().isEmpty(), response.toString());
    assertTrue(response.reply().startsWith("[cards]"), response.reply());
  }

  @Test
  void eachTurnAppendsExactlyOneUserAndOneAssistantMessage() {
    String conversationId = randomId("c");
    String userId = randomId("u");
    int turns = ThreadLocalRandom.current().nextInt(2, 6);
    for (int i = 0; i < turns; i++) {
      String text = randomBalanceQuestion() + " " + UUID.randomUUID();
      AgentResponse response = post(new AgentRequest(conversationId, userId, text));

      List<ChatMessage> history = store.history(conversationId);
      assertEquals(2 * (i + 1), history.size(), "messages after turn " + (i + 1) + ": " + history);
      assertEquals(2 * (i + 1), store.messageCount(conversationId));
      ChatMessage user = history.get(history.size() - 2);
      ChatMessage assistant = history.get(history.size() - 1);
      assertEquals("user", user.role());
      assertEquals(text, user.content());
      assertEquals("assistant", assistant.role());
      assertEquals(response.reply(), assistant.content());
    }
    long userMessages = store.history(conversationId).stream().filter(m -> "user".equals(m.role())).count();
    assertEquals(turns, userMessages);
  }

  @Test
  void integrationFlowRoutesThroughPathChannelToVerifier() {
    String conversationId = randomId("c");
    String userId = randomId("u");
    Event event = EventBuilder.turn()
        .conversationId(conversationId)
        .userId(userId)
        .text(randomBalanceQuestion())
        .build();
    RoutedFlow.TurnRequest request = RoutedFlow.turnRequest(event, store, state, tools, retriever);

    MessagingTemplate template = new MessagingTemplate();
    TurnResult result = template.convertSendAndReceive(requestsIn, request, TurnResult.class);

    assertNotNull(result, "verify flow produced no result");
    assertEquals(conversationId, result.conversationId);
    assertEquals("payments", result.path);
    assertTrue(result.ok);
    assertEquals(List.of("get_balance"), result.toolCalls);
    assertEquals(2, store.messageCount(conversationId));
  }

  @Test
  void eventBuilderKeepsUserIdAndTextInTheirOwnFields() {
    String conversationId = randomId("c");
    String userId = randomId("u");
    String text = "text-" + UUID.randomUUID();
    String turnId = randomId("t");
    Event event = EventBuilder.turn()
        .conversationId(conversationId)
        .turnId(turnId)
        .userId(userId)
        .text(text)
        .metadata(Map.of("event_time_ms", "42"))
        .build();

    assertEquals(conversationId, event.conversationId());
    assertEquals(turnId, event.turnId());
    assertEquals(userId, event.userId());
    assertEquals(text, event.text());
    assertEquals("42", event.metadata().get("event_time_ms"));

    Event minted = EventBuilder.turn().conversationId(conversationId).userId(userId).text(text).build();
    assertNotNull(minted.turnId());
    assertTrue(minted.metadata().isEmpty());
  }
}
