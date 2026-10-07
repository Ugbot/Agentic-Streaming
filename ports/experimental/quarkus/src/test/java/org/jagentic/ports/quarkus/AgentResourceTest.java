package org.jagentic.ports.quarkus;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.ports.quarkus.AgentMessages.AgentRequest;
import org.junit.jupiter.api.Test;

/** Drives randomized banking turns through the REST edge of the running Quarkus application. */
@QuarkusTest
class AgentResourceTest {

  @Inject
  ConversationStore store;

  static String randomId(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  /** A balance question with arbitrary casing and surrounding words; the router only needs the keyword. */
  static String randomBalanceQuestion() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    String[] prefixes = {"hi, ", "please tell me ", "quick question: ", ""};
    String core = rnd.nextBoolean() ? "what is my balance" : "show me my current BALANCE";
    return prefixes[rnd.nextInt(prefixes.length)] + core + (rnd.nextBoolean() ? "?" : "");
  }

  @Test
  void balanceTurnRoutesToPaymentsAndReturnsTheBalance() {
    String conversationId = randomId("c");
    String userId = randomId("u");
    given()
        .contentType(ContentType.JSON)
        .body(new AgentRequest(conversationId, userId, randomBalanceQuestion()))
        .when().post("/agent")
        .then()
        .statusCode(200)
        .body("conversationId", equalTo(conversationId))
        .body("path", equalTo("payments"))
        .body("ok", is(true))
        .body("reply", startsWith("[payments]"))
        .body("reply", containsString("1234.56"));

    assertEquals(List.of(conversationId), store.conversationsForUser(userId));
  }

  @Test
  void cardTurnRoutesToCards() {
    given()
        .contentType(ContentType.JSON)
        .body(new AgentRequest(randomId("c"), randomId("u"), "which crypto cash-back card do you have"))
        .when().post("/agent")
        .then()
        .statusCode(200)
        .body("path", equalTo("cards"))
        .body("ok", is(true))
        .body("reply", startsWith("[cards]"));
  }

  @Test
  void eachTurnAppendsExactlyOneUserAndOneAssistantMessage() {
    String conversationId = randomId("c");
    String userId = randomId("u");
    int turns = ThreadLocalRandom.current().nextInt(2, 6);
    for (int i = 0; i < turns; i++) {
      String text = randomBalanceQuestion() + " " + UUID.randomUUID();
      String reply = given()
          .contentType(ContentType.JSON)
          .body(new AgentRequest(conversationId, userId, text))
          .when().post("/agent")
          .then().statusCode(200)
          .extract().path("reply");

      List<ChatMessage> history = store.history(conversationId);
      assertEquals(2 * (i + 1), history.size(), "messages after turn " + (i + 1) + ": " + history);
      assertEquals(2 * (i + 1), store.messageCount(conversationId));
      ChatMessage user = history.get(history.size() - 2);
      ChatMessage assistant = history.get(history.size() - 1);
      assertEquals("user", user.role());
      assertEquals(text, user.content());
      assertEquals("assistant", assistant.role());
      assertEquals(reply, assistant.content());
    }
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
