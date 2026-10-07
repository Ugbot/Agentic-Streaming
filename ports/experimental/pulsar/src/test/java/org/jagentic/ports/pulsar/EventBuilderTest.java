package org.jagentic.ports.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;

/**
 * The core {@code Event} constructor takes {@code userId} and {@code text} as adjacent
 * positional strings. These tests pin the adapter's named builder to the right fields
 * with arbitrary values, so a swapped argument can never pass unnoticed.
 */
class EventBuilderTest {

  private static String random(String prefix) {
    return prefix + "-" + UUID.randomUUID() + "-" + ThreadLocalRandom.current().nextInt(1_000_000);
  }

  @RepeatedTest(5)
  void namedFieldsLandOnTheMatchingAccessors() {
    String cid = random("conversation");
    String userId = random("user");
    String text = random("text");
    assertNotEquals(userId, text);

    Event event = EventBuilder.turn().conversationId(cid).userId(userId).text(text).build();

    assertEquals(cid, event.conversationId());
    assertEquals(userId, event.userId());
    assertEquals(text, event.text());
    assertNotNull(event.turnId());
  }

  @Test
  void explicitTurnIdIsPreserved() {
    String turnId = random("turn");
    Event event = EventBuilder.turn()
        .conversationId(random("c")).userId(random("u")).text(random("t")).turnId(turnId).build();
    assertEquals(turnId, event.turnId());
  }

  @Test
  void missingRequiredFieldsAreRejected() {
    assertThrows(NullPointerException.class,
        () -> EventBuilder.turn().userId(random("u")).text(random("t")).build());
    assertThrows(NullPointerException.class,
        () -> EventBuilder.turn().conversationId(random("c")).text(random("t")).build());
    assertThrows(NullPointerException.class,
        () -> EventBuilder.turn().conversationId(random("c")).userId(random("u")).build());
  }
}
