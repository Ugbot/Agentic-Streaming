package org.jagentic.pekko.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.pekko.entity.ConversationEntity.TurnReply;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoSystem;

/** Broker-free proof of the streaming core: a Source of Events through the backpressured
 * ask-the-entity flow yields ordered TurnReplies. (The Kafka binding in KafkaStreamApp wraps this
 * same flow with a committable source/sink; its live round-trip is an opt-in integration test.) */
class AgentStreamTest {

  @Test
  void streamsEventsThroughEntitiesPreservingOrder() throws Exception {
    PekkoSystem sys = new PekkoSystem(AgentDeps.banking());
    try {
      List<Event> events = List.of(
          new Event("c1", "u", "what is my balance?"),
          new Event("c2", "u", "tell me about crypto cash-back"),
          new Event("c3", "u", "hello there"));

      List<TurnReply> replies = Source.from(events)
          .via(AgentStream.flow(sys.system(), 4, Duration.ofSeconds(10)))
          .runWith(Sink.seq(), sys.system())
          .toCompletableFuture().get(30, TimeUnit.SECONDS);

      assertEquals(3, replies.size());
      // mapAsync preserves order
      assertEquals("payments", replies.get(0).path());
      assertTrue(replies.get(0).reply().contains("1234.56"), replies.get(0).reply());
      assertEquals("cards", replies.get(1).path());
      assertEquals("general", replies.get(2).path());
    } finally {
      sys.close();
    }
  }
}
