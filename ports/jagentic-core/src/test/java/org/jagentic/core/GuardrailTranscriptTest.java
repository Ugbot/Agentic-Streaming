package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/**
 * A reply blocked by an output guardrail must never reach the transcript: neither the {@link
 * ConversationStore} the next turn's brain reads its history from, nor the folded {@link
 * ConversationState#transcript()}. The turn's own reply is the redacted {@code [blocked] ...} text.
 */
class GuardrailTranscriptTest {

  /** Replies with the secret on the first turn and records the history each turn sees. */
  static final class LeakyBrain implements Brain {
    final String secret;
    final List<List<ChatMessage>> historySeen = new ArrayList<>();
    int turns;

    LeakyBrain(String secret) {
      this.secret = secret;
    }

    @Override
    public String turn(String userText, AgentContext ctx) {
      historySeen.add(ctx.store.history(ctx.conversationId));
      turns++;
      return turns == 1 ? "your account number is " + secret : "[general] all good";
    }
  }

  @Test
  void blockedReplyIsNotPersistedAndNextPromptSeesRedactedHistoryOnly() {
    String secret = String.valueOf(ThreadLocalRandom.current().nextInt(1000, 10_000));
    String reason = "leaked account number " + UUID.randomUUID();
    LeakyBrain brain = new LeakyBrain(secret);
    RoutedGraph graph = new RoutedGraph((e, c) -> "general",
        Map.of("general", new Agent("general", "g", brain)),
        (reply, ctx) -> new RoutedGraph.Verifier.Result(true, reply),
        List.of(new RegexGuardrail(List.of("\\d{4}"), reason, true)), List.of());
    ConversationStore store = new ConversationStore.InMemory();
    LocalRuntime rt = new LocalRuntime(graph, store, new KeyedStateStore.InMemory(),
        Banking.defaultTools(), Banking.retriever());
    String cid = "c-" + UUID.randomUUID();

    TurnResult first = rt.submit(new Event(cid, "u", "what is my account number?"));
    assertFalse(first.ok);
    assertEquals(TurnStatus.REJECTED, first.status);
    assertEquals("[blocked] " + reason, first.reply);
    assertFalse(first.reply.contains(secret));

    List<ChatMessage> history = store.history(cid);
    assertTrue(history.isEmpty(), () -> "blocked turn leaked into the store: " + history);
    ConversationState state = rt.replay(cid);
    assertTrue(state.transcript().isEmpty(), () -> "blocked turn leaked into the fold: " + state);
    assertEquals("[blocked] " + reason, state.turns().get(first.turnId).reply());

    TurnResult second = rt.submit(new Event(cid, "u", "thanks"));
    assertTrue(second.ok);
    assertEquals(2, brain.historySeen.size());
    for (ChatMessage m : brain.historySeen.get(1)) {
      assertFalse(m.content().contains(secret), () -> "next prompt replayed raw blocked text: " + m);
    }
    for (ChatMessage m : store.history(cid)) {
      assertFalse(m.content().contains(secret), () -> "store still holds the raw reply: " + m);
    }
    assertEquals(2, store.history(cid).size());
  }
}
