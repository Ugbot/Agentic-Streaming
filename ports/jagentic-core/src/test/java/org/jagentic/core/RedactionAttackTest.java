package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Redaction covered what was recorded, and the failure's message in the result was the exception's
 * own. The result is what the gateway answers and what a Flink job writes to its results topic, so a
 * credential in an exception reached both, while the log had it removed.
 */
class RedactionAttackTest {

  @Test
  void aFailedTurnsErrorIsRedactedInTheResultAsOnTheLog() {
    Brain leaky = (text, ctx) -> {
      throw new IllegalStateException("upstream said: Authorization: Bearer abcdefghijklmnop1234 for acct 4111");
    };
    Redactor redactor = Redactor.fromPolicies(Map.of("redact", Map.of("patterns", List.of("acct \\d+"))));
    RoutedGraph graph = new RoutedGraph((e, c) -> "p", Map.of("p", new Agent("p", "p", leaky)), null,
        List.of(), List.of()).withRedactor(redactor);
    AgentContext ctx = new AgentContext("c", "t", "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), new ToolRegistry(), null, new ConversationLog.InMemory(), Policies.DEFAULTS);
    TurnResult r = graph.handle(Event.turn("c", "t", "u", "hi"), ctx);
    assertEquals(TurnStatus.FAILED, r.status);
    assertFalse(ctx.log.events("c").toString().contains("abcdefghijklmnop1234"), "the log was already clean");
    String result = r.toMap().toString();
    assertFalse(result.contains("abcdefghijklmnop1234"), result);
    assertFalse(result.contains("acct 4111"), result);
  }
}
