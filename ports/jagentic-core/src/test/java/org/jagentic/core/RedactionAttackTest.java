package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Redaction covered what was recorded, and the failure's message in the result was the exception's
 * own. The result is what the gateway answers and what a Flink job writes to its results topic, so a
 * credential in an exception reached both, while the log had it removed.
 */
class RedactionAttackTest {

  /**
   * Credential fields were matched by their whole name, so {@code access_token}, {@code
   * client_secret}, {@code x-api-key} and {@code apiKey}'s cousins went to the log as they were, and
   * the shapes knew OpenAI, AWS and JWTs only.
   */
  @Test
  void theCommonNamesAndShapesOfACredentialAreRedacted() {
    Map<String, Object> args = new java.util.LinkedHashMap<>();
    args.put("access_token", "opaque-0123456789");
    args.put("refreshToken", "opaque-1123456789");
    args.put("client_secret", "opaque-2123456789");
    args.put("x-api-key", "opaque-3123456789");
    args.put("AWS_SECRET_ACCESS_KEY", "opaque-4123456789");
    args.put("db_password", "opaque-5123456789");
    args.put("Set-Cookie", "opaque-6123456789");
    args.put("note", "ghp_abcdefghijklmnopqrstuvwxyz0123456789 xoxb-1234567890-abcdefghij "
        + "sk_live_abcdefghijklmnop0123 AIzaSyA-abcdefghijklmnopqrstuvwxyz012345");
    args.put("max_tokens", 100);
    args.put("tokens_per_day", 7);
    String out = Redactor.DEFAULT.apply(args).toString();
    for (String leaked : List.of("opaque-", "ghp_", "xoxb-", "sk_live_", "AIzaSy")) {
      assertFalse(out.contains(leaked), leaked + " in " + out);
    }
    assertTrue(out.contains("max_tokens=100") && out.contains("tokens_per_day=7"), "a count is not a credential: " + out);
  }

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
