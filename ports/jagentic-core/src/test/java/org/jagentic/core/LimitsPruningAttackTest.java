package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The day counters were kept for ever, one per principal, tool and day, so a runtime's memory grew
 * with every principal that ever called. A day that is over is dropped when the next one starts.
 */
class LimitsPruningAttackTest {

  private static final long DAY = 86_400_000L;

  @Test
  void aDayThatIsOverIsDropped() {
    Limits limits = Limits.fromPolicies(Map.of("limits", Map.of(
        "tokens_per_day", 1_000_000, "cost_per_day", 1000, "cost_per_1k_tokens", 0.01,
        "tool_calls_per_day", Map.of("pay", 5))), key -> null);
    for (int i = 0; i < 1000; i++) {
      limits.chargeModel("acme/user" + i, 0, 400);
      assertNull(limits.admitTool("acme/user" + i, 0, "pay", false));
    }
    assertEquals(3000, limits.counters());
    limits.chargeModel("acme/late", 2 * DAY, 400);
    assertNull(limits.admitTool("acme/late", 2 * DAY, "pay", false));
    assertEquals(3, limits.counters(), "only today's counters are kept");
    assertNull(limits.admitModel("acme/user1", 2 * DAY));
  }
}
