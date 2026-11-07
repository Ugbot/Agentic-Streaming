package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What the fixtures cannot show: a limit follows the principal, and the operator's switch. */
class LimitsTest {

  private static Map<String, Object> workflow(Map<String, Object> limits) {
    return Map.of("spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
            "paths", Map.of("main", Map.of("brain", "rule")), "verifier", Map.of("kind", "none")),
        "policies", Map.of("limits", limits));
  }

  private static LocalRuntime runtime(Map<String, Object> limits) {
    GraphBuilder.Built built = GraphBuilder.build(workflow(limits), null);
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new ConversationLog.InMemory());
  }

  @Test
  void aRateFollowsThePrincipalAcrossConversationsAndTenantsDoNotShareOne() {
    LocalRuntime rt = runtime(Map.of("turns_per_minute", 1));
    Principal alice = Principal.of("alice", "acme");
    assertEquals(TurnStatus.COMPLETED, rt.submit(Event.turn("c1", "t", "", "hi").withPrincipal(alice)).status);
    TurnResult second = rt.submit(Event.turn("c2", "t", "", "hi").withPrincipal(alice));
    assertEquals(TurnStatus.FAILED, second.status);
    assertEquals(TurnError.ErrorClass.POLICY, second.error.errorClass());
    assertEquals(TurnStatus.COMPLETED,
        rt.submit(Event.turn("c3", "t", "", "hi").withPrincipal(Principal.of("alice", "beta"))).status);
  }

  @Test
  void theOperatorsSwitchWorksWhateverTheDocumentSays(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("stop");
    Map<String, Object> policies = Map.of("limits", Map.of());
    Limits limits = Limits.fromPolicies(policies, k -> k.equals("AGENTIC_KILL_SWITCH_FILE") ? file.toString() : null);
    assertFalse(limits.stopped());
    assertNull(limits.admitTurn("a", "t", 0));
    Files.writeString(file, "");
    assertTrue(limits.stopped(), "no restart needed");
    assertEquals("kill_switch", limits.admitTurn("a", "t", 0).limit());
    assertEquals("kill_switch", limits.admitModel("a", 0).limit());
    assertEquals("kill_switch", limits.admitTool("a", 0, "pay", false).limit());
    assertTrue(Limits.fromPolicies(policies, k -> k.equals("AGENTIC_KILL_SWITCH") ? "true" : null).stopped());
  }

  @Test
  void aCallAlreadyBegunDoesNotCountAgainstTheQuota() {
    Limits limits = Limits.fromPolicies(Map.of("limits", Map.of("tool_calls_per_day", Map.of("pay", 1))), k -> null);
    assertNull(limits.admitTool("a", 0, "pay", false));
    assertNull(limits.admitTool("a", 0, "pay", true));
    assertNotNull(limits.admitTool("a", 0, "pay", false));
    assertNull(limits.admitTool("a", 86_400_000, "pay", false), "a new day");
    assertNull(limits.admitTool("b", 0, "pay", false), "another principal");
    assertNull(limits.admitTool("a", 0, "look", false), "another tool");
  }

  @Test
  void aBudgetIsSpentByTheCallThatCrossesIt() {
    Limits limits = Limits.fromPolicies(Map.of("limits", Map.of("tokens_per_day", 10)), k -> null);
    assertNull(limits.admitModel("a", 0));
    limits.chargeModel("a", 0, 1000);
    assertEquals("tokens_per_day", limits.admitModel("a", 0).limit());
    assertNull(limits.admitModel("b", 0));
    assertEquals(List.of("max"), List.copyOf(limits.admitModel("a", 0).detail().keySet()));
  }
}
