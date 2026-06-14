package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import org.jagentic.core.llm.ChatResult;
import org.jagentic.core.llm.LlmBrain;
import org.jagentic.core.llm.StubChatClient;
import org.jagentic.core.skill.Skill;
import org.jagentic.core.skill.SkillRegistry;
import org.jagentic.core.structured.Structured;

/** Phase B feature tests: skills, structured output, richer listeners. */
class FeaturesTest {

  private AgentContext ctx(ToolRegistry tools) {
    return new AgentContext("c1", "alice", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null);
  }

  // ---- skills ----

  @Test
  void skillRegistryExpands() {
    SkillRegistry reg = new SkillRegistry().register(
        new Skill("billing", List.of("get_balance", "refund"), "Be precise about amounts.", List.of("account")));
    SkillRegistry.Expanded e = reg.expand(List.of("billing"));
    assertEquals(List.of("get_balance", "refund"), e.tools());
    assertTrue(e.promptFragment().contains("precise"));
    assertEquals(List.of("account"), e.facts());
  }

  // ---- structured output ----

  @Test
  void validateRequiredAndTypes() {
    Map<String, Object> schema = Map.of("type", "object", "required", List.of("category", "amount"),
        "properties", Map.of("category", Map.of("type", "string"), "amount", Map.of("type", "number")));
    assertTrue(Structured.validate(Map.of("category", "refund", "amount", 42.0), schema).isEmpty());
    assertTrue(Structured.validate(Map.of("category", "refund"), schema).stream().anyMatch(s -> s.contains("amount")));
    assertEquals(2, Structured.validate(Map.of("category", 1, "amount", "x"), schema).size());
  }

  @Test
  void parseStructuredToleratesProse() {
    Map<String, Object> schema = Map.of("type", "object", "required", List.of("ok"),
        "properties", Map.of("ok", Map.of("type", "boolean")));
    Structured.Result r = Structured.parse("here you go: {\"ok\": true} done", schema);
    assertTrue(r.ok());
    assertEquals(Boolean.TRUE, r.value().get("ok"));
  }

  @Test
  void llmBrainOutputSchemaReturnsValidatedJson() {
    Map<String, Object> schema = Map.of("type", "object", "required", List.of("category"),
        "properties", Map.of("category", Map.of("type", "string")));
    LlmBrain brain = new LlmBrain(new StubChatClient(List.of(ChatResult.text("{\"category\": \"refund\"}"))), "triage")
        .withOutputSchema(schema);
    TurnResult res = new Agent("triage", "p", brain).turn(new Event("c1", "u", "refund please"), ctx(new ToolRegistry()));
    assertEquals("[triage] {\"category\":\"refund\"}", res.reply);
  }

  // ---- richer listeners ----

  @Test
  void toolCallAndCompositeListenerHooksFire() {
    MetricsListener m1 = new MetricsListener();
    MetricsListener m2 = new MetricsListener();
    CompositeListener composite = new CompositeListener(m1, m2);
    ToolRegistry tools = new ToolRegistry().register("get_balance", "balance", p -> 1234.56);
    Agent pay = new Agent("payments", "p", new LlmBrain(new StubChatClient(List.of(
        ChatResult.toolCall("get_balance", Map.of()), ChatResult.text("done"))), "payments"));
    RoutedGraph graph = new RoutedGraph((ev, c) -> "payments", Map.of("payments", pay), null,
        List.of(), List.of(composite));
    graph.handle(new Event("c1", "u", "balance?"), ctx(tools));
    for (MetricsListener m : List.of(m1, m2)) {
      assertEquals(1, m.turns.get());
      assertEquals(1, m.toolCalls.get());
      assertEquals(1, m.pathCount("payments"));
    }
  }

  @Test
  void listenerErrorHookOnToolFailure() {
    MetricsListener m = new MetricsListener();
    ToolRegistry tools = new ToolRegistry().register("boom", "fails", p -> { throw new RuntimeException("boom"); });
    Agent pay = new Agent("payments", "p", new LlmBrain(new StubChatClient(List.of(
        ChatResult.toolCall("boom", Map.of()), ChatResult.text("x"))), "payments"));
    RoutedGraph graph = new RoutedGraph((ev, c) -> "payments", Map.of("payments", pay), null,
        List.of(), List.of(m));
    try {
      graph.handle(new Event("c1", "u", "go"), ctx(tools));
    } catch (RuntimeException ignore) {
    }
    assertEquals(1, m.errors.get());
  }
}
