package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import org.jagentic.core.llm.ChatResult;
import org.jagentic.core.llm.LlmBrain;
import org.jagentic.core.llm.StubChatClient;

/** LLM brain feature tests (offline, deterministic via StubChatClient). */
class LlmBrainTest {

  private AgentContext ctx(ToolRegistry tools) {
    return new AgentContext("c1", "alice", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null);
  }

  @Test
  void runsReactToolThenFinal() {
    ToolRegistry tools = new ToolRegistry().register("get_balance", "Look up balance", p -> 1234.56);
    StubChatClient stub = new StubChatClient(List.of(
        ChatResult.toolCall("get_balance", Map.of("user", "alice")),
        ChatResult.text("Your balance is 1234.56.")));
    LlmBrain brain = new LlmBrain(stub, "payments", "", List.of("get_balance"), 6);
    Agent agent = new Agent("payments", "You answer payment questions.", brain);
    TurnResult res = agent.turn(new Event("c1", "alice", "what is my balance?"), ctx(tools));
    assertTrue(res.toolCalls.contains("get_balance"));
    assertEquals("[payments] Your balance is 1234.56.", res.reply);
  }

  @Test
  void directFinalNoTool() {
    LlmBrain brain = new LlmBrain(new StubChatClient(List.of(ChatResult.text("Hello!"))), "general");
    TurnResult res = new Agent("general", "p", brain).turn(new Event("c1", "u", "hi"), ctx(new ToolRegistry()));
    assertEquals("[general] Hello!", res.reply);
    assertTrue(res.toolCalls.isEmpty());
  }

  @Test
  void inRoutedGraph() {
    ToolRegistry tools = new ToolRegistry().register("get_balance", "balance", p -> 1234.56);
    Agent pay = new Agent("payments", "p", new LlmBrain(new StubChatClient(List.of(
        ChatResult.toolCall("get_balance", Map.of()), ChatResult.text("It is 1234.56."))), "payments"));
    Agent general = new Agent("general", "p",
        new LlmBrain(new StubChatClient(List.of(ChatResult.text("hi"))), "general"));
    RoutedGraph graph = new RoutedGraph(
        (ev, c) -> ev.text().toLowerCase().contains("balance") ? "payments" : "general",
        Map.of("payments", pay, "general", general),
        (reply, c) -> new RoutedGraph.Verifier.Result(reply.startsWith("["), reply));
    TurnResult res = graph.handle(new Event("c1", "alice", "what is my balance?"), ctx(tools));
    assertEquals("payments", res.path);
    assertTrue(res.ok && res.toolCalls.contains("get_balance") && res.reply.contains("1234.56"));
  }
}
