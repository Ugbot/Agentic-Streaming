package org.agentic.flink.example.banking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.tools.ToolExecutor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Exercises the bounded ReAct loop with a scripted chat connection — no live model key. */
final class ReActTurnBrainTest {

  private static ChatSetup setup() {
    return ChatSetup.builder().withModel("test").withTemperature(0.0).withMaxResponseTokens(256).build();
  }

  private static BankingTurnContext ctx(
      RoutingBudget budget, BankingTurnContext.CustomerServiceClient cs) {
    budget.startTurn(0L);
    return new BankingTurnContext("ctx-1", budget, 0L, cs);
  }

  @Test
  @DisplayName("calls a tool then returns the final answer")
  void toolThenFinal() {
    AtomicInteger toolCalls = new AtomicInteger();
    ToolExecutor lookup =
        new StubTool(
            "lookup_balance",
            p -> {
              toolCalls.incrementAndGet();
              return Map.of("error", false, "content", "balance is $42");
            });

    ChatConnection chat =
        new ScriptedChat(
            List.of(
                "{\"type\":\"action\",\"tool\":\"lookup_balance\",\"arguments\":{\"id\":\"x\"}}",
                "{\"type\":\"final\",\"answer\":\"Your balance is $42.\"}"));

    ReActTurnBrain brain =
        new ReActTurnBrain(chat, setup(), "You are a bank agent.", Map.of("lookup_balance", lookup), 5000);

    String reply = brain.respond("what's my balance?", ctx(RoutingBudget.defaults(), null));
    assertEquals("Your balance is $42.", reply);
    assertEquals(1, toolCalls.get());
  }

  @Test
  @DisplayName("ask_customer_service is capped by the round-trip budget")
  void csRoundTripCapped() {
    AtomicInteger csCalls = new AtomicInteger();
    BankingTurnContext.CustomerServiceClient cs =
        (cid, msg) -> {
          csCalls.incrementAndGet();
          return "cs says: keep going";
        };
    // The model never finalizes — it always asks CS again (the explosion the budget must stop).
    ChatConnection chat =
        new AlwaysAsk("{\"type\":\"action\",\"tool\":\"ask_customer_service\",\"arguments\":{\"message\":\"more\"}}");

    RoutingBudget budget = new RoutingBudget(2, 50, 240_000L, 0); // cap CS round-trips at 2
    ReActTurnBrain brain = new ReActTurnBrain(chat, setup(), "personal", Map.of(), 5000);
    String reply = brain.respond("escalate", ctx(budget, cs));

    assertEquals(2, csCalls.get(), "CS round-trips must be capped at the budget");
    assertTrue(reply != null && !reply.isBlank());
  }

  @Test
  @DisplayName("iteration cap stops a non-finalizing loop")
  void iterationCapped() {
    // Model only ever 'thinks', never finalizes; the iteration budget must end the turn.
    ChatConnection chat = new AlwaysAsk("{\"type\":\"thought\",\"thought\":\"hmm\"}");
    RoutingBudget budget = new RoutingBudget(5, 3, 240_000L, 0); // max 3 iterations
    ReActTurnBrain brain = new ReActTurnBrain(chat, setup(), "agent", Map.of(), 5000);
    String reply = brain.respond("loop forever", ctx(budget, null));
    assertEquals(3, budget.iterationsUsed(), "must stop at the iteration cap");
    assertTrue(reply != null);
  }

  // ---- scripted chat doubles ----

  /** Returns each scripted response once, in order; repeats the last when exhausted. */
  static final class ScriptedChat implements ChatConnection {
    private static final long serialVersionUID = 1L;
    private final List<String> responses;

    ScriptedChat(List<String> responses) {
      this.responses = responses;
    }

    @Override
    public ChatClient bind(RuntimeContext rc) {
      Deque<String> queue = new ArrayDeque<>(responses);
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          String text = queue.size() > 1 ? queue.poll() : queue.peek();
          return new ChatResponse(
              text, "test", List.of(), 1L, ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "scripted";
        }
      };
    }

    @Override
    public String providerName() {
      return "scripted";
    }
  }

  /** Always returns the same scripted step (models a runaway / non-finalizing agent). */
  static final class AlwaysAsk implements ChatConnection {
    private static final long serialVersionUID = 1L;
    private final String step;

    AlwaysAsk(String step) {
      this.step = step;
    }

    @Override
    public ChatClient bind(RuntimeContext rc) {
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          return new ChatResponse(step, "test", List.of(), 1L, ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "always";
        }
      };
    }

    @Override
    public String providerName() {
      return "always";
    }
  }

  static final class StubTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    private final String id;
    private final java.util.function.Function<Map<String, Object>, Object> fn;

    StubTool(String id, java.util.function.Function<Map<String, Object>, Object> fn) {
      this.id = id;
      this.fn = fn;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      return CompletableFuture.completedFuture(fn.apply(parameters));
    }

    @Override
    public String getToolId() {
      return id;
    }

    @Override
    public String getDescription() {
      return id;
    }
  }
}
