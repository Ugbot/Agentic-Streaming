package org.jagentic.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The banking {@code router -> path -> verifier} worked example, engine-agnostic and
 * model-free — the Java mirror of {@code pyagentic.banking} / the Flink
 * {@code BankingAgentGraph}. Every JVM engine port reuses this to demonstrate the
 * same workflow on its runtime.
 */
public final class Banking {
  private Banking() {}

  public static final int DIM = 256;

  /** A tiny KB the cards/payments paths retrieve from. */
  public static final Map<String, String> KB = Map.of(
      "kb_cards_types", "We offer three card types: classic, gold, and platinum, each with different fees.",
      "kb_cards_crypto", "Crypto cash-back can be redeemed to a linked wallet or a manual address.",
      "kb_payments_limits", "Daily transfer limits are 10,000 by default; raise them in settings.",
      "kb_payments_dispute", "To dispute a charge, open the transaction and tap Dispute within 60 days.");

  public static void seedKb(Retrieval.HotVectorIndex index) {
    for (Map.Entry<String, String> e : KB.entrySet()) {
      index.upsert(e.getKey(), Retrieval.embed(e.getValue(), DIM), e.getValue());
    }
  }

  /** Deterministic brain: keyword rules + optional tool call / retrieval. */
  public static final class RuleBrain implements Brain {
    private final String name;

    public RuleBrain(String name) {
      this.name = name;
    }

    @Override
    public String turn(String userText, AgentContext ctx) {
      String low = userText.toLowerCase();
      if (low.contains("balance")) {
        Object bal = ctx.callTool("get_balance", Map.of("user", ctx.userId));
        return "[" + name + "] Your balance is " + bal + ".";
      }
      if (ctx.retriever != null) {
        List<Retrieval.Scored> hits = ctx.retriever.retrieve(Retrieval.embed(userText, DIM), 1);
        if (!hits.isEmpty() && hits.get(0).score() > 0.15) {
          return "[" + name + "] " + hits.get(0).text();
        }
      }
      return "[" + name + "] I can help with " + name + " questions. You said: \"" + userText + "\"";
    }
  }

  public static String router(Event event, AgentContext ctx) {
    String low = event.text().toLowerCase();
    if (low.contains("card") || low.contains("crypto") || low.contains("cash-back") || low.contains("cashback")) {
      return "cards";
    }
    if (low.contains("transfer") || low.contains("payment") || low.contains("dispute")
        || low.contains("charge") || low.contains("limit") || low.contains("balance")) {
      return "payments";
    }
    return "general";
  }

  public static RoutedGraph buildGraph() {
    Map<String, Agent> paths = new LinkedHashMap<>();
    paths.put("cards", new Agent("cards", "You answer card questions.", new RuleBrain("cards")));
    paths.put("payments", new Agent("payments", "You answer payment questions.", new RuleBrain("payments")));
    paths.put("general", new Agent("general", "You answer general questions.", new RuleBrain("general")));
    RoutedGraph.Verifier verifier =
        (reply, ctx) -> new RoutedGraph.Verifier.Result(
            reply != null && reply.startsWith("["), reply);
    return new RoutedGraph(Banking::router, paths, verifier);
  }

  public static ToolRegistry defaultTools() {
    return new ToolRegistry().register("get_balance", "Look up the user's balance", p -> 1234.56);
  }

  /** A ready-to-use retriever seeded with the KB (hot tier only; cold = null). */
  public static Retrieval.TwoTierRetriever retriever() {
    Retrieval.InMemoryHotVectorIndex hot = new Retrieval.InMemoryHotVectorIndex();
    seedKb(hot);
    return new Retrieval.TwoTierRetriever(hot, null, 4, 4);
  }
}
