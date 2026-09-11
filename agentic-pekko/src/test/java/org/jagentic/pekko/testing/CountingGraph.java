package org.jagentic.pekko.testing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.jagentic.core.Agent;
import org.jagentic.core.Brain;
import org.jagentic.core.Guardrail;
import org.jagentic.core.Policies;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.pekko.runtime.AgentDeps;

/**
 * A small deterministic graph whose brain, tool and guardrail count every invocation, so tests can
 * prove what did and did not run: recovery and duplicate delivery must leave all counters untouched.
 *
 * <ul>
 *   <li>text containing {@code balance} routes to {@code payments}; the brain calls the
 *       {@code lookup} tool with structured args and replies with its result</li>
 *   <li>text containing {@code refund} routes to {@code approval}, which suspends until a signal</li>
 *   <li>text containing {@code crash} routes to {@code crash}, whose brain throws the
 *       {@link Error} supplied to {@link #withCrash} (default: never routes there)</li>
 *   <li>everything else routes to {@code general}</li>
 *   <li>text containing {@code forbidden} is rejected by the guardrail</li>
 * </ul>
 */
public final class CountingGraph {
  public final AtomicInteger brainCalls = new AtomicInteger();
  public final AtomicInteger toolCalls = new AtomicInteger();
  public final AtomicInteger guardrailCalls = new AtomicInteger();
  public final double balance = ThreadLocalRandom.current().nextInt(1, 1_000_000) / 100.0;
  private Supplier<Error> crash = () -> new AssertionError("crash path not armed");

  public CountingGraph withCrash(Supplier<Error> error) {
    this.crash = error;
    return this;
  }

  public AgentDeps deps() {
    ToolRegistry tools = new ToolRegistry().register("lookup", "balance lookup", args -> {
      toolCalls.incrementAndGet();
      return balance;
    });
    Brain payments = (text, ctx) -> {
      brainCalls.incrementAndGet();
      Object v = ctx.invoke("lookup", Map.of("user", ctx.userId), null);
      return "[payments] balance " + v;
    };
    Brain general = (text, ctx) -> {
      brainCalls.incrementAndGet();
      return "[general] " + text;
    };
    Brain approval = (text, ctx) -> {
      brainCalls.incrementAndGet();
      return "[approval] refund approved";
    };
    Brain crashing = (text, ctx) -> {
      brainCalls.incrementAndGet();
      throw crash.get();
    };
    Map<String, Agent> paths = new LinkedHashMap<>();
    paths.put("payments", new Agent("payments", "payments", payments));
    paths.put("approval", new Agent("approval", "approval", approval));
    paths.put("crash", new Agent("crash", "crash", crashing));
    paths.put("general", new Agent("general", "general", general));
    RoutedGraph.Router router = (event, ctx) -> {
      String t = event.text() == null ? "" : event.text().toLowerCase();
      if (t.contains("balance")) {
        return "payments";
      }
      if (t.contains("refund")) {
        return "approval";
      }
      if (t.contains("crash")) {
        return "crash";
      }
      return "general";
    };
    Guardrail guard = new Guardrail() {
      @Override
      public String checkInput(String text) {
        guardrailCalls.incrementAndGet();
        return text != null && text.toLowerCase().contains("forbidden") ? "forbidden input" : null;
      }
    };
    RoutedGraph graph = new RoutedGraph(router, paths, null, List.of(guard), List.of(), Policies.DEFAULTS, null,
        Map.of("approval", "approval"));
    return new AgentDeps(graph, tools, new Retrieval.TwoTierRetriever(new Retrieval.InMemoryHotVectorIndex(), null, 4, 4));
  }
}
