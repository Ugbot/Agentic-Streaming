package org.jagentic.pekko.runtime;

import org.jagentic.core.Banking;
import org.jagentic.core.LogicalClock;
import org.jagentic.core.Policies;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;

/** The Flink-free "agent brain" the Pekko runtime hosts: a compiled {@link RoutedGraph} plus its
 * {@link ToolRegistry} and retriever. Built once (from {@code GraphBuilder}/{@code Banking}) and
 * shared by every conversation entity — the entities add only the durable, single-writer actor
 * shell around it. The {@link LogicalClock} is the spec's processing clock for workflow
 * {@code timers}: wall time by default, a manual clock under a conformance fixture. */
public final class AgentDeps {

  private final RoutedGraph graph;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;
  private final LogicalClock clock;

  public AgentDeps(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this(graph, tools, retriever, LogicalClock.system());
  }

  public AgentDeps(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever,
                   LogicalClock clock) {
    this.graph = graph;
    this.tools = tools;
    this.retriever = retriever;
    this.clock = clock == null ? LogicalClock.system() : clock;
  }

  /** The built-in banking worked example (router → cards/payments/general → verifier). */
  public static AgentDeps banking() {
    return new AgentDeps(Banking.buildGraph(), Banking.defaultTools(), Banking.retriever());
  }

  public RoutedGraph graph() {
    return graph;
  }

  public ToolRegistry tools() {
    return tools;
  }

  public Retrieval.TwoTierRetriever retriever() {
    return retriever;
  }

  /** The processing-time clock workflow timers read (spec section 8). */
  public LogicalClock clock() {
    return clock;
  }

  /** The turn policies (idempotency, retry, verification) declared by the compiled graph. */
  public Policies policies() {
    return graph.policies();
  }
}
