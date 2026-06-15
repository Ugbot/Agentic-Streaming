package org.jagentic.pekko.runtime;

import org.jagentic.core.Banking;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;

/** The Flink-free "agent brain" the Pekko runtime hosts: a compiled {@link RoutedGraph} plus its
 * {@link ToolRegistry} and retriever. Built once (from {@code GraphBuilder}/{@code Banking}) and
 * shared by every conversation entity — the entities add only the durable, single-writer actor
 * shell around it. */
public final class AgentDeps {

  private final RoutedGraph graph;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;

  public AgentDeps(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
    this.graph = graph;
    this.tools = tools;
    this.retriever = retriever;
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
}
