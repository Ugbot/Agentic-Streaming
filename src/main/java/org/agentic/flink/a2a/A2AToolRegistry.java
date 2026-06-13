package org.agentic.flink.a2a;

import java.util.ArrayList;
import java.util.List;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.tool.ToolRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds {@link A2AToolExecutor}s for an agent's configured {@linkplain
 * org.agentic.flink.dsl.Agent#getRemoteAgents() remote A2A peers} and registers them into a {@link
 * ToolRegistry}.
 *
 * <p>The A2A counterpart of {@link org.agentic.flink.tools.mcp.McpToolRegistry}. Typical usage at
 * job-assembly time, alongside any other tools:
 *
 * <pre>{@code
 * ToolRegistry.ToolRegistryBuilder b = ToolRegistry.builder();
 * b.registerTool("calculator", new CalculatorTool());
 * A2AToolRegistry.registerInto(b, agent);          // adds one a2a:<peer> tool per remote agent
 * ToolRegistry registry = b.build();
 * }</pre>
 */
public final class A2AToolRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(A2AToolRegistry.class);

  private A2AToolRegistry() {}

  /** Build one executor per remote-agent spec on the agent, using the agent's client factory. */
  public static List<A2AToolExecutor> build(Agent agent) {
    A2AClientFactory factory = agent.getA2AClientFactory();
    List<A2AToolExecutor> executors = new ArrayList<>(agent.getRemoteAgents().size());
    for (RemoteAgentSpec spec : agent.getRemoteAgents()) {
      executors.add(new A2AToolExecutor(spec, factory));
    }
    return executors;
  }

  /** Build executors from explicit specs with an explicit factory (no Agent needed). */
  public static List<A2AToolExecutor> build(
      List<RemoteAgentSpec> specs, A2AClientFactory factory) {
    A2AClientFactory f = factory == null ? A2AClientFactory.discovering() : factory;
    List<A2AToolExecutor> executors = new ArrayList<>(specs.size());
    for (RemoteAgentSpec spec : specs) {
      executors.add(new A2AToolExecutor(spec, f));
    }
    return executors;
  }

  /** Build and register one {@code a2a:<peer>} tool per remote agent into the registry builder. */
  public static void registerInto(ToolRegistry.ToolRegistryBuilder builder, Agent agent) {
    List<A2AToolExecutor> executors = build(agent);
    for (A2AToolExecutor executor : executors) {
      builder.registerTool(executor.getToolId(), executor.getDescription(), executor);
    }
    if (!executors.isEmpty()) {
      LOG.info(
          "Registered {} A2A peer tool(s) for agent '{}'",
          executors.size(),
          agent.getAgentId());
    }
  }
}
