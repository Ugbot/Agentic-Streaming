package org.agentic.flink.job;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.SupervisorChain;
import org.agentic.flink.job.AgentJob.MonitoringConfig;
import org.agentic.flink.job.AgentJob.RoutingConfig;
import org.agentic.flink.tool.ToolRegistry;
import java.util.*;

/**
 * Fluent builder for creating AgentJob instances.
 *
 * <p>Provides a declarative API for assembling agent jobs with all required configuration.
 *
 * <p><b>Basic Usage:</b>
 * <pre>{@code
 * AgentJob job = AgentJob.builder()
 *     .withId("my-job")
 *     .withAgent(myAgent)
 *     .withToolRegistry(toolRegistry)
 *     .build();
 * }</pre>
 *
 * <p><b>Full Configuration:</b>
 * <pre>{@code
 * AgentJob job = AgentJob.builder()
 *     .withId("production-job")
 *     .withName("Production Agent Pipeline")
 *     .withAgent(executorAgent)
 *     .withAgent(validatorAgent)
 *     .withToolRegistry(toolRegistry)
 *     .withAgenticFlinkConfig(storageConfig)
 *     .withRoutingConfig(RoutingConfig.kafka(
 *         "localhost:9092",
 *         "agent-input",
 *         "agent-output"))
 *     .withMonitoringConfig(MonitoringConfig.defaults())
 *     .withProperty("max-parallelism", 8)
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see AgentJob
 */
public class AgentJobBuilder {

  String jobId;
  String jobName;
  List<Agent> agents = new ArrayList<>();
  SupervisorChain supervisorChain;
  ToolRegistry toolRegistry;
  AgenticFlinkConfig storageConfig;
  RoutingConfig routingConfig = RoutingConfig.internal();  // Default to internal routing
  MonitoringConfig monitoringConfig = MonitoringConfig.defaults();  // Default monitoring
  Map<String, Object> jobProperties = new HashMap<>();
  List<org.agentic.flink.a2a.A2AStep> a2aSteps = new ArrayList<>();

  // Package-private constructor
  AgentJobBuilder() {}

  // ==================== Core Configuration ====================

  /**
   * Sets the job ID (required).
   *
   * @param jobId The job ID
   * @return this builder
   */
  public AgentJobBuilder withId(String jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * Sets the job name (optional, defaults to job ID).
   *
   * @param jobName The job name
   * @return this builder
   */
  public AgentJobBuilder withName(String jobName) {
    this.jobName = jobName;
    return this;
  }

  // ==================== Agents ====================

  /**
   * Adds an agent to this job.
   *
   * <p>Can be called multiple times to create multi-agent pipelines.
   *
   * @param agent The agent to add
   * @return this builder
   */
  public AgentJobBuilder withAgent(Agent agent) {
    this.agents.add(agent);
    return this;
  }

  /**
   * Adds multiple agents to this job.
   *
   * @param agents The agents to add
   * @return this builder
   */
  public AgentJobBuilder withAgents(List<Agent> agents) {
    this.agents.addAll(agents);
    return this;
  }

  /**
   * Sets a supervisor chain (replaces any previously added agents).
   *
   * <p>When a supervisor chain is used, individual agents are ignored - the chain
   * contains all agents in its tiers.
   *
   * @param supervisorChain The supervisor chain
   * @return this builder
   */
  public AgentJobBuilder withSupervisorChain(SupervisorChain supervisorChain) {
    this.supervisorChain = supervisorChain;
    return this;
  }

  // ==================== Infrastructure ====================

  /**
   * Sets the tool registry (required if agents use tools).
   *
   * @param toolRegistry The tool registry
   * @return this builder
   */
  public AgentJobBuilder withToolRegistry(ToolRegistry toolRegistry) {
    this.toolRegistry = toolRegistry;
    return this;
  }

  /**
   * Sets the storage configuration (optional).
   *
   * <p>If not set, no persistence is used (in-memory only).
   *
   * @param storageConfig The storage config
   * @return this builder
   */
  public AgentJobBuilder withAgenticFlinkConfig(AgenticFlinkConfig storageConfig) {
    this.storageConfig = storageConfig;
    return this;
  }

  /**
   * Sets the routing configuration (default: internal).
   *
   * @param routingConfig The routing config
   * @return this builder
   */
  public AgentJobBuilder withRoutingConfig(RoutingConfig routingConfig) {
    this.routingConfig = routingConfig;
    return this;
  }

  /**
   * Sets the monitoring configuration (default: all enabled).
   *
   * @param monitoringConfig The monitoring config
   * @return this builder
   */
  public AgentJobBuilder withMonitoringConfig(MonitoringConfig monitoringConfig) {
    this.monitoringConfig = monitoringConfig;
    return this;
  }

  // ==================== A2A steps ====================

  /**
   * Add one or more explicit {@link org.agentic.flink.a2a.A2AStep}s — deterministic remote-agent
   * delegations spliced into the stream graph (as opposed to LLM-selected {@code a2a:} tools added
   * via {@code AgentBuilder.withRemoteAgent}). Recorded on the job; wire them into the topology with
   * {@link org.agentic.flink.a2a.A2AStep#applyTo(org.apache.flink.streaming.api.datastream.DataStream)}.
   */
  public AgentJobBuilder withA2AStep(org.agentic.flink.a2a.A2AStep... steps) {
    if (steps != null) {
      this.a2aSteps.addAll(Arrays.asList(steps));
    }
    return this;
  }

  // ==================== Properties ====================

  /**
   * Sets a custom job property.
   *
   * <p>Properties can be used for custom configuration that doesn't fit the standard API.
   *
   * @param key Property key
   * @param value Property value
   * @return this builder
   */
  public AgentJobBuilder withProperty(String key, Object value) {
    this.jobProperties.put(key, value);
    return this;
  }

  /**
   * Sets multiple custom properties.
   *
   * @param properties Properties map
   * @return this builder
   */
  public AgentJobBuilder withProperties(Map<String, Object> properties) {
    this.jobProperties.putAll(properties);
    return this;
  }

  // ==================== Build ====================

  /**
   * Builds the immutable AgentJob.
   *
   * @return new AgentJob
   * @throws IllegalStateException if validation fails
   */
  public AgentJob build() {
    validate();
    applyDefaults();
    return new AgentJob(this);
  }

  // ==================== Private Methods ====================

  private void validate() {
    if (jobId == null || jobId.isEmpty()) {
      throw new IllegalStateException("Job ID is required");
    }

    // Must have either agents or a supervisor chain
    if (agents.isEmpty() && supervisorChain == null) {
      throw new IllegalStateException(
          "Job must have at least one agent or a supervisor chain");
    }

    // If using supervisor chain, agents list should be empty (chain contains the agents)
    if (supervisorChain != null && !agents.isEmpty()) {
      throw new IllegalStateException(
          "Cannot specify both individual agents and a supervisor chain. "
              + "The supervisor chain contains its own agents.");
    }

    // Tool registry required if any agent uses tools
    if (toolRegistry == null) {
      boolean anyAgentUsesTools = agents.stream()
          .anyMatch(agent -> !agent.getAllowedTools().isEmpty());

      if (supervisorChain != null) {
        anyAgentUsesTools = supervisorChain.getTiers().stream()
            .anyMatch(tier -> !tier.getAgent().getAllowedTools().isEmpty());
      }

      if (anyAgentUsesTools) {
        throw new IllegalStateException(
            "Tool registry is required when agents use tools");
      }
    }
  }

  private void applyDefaults() {
    // Apply job name default
    if (jobName == null || jobName.isEmpty()) {
      if (supervisorChain != null) {
        jobName = supervisorChain.getChainName() + " Job";
      } else if (agents.size() == 1) {
        jobName = agents.get(0).getAgentName() + " Job";
      } else {
        jobName = jobId + " Job";
      }
    }
  }

  // ==================== Quick Start Templates ====================

  /**
   * Creates a simple single-agent job with minimal configuration.
   *
   * @param jobId The job ID
   * @param agent The agent
   * @param toolRegistry The tool registry
   * @return configured job
   */
  public static AgentJob simpleSingleAgent(
      String jobId, Agent agent, ToolRegistry toolRegistry) {
    return AgentJob.builder()
        .withId(jobId)
        .withAgent(agent)
        .withToolRegistry(toolRegistry)
        .build();
  }

  /**
   * Creates a supervisor chain job with standard configuration.
   *
   * @param jobId The job ID
   * @param chain The supervisor chain
   * @param toolRegistry The tool registry
   * @return configured job
   */
  public static AgentJob supervisorChainJob(
      String jobId, SupervisorChain chain, ToolRegistry toolRegistry) {
    return AgentJob.builder()
        .withId(jobId)
        .withSupervisorChain(chain)
        .withToolRegistry(toolRegistry)
        .build();
  }

  /**
   * Creates a production-ready job with storage and monitoring.
   *
   * @param jobId The job ID
   * @param agent The agent
   * @param toolRegistry The tool registry
   * @param storageConfig The storage config
   * @return configured job with production settings
   */
  public static AgentJob productionJob(
      String jobId,
      Agent agent,
      ToolRegistry toolRegistry,
      AgenticFlinkConfig storageConfig) {
    return AgentJob.builder()
        .withId(jobId)
        .withAgent(agent)
        .withToolRegistry(toolRegistry)
        .withAgenticFlinkConfig(storageConfig)
        .withRoutingConfig(RoutingConfig.internal())
        .withMonitoringConfig(MonitoringConfig.defaults())
        .build();
  }
}
