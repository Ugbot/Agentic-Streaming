package org.agentic.flink.job;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.SupervisorChain;
import org.agentic.flink.tool.ToolRegistry;
import java.io.Serializable;
import java.util.*;

/**
 * Immutable definition of an agent job (collection of agents + configuration).
 *
 * <p>An AgentJob packages everything needed to generate a complete Flink pipeline:
 * <ul>
 *   <li>One or more Agent definitions</li>
 *   <li>Optional SupervisorChain for tiered review</li>
 *   <li>ToolRegistry for tool execution</li>
 *   <li>AgenticFlinkConfig for persistence (PostgreSQL, Redis)</li>
 *   <li>Routing configuration (internal vs Kafka)</li>
 *   <li>Monitoring and observability settings</li>
 * </ul>
 *
 * <p><b>Single Agent Job:</b>
 * <pre>{@code
 * AgentJob job = AgentJob.builder()
 *     .withId("research-job")
 *     .withAgent(researchAgent)
 *     .withToolRegistry(toolRegistry)
 *     .withAgenticFlinkConfig(storageConfig)
 *     .build();
 * }</pre>
 *
 * <p><b>Multi-Agent Job:</b>
 * <pre>{@code
 * AgentJob job = AgentJob.builder()
 *     .withId("parallel-research")
 *     .withAgent(webSearchAgent)
 *     .withAgent(documentAnalysisAgent)
 *     .withAgent(synthesisAgent)
 *     .withToolRegistry(toolRegistry)
 *     .build();
 * }</pre>
 *
 * <p><b>Supervisor Chain Job:</b>
 * <pre>{@code
 * SupervisorChain chain = SupervisorChain.builder()
 *     .withId("quality-chain")
 *     .addTier("executor", executorAgent)
 *     .addTier("qa-review", qaAgent)
 *     .addTier("final-approval", supervisorAgent)
 *     .build();
 *
 * AgentJob job = AgentJob.builder()
 *     .withId("quality-job")
 *     .withSupervisorChain(chain)
 *     .withToolRegistry(toolRegistry)
 *     .withAgenticFlinkConfig(storageConfig)
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see Agent
 * @see SupervisorChain
 * @see AgentJobGenerator
 */
public class AgentJob implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String jobId;
  private final String jobName;
  private final List<Agent> agents;
  private final SupervisorChain supervisorChain;
  private final ToolRegistry toolRegistry;
  private final AgenticFlinkConfig storageConfig;
  private final RoutingConfig routingConfig;
  private final MonitoringConfig monitoringConfig;
  private final Map<String, Object> jobProperties;
  private final List<org.agentic.flink.a2a.A2AStep> a2aSteps;

  // Package-private constructor - use builder
  AgentJob(AgentJobBuilder builder) {
    this.jobId = builder.jobId;
    this.jobName = builder.jobName;
    this.agents = Collections.unmodifiableList(new ArrayList<>(builder.agents));
    this.supervisorChain = builder.supervisorChain;
    this.toolRegistry = builder.toolRegistry;
    this.storageConfig = builder.storageConfig;
    this.routingConfig = builder.routingConfig;
    this.monitoringConfig = builder.monitoringConfig;
    this.jobProperties = Collections.unmodifiableMap(new HashMap<>(builder.jobProperties));
    this.a2aSteps = Collections.unmodifiableList(new ArrayList<>(builder.a2aSteps));
  }

  // ==================== Getters ====================

  public String getJobId() { return jobId; }
  public String getJobName() { return jobName; }
  public List<Agent> getAgents() { return agents; }
  public SupervisorChain getSupervisorChain() { return supervisorChain; }
  public ToolRegistry getToolRegistry() { return toolRegistry; }
  public AgenticFlinkConfig getStorageConfig() { return storageConfig; }
  public RoutingConfig getRoutingConfig() { return routingConfig; }
  public MonitoringConfig getMonitoringConfig() { return monitoringConfig; }
  public List<org.agentic.flink.a2a.A2AStep> getA2ASteps() { return a2aSteps; }
  public Map<String, Object> getJobProperties() { return jobProperties; }

  // ==================== Helper Methods ====================

  /**
   * Checks if this job has a single agent.
   */
  public boolean hasSingleAgent() {
    return agents.size() == 1 && supervisorChain == null;
  }

  /**
   * Checks if this job has multiple independent agents.
   */
  public boolean hasMultipleAgents() {
    return agents.size() > 1 && supervisorChain == null;
  }

  /**
   * Checks if this job uses a supervisor chain.
   */
  public boolean hasSupervisorChain() {
    return supervisorChain != null;
  }

  /**
   * Gets an agent by ID.
   */
  public Optional<Agent> getAgent(String agentId) {
    return agents.stream()
        .filter(a -> a.getAgentId().equals(agentId))
        .findFirst();
  }

  /**
   * Gets all agent IDs in this job (including those in supervisor chain).
   */
  public Set<String> getAllAgentIds() {
    Set<String> agentIds = new HashSet<>();
    for (Agent agent : agents) {
      agentIds.add(agent.getAgentId());
    }
    if (supervisorChain != null) {
      for (SupervisorChain.SupervisorTier tier : supervisorChain.getTiers()) {
        agentIds.add(tier.getAgent().getAgentId());
      }
    }
    return agentIds;
  }

  /**
   * Creates a builder initialized with this job's configuration.
   */
  public AgentJobBuilder toBuilder() {
    return new AgentJobBuilder()
        .withId(this.jobId)
        .withName(this.jobName)
        .withAgents(this.agents)
        .withSupervisorChain(this.supervisorChain)
        .withToolRegistry(this.toolRegistry)
        .withAgenticFlinkConfig(this.storageConfig)
        .withRoutingConfig(this.routingConfig)
        .withMonitoringConfig(this.monitoringConfig);
  }

  public static AgentJobBuilder builder() {
    return new AgentJobBuilder();
  }

  @Override
  public String toString() {
    return String.format(
        "AgentJob[id=%s, agents=%d, supervisorChain=%s]",
        jobId, agents.size(), supervisorChain != null ? supervisorChain.getChainId() : "none");
  }

  // ==================== Configuration Classes ====================

  /**
   * Routing configuration (internal vs Kafka).
   */
  public static class RoutingConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RoutingMode mode;
    private final String kafkaBootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final Map<String, String> kafkaProperties;

    public RoutingConfig(
        RoutingMode mode,
        String kafkaBootstrapServers,
        String inputTopic,
        String outputTopic,
        Map<String, String> kafkaProperties) {
      this.mode = mode;
      this.kafkaBootstrapServers = kafkaBootstrapServers;
      this.inputTopic = inputTopic;
      this.outputTopic = outputTopic;
      this.kafkaProperties = Collections.unmodifiableMap(new HashMap<>(kafkaProperties));
    }

    public RoutingMode getMode() { return mode; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getInputTopic() { return inputTopic; }
    public String getOutputTopic() { return outputTopic; }
    public Map<String, String> getKafkaProperties() { return kafkaProperties; }

    public boolean isKafkaMode() {
      return mode == RoutingMode.KAFKA || mode == RoutingMode.HYBRID;
    }

    public boolean isInternalMode() {
      return mode == RoutingMode.INTERNAL || mode == RoutingMode.HYBRID;
    }

    /**
     * Creates internal-only routing config.
     */
    public static RoutingConfig internal() {
      return new RoutingConfig(RoutingMode.INTERNAL, null, null, null, Collections.emptyMap());
    }

    /**
     * Creates Kafka-based routing config.
     */
    public static RoutingConfig kafka(
        String bootstrapServers, String inputTopic, String outputTopic) {
      return new RoutingConfig(
          RoutingMode.KAFKA, bootstrapServers, inputTopic, outputTopic, Collections.emptyMap());
    }

    /**
     * Creates hybrid routing config (internal + Kafka).
     */
    public static RoutingConfig hybrid(
        String bootstrapServers, String inputTopic, String outputTopic) {
      return new RoutingConfig(
          RoutingMode.HYBRID, bootstrapServers, inputTopic, outputTopic, Collections.emptyMap());
    }
  }

  /**
   * Routing mode enum.
   */
  public enum RoutingMode {
    /** Internal Flink routing only (no Kafka) */
    INTERNAL,
    /** External Kafka routing only */
    KAFKA,
    /** Hybrid: internal routing + Kafka for external integrations */
    HYBRID
  }

  /**
   * Monitoring and observability configuration.
   */
  public static class MonitoringConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean metricsEnabled;
    private final boolean tracingEnabled;
    private final boolean sideOutputsEnabled;
    private final int checkpointIntervalMs;
    private final String metricsReporterClass;

    public MonitoringConfig(
        boolean metricsEnabled,
        boolean tracingEnabled,
        boolean sideOutputsEnabled,
        int checkpointIntervalMs,
        String metricsReporterClass) {
      this.metricsEnabled = metricsEnabled;
      this.tracingEnabled = tracingEnabled;
      this.sideOutputsEnabled = sideOutputsEnabled;
      this.checkpointIntervalMs = checkpointIntervalMs;
      this.metricsReporterClass = metricsReporterClass;
    }

    public boolean isMetricsEnabled() { return metricsEnabled; }
    public boolean isTracingEnabled() { return tracingEnabled; }
    public boolean isSideOutputsEnabled() { return sideOutputsEnabled; }
    public int getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public String getMetricsReporterClass() { return metricsReporterClass; }

    /**
     * Creates default monitoring config (everything enabled).
     */
    public static MonitoringConfig defaults() {
      return new MonitoringConfig(true, true, true, 60000, null);
    }

    /**
     * Creates minimal monitoring config (metrics only).
     */
    public static MonitoringConfig minimal() {
      return new MonitoringConfig(true, false, false, 300000, null);
    }
  }
}
