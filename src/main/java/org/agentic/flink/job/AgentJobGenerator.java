package org.agentic.flink.job;

import org.agentic.flink.cep.CepPatternBuilder;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.SupervisorChain;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.tool.ToolRegistry;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates complete Flink DataStream jobs from declarative agent definitions.
 *
 * <p>This is the key orchestrator that transforms Agent and SupervisorChain specifications into
 * executable Flink pipelines with CEP patterns, storage backends, and routing logic.
 *
 * <p><b>Core Responsibilities:</b>
 * <ul>
 *   <li>Wire agents to CEP patterns based on their state machines</li>
 *   <li>Route events through supervisor chains with escalation logic</li>
 *   <li>Connect storage backends (PostgreSQL, Redis) for state persistence</li>
 *   <li>Set up tool execution infrastructure</li>
 *   <li>Handle internal vs external (Kafka) event routing</li>
 *   <li>Generate side outputs for monitoring and debugging</li>
 * </ul>
 *
 * <p><b>Basic Usage:</b>
 * <pre>{@code
 * // 1. Define agents
 * Agent executor = Agent.builder()
 *     .withId("executor")
 *     .withSystemPrompt("Execute tasks")
 *     .withTools("calculator", "web-search")
 *     .build();
 *
 * // 2. Create job
 * AgentJob job = AgentJob.builder()
 *     .withAgent(executor)
 *     .withAgenticFlinkConfig(storageConfig)
 *     .withToolRegistry(toolRegistry)
 *     .build();
 *
 * // 3. Generate pipeline
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * AgentJobGenerator generator = new AgentJobGenerator(env, job);
 *
 * DataStream<AgentEvent> input = ...; // From Kafka, socket, etc.
 * DataStream<AgentEvent> results = generator.generate(input);
 *
 * env.execute("Agentic Flink Job");
 * }</pre>
 *
 * <p><b>With Supervisor Chain:</b>
 * <pre>{@code
 * SupervisorChain chain = SupervisorChain.builder()
 *     .withId("quality-chain")
 *     .addTier("executor", executorAgent)
 *     .addTier("qa-review", qaAgent)
 *     .addTier("final-approval", supervisorAgent)
 *     .withEscalationPolicy(EscalationPolicy.NEXT_TIER)
 *     .build();
 *
 * AgentJob job = AgentJob.builder()
 *     .withSupervisorChain(chain)
 *     .withAgenticFlinkConfig(storageConfig)
 *     .withToolRegistry(toolRegistry)
 *     .build();
 *
 * AgentJobGenerator generator = new AgentJobGenerator(env, job);
 * DataStream<AgentEvent> results = generator.generate(input);
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see AgentJob
 * @see Agent
 * @see SupervisorChain
 */
public class AgentJobGenerator implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AgentJobGenerator.class);

  private final StreamExecutionEnvironment env;
  private final AgentJob job;

  // Side output tags for monitoring
  public static final OutputTag<AgentEvent> VALIDATION_FAILURES_TAG =
      new OutputTag<AgentEvent>("validation-failures") {};
  public static final OutputTag<AgentEvent> TIMEOUT_TAG =
      new OutputTag<AgentEvent>("timeouts") {};
  public static final OutputTag<AgentEvent> ESCALATION_TAG =
      new OutputTag<AgentEvent>("escalations") {};
  public static final OutputTag<AgentEvent> COMPENSATION_TAG =
      new OutputTag<AgentEvent>("compensations") {};

  /**
   * Creates a new job generator.
   *
   * @param env The Flink execution environment
   * @param job The agent job definition
   */
  public AgentJobGenerator(StreamExecutionEnvironment env, AgentJob job) {
    this.env = env;
    this.job = job;
  }

  /**
   * Generates the complete Flink pipeline from the agent job definition.
   *
   * <p>This is the main entry point that:
   * <ol>
   *   <li>Validates the job configuration</li>
   *   <li>Sets up storage backends</li>
   *   <li>Wires agent pipelines with CEP patterns</li>
   *   <li>Configures supervisor chain routing (if applicable)</li>
   *   <li>Sets up side outputs for monitoring</li>
   * </ol>
   *
   * @param input The input stream of AgentEvents
   * @return The output stream of completed/failed events
   */
  public DataStream<AgentEvent> generate(DataStream<AgentEvent> input) {
    LOG.info("Generating agent job: {}", job.getJobId());

    // Apply watermark strategy for event time processing
    DataStream<AgentEvent> inputWithWatermarks = input.assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<AgentEvent>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );

    // Route to appropriate pipeline based on job type
    if (job.hasSupervisorChain()) {
      LOG.info("Generating supervisor chain pipeline: {}", job.getSupervisorChain().getChainId());
      return generateSupervisorChainPipeline(inputWithWatermarks);
    } else if (job.hasMultipleAgents()) {
      LOG.info("Generating multi-agent pipeline with {} agents", job.getAgents().size());
      return generateMultiAgentPipeline(inputWithWatermarks);
    } else if (job.hasSingleAgent()) {
      LOG.info("Generating single agent pipeline: {}", job.getAgents().get(0).getAgentId());
      return generateSingleAgentPipeline(inputWithWatermarks, job.getAgents().get(0));
    } else {
      throw new IllegalStateException(
          "AgentJob must have at least one agent or a supervisor chain");
    }
  }

  // ==================== Single Agent Pipeline ====================

  /**
   * Generates a pipeline for a single agent (simplest case).
   *
   * <p>Flow: Input → CEP Pattern → Agent Execution → Storage → Output
   */
  private DataStream<AgentEvent> generateSingleAgentPipeline(
      DataStream<AgentEvent> input, Agent agent) {

    // Filter events for this agent
    DataStream<AgentEvent> agentEvents = input
        .filter(event -> event.getAgentId().equals(agent.getAgentId()))
        .name("filter-" + agent.getAgentId());

    // Apply CEP pattern from agent's state machine
    PatternStream<AgentEvent> patternStream = CEP.pattern(
        agentEvents.keyBy(AgentEvent::getFlowId),
        agent.getStateMachine().generateCepPattern()
    );

    // Process pattern matches → agent execution
    SingleOutputStreamOperator<AgentEvent> processedEvents = patternStream
        .process(new AgentExecutionFunction(agent, job.getToolRegistry()))
        .name("execute-" + agent.getAgentId());

    // Wire storage if configured
    if (job.getStorageConfig() != null) {
      processedEvents = wireStorage(processedEvents, agent);
    }

    return processedEvents;
  }

  // ==================== Multi-Agent Pipeline ====================

  /**
   * Generates a pipeline for multiple independent agents.
   *
   * <p>Flow: Input → Route by AgentId → [Agent Pipeline 1, 2, 3...] → Union → Output
   */
  private DataStream<AgentEvent> generateMultiAgentPipeline(DataStream<AgentEvent> input) {

    List<DataStream<AgentEvent>> agentStreams = new ArrayList<>();

    for (Agent agent : job.getAgents()) {
      DataStream<AgentEvent> agentStream = generateSingleAgentPipeline(input, agent);
      agentStreams.add(agentStream);
    }

    // Union all agent output streams
    DataStream<AgentEvent> unionStream = agentStreams.get(0);
    for (int i = 1; i < agentStreams.size(); i++) {
      unionStream = unionStream.union(agentStreams.get(i));
    }

    return unionStream;
  }

  // ==================== Supervisor Chain Pipeline ====================

  /**
   * Generates a pipeline for a supervisor chain (N-tier escalation).
   *
   * <p>Flow:
   * <pre>
   * Input → Tier 0 → Quality Check → Pass? → Output
   *                              ↓
   *                         Escalate to Tier 1 → Quality Check → Pass? → Output
   *                                                           ↓
   *                                                      Escalate to Tier 2 → ...
   * </pre>
   *
   * <p>Uses side outputs for escalation routing between tiers.
   */
  private DataStream<AgentEvent> generateSupervisorChainPipeline(DataStream<AgentEvent> input) {

    SupervisorChain chain = job.getSupervisorChain();

    // Start with first tier
    DataStream<AgentEvent> currentTierInput = input;
    DataStream<AgentEvent> currentTierOutput = null;

    for (int i = 0; i < chain.getTierCount(); i++) {
      SupervisorChain.SupervisorTier tier = chain.getTier(i);
      Agent tierAgent = tier.getAgent();

      LOG.info("Wiring tier {}: {}", i, tier.getTierName());

      // Generate agent pipeline for this tier
      SingleOutputStreamOperator<AgentEvent> tierOutput =
          generateTierPipeline(currentTierInput, tier, chain);

      if (i == 0) {
        // First tier - save main output
        currentTierOutput = tierOutput;
      } else {
        // Subsequent tiers - union with previous output
        currentTierOutput = currentTierOutput.union(tierOutput);
      }

      // If not the last tier, route escalations to next tier
      if (i < chain.getTierCount() - 1) {
        DataStream<AgentEvent> escalatedEvents = tierOutput.getSideOutput(ESCALATION_TAG);
        currentTierInput = escalatedEvents;
      }
    }

    return currentTierOutput;
  }

  /**
   * Generates the pipeline for a single tier in a supervisor chain.
   *
   * <p>Includes quality checking and escalation logic.
   */
  private SingleOutputStreamOperator<AgentEvent> generateTierPipeline(
      DataStream<AgentEvent> input,
      SupervisorChain.SupervisorTier tier,
      SupervisorChain chain) {

    Agent agent = tier.getAgent();

    // Filter events for this tier
    DataStream<AgentEvent> tierEvents = input
        .filter(event -> {
          // Check if event is for this tier
          Integer targetTier = (Integer) event.getMetadata().get("target_tier");
          return targetTier == null || targetTier == tier.getTierIndex();
        })
        .name("filter-tier-" + tier.getTierIndex());

    // Apply CEP pattern
    PatternStream<AgentEvent> patternStream = CEP.pattern(
        tierEvents.keyBy(AgentEvent::getFlowId),
        agent.getStateMachine().generateCepPattern()
    );

    // Process with escalation logic
    SingleOutputStreamOperator<AgentEvent> processedEvents = patternStream
        .process(new SupervisorTierFunction(tier, chain, job.getToolRegistry()))
        .name("tier-" + tier.getTierIndex() + "-" + tier.getTierName());

    // Wire storage
    if (job.getStorageConfig() != null) {
      processedEvents = wireStorage(processedEvents, agent);
    }

    return processedEvents;
  }

  // ==================== Storage Wiring ====================

  /**
   * Wires storage backends (PostgreSQL, Redis) to persist agent state.
   *
   * <p>Uses async I/O for non-blocking storage operations.
   */
  private SingleOutputStreamOperator<AgentEvent> wireStorage(
      SingleOutputStreamOperator<AgentEvent> stream, Agent agent) {

    AgenticFlinkConfig config = job.getStorageConfig();

    return (SingleOutputStreamOperator<AgentEvent>) AsyncDataStream.unorderedWait(
        stream,
        new StorageSinkFunction(config),
        5000, TimeUnit.MILLISECONDS, 100
    ).name("storage-" + agent.getAgentId());
  }

  // ==================== Side Output Routing ====================

  /**
   * Gets side output stream for validation failures.
   */
  public DataStream<AgentEvent> getValidationFailures(DataStream<AgentEvent> mainStream) {
    if (mainStream instanceof SingleOutputStreamOperator) {
      return ((SingleOutputStreamOperator<AgentEvent>) mainStream).getSideOutput(VALIDATION_FAILURES_TAG);
    }
    throw new IllegalArgumentException("Stream must be SingleOutputStreamOperator to get side outputs");
  }

  /**
   * Gets side output stream for timeouts.
   */
  public DataStream<AgentEvent> getTimeouts(DataStream<AgentEvent> mainStream) {
    if (mainStream instanceof SingleOutputStreamOperator) {
      return ((SingleOutputStreamOperator<AgentEvent>) mainStream).getSideOutput(TIMEOUT_TAG);
    }
    throw new IllegalArgumentException("Stream must be SingleOutputStreamOperator to get side outputs");
  }

  /**
   * Gets side output stream for escalations.
   */
  public DataStream<AgentEvent> getEscalations(DataStream<AgentEvent> mainStream) {
    if (mainStream instanceof SingleOutputStreamOperator) {
      return ((SingleOutputStreamOperator<AgentEvent>) mainStream).getSideOutput(ESCALATION_TAG);
    }
    throw new IllegalArgumentException("Stream must be SingleOutputStreamOperator to get side outputs");
  }

  /**
   * Gets side output stream for compensations.
   */
  public DataStream<AgentEvent> getCompensations(DataStream<AgentEvent> mainStream) {
    if (mainStream instanceof SingleOutputStreamOperator) {
      return ((SingleOutputStreamOperator<AgentEvent>) mainStream).getSideOutput(COMPENSATION_TAG);
    }
    throw new IllegalArgumentException("Stream must be SingleOutputStreamOperator to get side outputs");
  }

  // ==================== Builder Support ====================

  /**
   * Creates a generator with default settings.
   */
  public static AgentJobGenerator create(StreamExecutionEnvironment env, AgentJob job) {
    return new AgentJobGenerator(env, job);
  }
}
