package org.agentic.flink.example;

import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.dsl.SupervisorChain;
import org.agentic.flink.dsl.SupervisorChain.EscalationPolicy;
import org.agentic.flink.job.AgentJob;
import org.agentic.flink.job.AgentJobGenerator;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Supervisor Chain Example - N-Tier Escalation
 *
 * <p>Demonstrates the flexible N-tier supervisor chain feature:
 *
 * <ul>
 *   <li><b>Tier 0: Executor</b> - Performs the actual work
 *   <li><b>Tier 1: QA Review</b> - Checks quality (threshold: 0.8)
 *   <li><b>Tier 2: Security Check</b> - Validates security concerns
 *   <li><b>Tier 3: Final Approval</b> - Human-in-the-loop approval
 * </ul>
 *
 * <p><b>Escalation Flow:</b>
 * <pre>
 * Request → Executor → quality < 0.8? → QA Review → quality < 0.9? → Security
 *                                                                        ↓
 *                                                         Final Approval ← pass
 * </pre>
 *
 * <p><b>Key Features:</b>
 * <ul>
 *   <li>Quality thresholds per tier
 *   <li>Automatic escalation on low quality scores
 *   <li>Max escalation limits
 *   <li>Human approval at final tier
 *   <li>Multiple escalation policies (NEXT_TIER, SKIP_TO_TOP, RETRY_CURRENT, FAIL_FAST)
 * </ul>
 *
 * <p><b>Prerequisites:</b>
 * <ul>
 *   <li>Ollama running: http://localhost:11434
 *   <li>Model: ollama pull qwen2.5:3b
 * </ul>
 *
 * <p><b>To run:</b>
 * <pre>
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.SupervisorChainExample"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class SupervisorChainExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=".repeat(80));
    System.out.println("  Supervisor Chain Example - N-Tier Quality Control");
    System.out.println("=".repeat(80));
    System.out.println();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // ==================== Step 1: Define Agents for Each Tier ====================

    System.out.println("🏗️  Step 1: Defining agents for each tier...\n");

    // Tier 0: Executor - Does the actual work
    Agent executorAgent = Agent.builder()
        .withId("executor")
        .withName("Task Executor")
        .withType(AgentType.EXECUTOR)
        .withSystemPrompt(
            "You are a task executor. Execute tasks efficiently using available tools.\n" +
            "Focus on correctness and completeness.")
        .withChatSetup(ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.7).build())
        .withTools("database-query", "api-call", "file-processor")
        .withMaxIterations(5)
        .build();

    // Tier 1: QA Reviewer - Checks quality
    Agent qaAgent = Agent.builder()
        .withId("qa-reviewer")
        .withName("Quality Assurance")
        .withType(AgentType.VALIDATOR)
        .withSystemPrompt(
            "You are a QA reviewer. Review execution results for:\n" +
            "- Correctness\n" +
            "- Completeness\n" +
            "- Code quality\n" +
            "- Test coverage\n" +
            "Reject if quality score < 0.8")
        .withChatSetup(ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.2).build())
        .withValidationEnabled(true)
        .build();

    // Tier 2: Security Checker - Validates security
    Agent securityAgent = Agent.builder()
        .withId("security-checker")
        .withName("Security Validator")
        .withType(AgentType.VALIDATOR)
        .withSystemPrompt(
            "You are a security validator. Check for:\n" +
            "- SQL injection risks\n" +
            "- XSS vulnerabilities\n" +
            "- Authentication issues\n" +
            "- Data exposure risks\n" +
            "Reject if security concerns found.")
        .withChatSetup(ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.1).build())
        .build();

    // Tier 3: Final Approver - Human approval
    Agent approvalAgent = Agent.builder()
        .withId("final-approver")
        .withName("Final Approval")
        .withType(AgentType.SUPERVISOR)
        .withSystemPrompt(
            "You are the final approver. Review the entire workflow and:\n" +
            "- Verify all checks passed\n" +
            "- Ensure business requirements met\n" +
            "- Give final sign-off")
        .withChatSetup(ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.1).build())
        .build();

    System.out.println("✅ 4 tier agents defined:");
    System.out.println("   - Tier 0: Executor");
    System.out.println("   - Tier 1: QA Reviewer");
    System.out.println("   - Tier 2: Security Checker");
    System.out.println("   - Tier 3: Final Approver");
    System.out.println();

    // ==================== Step 2: Build Supervisor Chain ====================

    System.out.println("⛓️  Step 2: Building supervisor chain with escalation...\n");

    SupervisorChain chain = SupervisorChain.builder()
        .withId("quality-chain")
        .withName("4-Tier Quality Control Chain")
        .addTier("executor", executorAgent)
        .addTier("qa-review", qaAgent)
            .withQualityThreshold(0.8)  // Escalate if quality < 0.8
        .addTier("security-check", securityAgent)
            .withQualityThreshold(0.9)  // Escalate if quality < 0.9
        .addTier("final-approval", approvalAgent)
            .withHumanApprovalRequired()  // Requires human sign-off
        .withEscalationPolicy(EscalationPolicy.NEXT_TIER)  // Escalate to next tier
        .withAutoEscalateOnScore(0.7)  // Global threshold
        .withMaxEscalations(3)  // Max 3 escalations
        .build();

    System.out.println("✅ Supervisor chain built:");
    System.out.println("   - Chain ID: " + chain.getChainId());
    System.out.println("   - Tiers: " + chain.getTierCount());
    System.out.println("   - Escalation policy: " + chain.getEscalationPolicy());
    System.out.println("   - Max escalations: " + chain.getMaxEscalations());
    System.out.println();

    // Print tier details
    System.out.println("📋 Tier Details:");
    for (int i = 0; i < chain.getTierCount(); i++) {
      SupervisorChain.SupervisorTier tier = chain.getTier(i);
      System.out.printf("   Tier %d: %s (threshold: %.1f, human approval: %s)%n",
          i, tier.getTierName(), tier.getQualityThreshold(),
          tier.isRequiresHumanApproval());
    }
    System.out.println();

    // ==================== Step 3: Setup Tools ====================

    System.out.println("🔧 Step 3: Setting up tools...\n");

    ToolRegistry toolRegistry = ToolRegistry.builder()
        .registerTool("database-query", "Executes database queries")
        .registerTool("api-call", "Makes API calls to external services")
        .registerTool("file-processor", "Processes and validates files")
        .build();

    System.out.println("✅ Tool registry created");
    System.out.println();

    // ==================== Step 4: Create Agent Job ====================

    System.out.println("📦 Step 4: Creating agent job with supervisor chain...\n");

    AgentJob job = AgentJob.builder()
        .withId("quality-control-job")
        .withName("Quality Control Pipeline")
        .withSupervisorChain(chain)  // Use supervisor chain instead of single agent!
        .withToolRegistry(toolRegistry)
        .withAgenticFlinkConfig(AgenticFlinkConfig.forTesting())
        .build();

    System.out.println("✅ Agent job created:");
    System.out.println("   - Job ID: " + job.getJobId());
    System.out.println("   - Has supervisor chain: " + job.hasSupervisorChain());
    System.out.println("   - All agent IDs: " + job.getAllAgentIds());
    System.out.println();

    // ==================== Step 5: Generate Pipeline ====================

    System.out.println("🏗️  Step 5: Generating Flink pipeline...\n");

    AgentJobGenerator generator = new AgentJobGenerator(env, job);

    // Create input events with different quality scenarios
    DataStream<AgentEvent> inputEvents = env.fromElements(
        // High quality - should pass all tiers
        createTaskRequest("flow-001", "executor",
            "Create a simple CRUD API", 0.95),

        // Medium quality - will escalate to QA
        createTaskRequest("flow-002", "executor",
            "Implement complex authentication", 0.75),

        // Low quality - will escalate through multiple tiers
        createTaskRequest("flow-003", "executor",
            "Build payment processing system", 0.60)
    );

    // Generate pipeline - automatically routes through supervisor chain!
    DataStream<AgentEvent> results = generator.generate(inputEvents);

    System.out.println("✅ Pipeline generated with supervisor chain routing!");
    System.out.println("   - Tier 0 → Tier 1 escalation: ✓");
    System.out.println("   - Tier 1 → Tier 2 escalation: ✓");
    System.out.println("   - Tier 2 → Tier 3 escalation: ✓");
    System.out.println("   - Quality checks: ✓");
    System.out.println("   - Human approval: ✓");
    System.out.println();

    // Print results
    results.print();

    // ==================== Step 6: Execute ====================

    System.out.println("🚀 Step 6: Executing with escalation...\n");
    System.out.println("=".repeat(80));
    System.out.println("Watch for escalation events as quality scores trigger tier changes!");
    System.out.println("=".repeat(80));

    env.execute("Supervisor Chain Example");
  }

  /**
   * Creates a task request with simulated quality.
   */
  private static AgentEvent createTaskRequest(
      String flowId, String agentId, String task, double expectedQuality) {
    AgentEvent event = new AgentEvent(
        flowId,
        "user-001",
        agentId,
        AgentEventType.FLOW_STARTED
    );
    event.putData("user_message", task);
    event.putData("task", task);
    event.putData("expected_quality", expectedQuality);  // Simulated quality
    event.putMetadata("target_tier", 0);  // Start at tier 0
    return event;
  }

  /**
   * Key Takeaways:
   *
   * 1. Flexible N-tier chains - Any number of tiers, any agents
   * 2. Quality-based escalation - Automatic routing based on scores
   * 3. Policy-driven - Different escalation strategies
   * 4. Human-in-the-loop - Approval gates where needed
   * 5. Declarative - No manual CEP pattern writing!
   *
   * OLD: ~600 lines of custom CEP + state management
   * NEW: ~60 lines of declarative configuration
   *
   * Result: 10x less code, infinitely more flexible!
   */
}
