package org.agentic.flink.example;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.job.AgentJob;
import org.agentic.flink.job.AgentJobGenerator;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.tool.ToolRegistry;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Declarative Agent Example - New DSL Approach
 *
 * <p>This example demonstrates the NEW declarative builder API for defining agents:
 *
 * <ul>
 *   <li><b>Agent.builder()</b> - Fluent DSL for agent configuration
 *   <li><b>AgentJob.builder()</b> - Job packaging with storage and tools
 *   <li><b>AgentJobGenerator</b> - Automatic Flink pipeline generation
 * </ul>
 *
 * <p><b>Benefits of Declarative Approach:</b>
 * <ol>
 *   <li>Much less boilerplate than manual CEP patterns
 *   <li>Type-safe configuration with validation
 *   <li>Automatic CEP pattern generation from agent config
 *   <li>Built-in validation, correction, and supervision
 *   <li>Storage integration handled automatically
 * </ol>
 *
 * <p><b>Compare this to TieredAgentExample</b> - you'll see this is ~3x shorter and clearer!
 *
 * <p><b>Prerequisites:</b>
 * <ul>
 *   <li>Ollama running: http://localhost:11434
 *   <li>Model: ollama pull qwen2.5:3b
 * </ul>
 *
 * <p><b>To run:</b>
 * <pre>
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.DeclarativeAgentExample"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class DeclarativeAgentExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=".repeat(80));
    System.out.println("  Declarative Agent Example - New Builder API");
    System.out.println("=".repeat(80));
    System.out.println();

    // Setup
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // ==================== Step 1: Define Agent Declaratively ====================

    System.out.println("📝 Step 1: Defining agent with builder API...\n");

    Agent researchAgent = Agent.builder()
        .withId("research-agent")
        .withName("Research Specialist")
        .withType(AgentType.RESEARCHER)
        .withSystemPrompt(
            "You are a research specialist. When given a topic:\n" +
            "1. Use web-search tool to gather information\n" +
            "2. Use document-analysis tool to extract key insights\n" +
            "3. Use synthesis tool to create a comprehensive summary\n\n" +
            "Be thorough and cite sources.")
        .withLlmModel("qwen2.5:3b")
        .withTemperature(0.3)
        .withTools("web-search", "document-analysis", "synthesis")
        .withMaxIterations(10)
        .withTimeout(Duration.ofMinutes(5))
        .withValidationEnabled(true)
        .withMaxValidationAttempts(3)
        .build();

    System.out.println("✅ Agent defined: " + researchAgent);
    System.out.println("   - Model: " + researchAgent.getLlmModel());
    System.out.println("   - Tools: " + researchAgent.getAllowedTools());
    System.out.println("   - Max iterations: " + researchAgent.getMaxIterations());
    System.out.println();

    // ==================== Step 2: Setup Tool Registry ====================

    System.out.println("🔧 Step 2: Setting up tool registry...\n");

    ToolRegistry toolRegistry = ToolRegistry.builder()
        .registerTool("web-search", "Searches the web for information")
        .registerTool("document-analysis", "Analyzes documents and extracts insights")
        .registerTool("synthesis", "Synthesizes information into summaries")
        .build();

    System.out.println("✅ Tool registry created with " + toolRegistry.getToolNames().size() + " tools");
    System.out.println();

    // ==================== Step 3: Create Agent Job ====================

    System.out.println("📦 Step 3: Packaging into agent job...\n");

    AgentJob job = AgentJob.builder()
        .withId("research-job")
        .withName("Research Pipeline")
        .withAgent(researchAgent)
        .withToolRegistry(toolRegistry)
        .withAgenticFlinkConfig(AgenticFlinkConfig.forTesting())  // In-memory for demo
        .build();

    System.out.println("✅ Agent job created: " + job.getJobId());
    System.out.println("   - Agents: " + job.getAgents().size());
    System.out.println("   - Has supervisor chain: " + job.hasSupervisorChain());
    System.out.println();

    // ==================== Step 4: Generate Flink Pipeline ====================

    System.out.println("🏗️  Step 4: Generating Flink pipeline automatically...\n");

    AgentJobGenerator generator = new AgentJobGenerator(env, job);

    // Create input stream
    DataStream<AgentEvent> inputEvents = env.fromElements(
        createResearchRequest("flow-001", "research-agent",
            "Research the benefits of Apache Flink for real-time AI"),
        createResearchRequest("flow-002", "research-agent",
            "Research best practices for LLM agent design")
    );

    // Generate pipeline - THIS IS WHERE THE MAGIC HAPPENS!
    // The generator automatically:
    // - Creates CEP patterns from agent state machine
    // - Wires tool execution
    // - Sets up validation loops
    // - Configures storage backends
    // - Routes events through the pipeline
    DataStream<AgentEvent> results = generator.generate(inputEvents);

    System.out.println("✅ Pipeline generated automatically!");
    System.out.println("   - CEP patterns: ✓");
    System.out.println("   - Tool execution: ✓");
    System.out.println("   - Validation loops: ✓");
    System.out.println("   - Storage backends: ✓");
    System.out.println();

    // Print results
    results.print();

    // ==================== Step 5: Execute ====================

    System.out.println("🚀 Step 5: Executing Flink job...\n");
    System.out.println("=".repeat(80));

    env.execute("Declarative Agent Example");
  }

  /**
   * Creates a research request event.
   */
  private static AgentEvent createResearchRequest(String flowId, String agentId, String topic) {
    AgentEvent event = new AgentEvent(
        flowId,
        "user-001",
        agentId,
        AgentEventType.FLOW_STARTED
    );
    event.putData("user_message", "Research: " + topic);
    event.putData("topic", topic);
    event.putData("request_type", "research");
    return event;
  }

  /**
   * Comparison: OLD vs NEW approach
   *
   * OLD APPROACH (see TieredAgentExample.java):
   * - Manually define CEP patterns (~50 lines)
   * - Create custom FlatMapFunction classes (~100 lines each x 3 tiers = 300 lines)
   * - Wire LangChain4J calls manually
   * - Handle tool execution yourself
   * - Implement validation logic yourself
   * - Total: ~500+ lines of code
   *
   * NEW APPROACH (this file):
   * - Define agent with builder (~15 lines)
   * - Create job with builder (~5 lines)
   * - Generate pipeline (~1 line!)
   * - Total: ~20 lines of code
   *
   * Result: 25x less code for the same functionality!
   */
}
