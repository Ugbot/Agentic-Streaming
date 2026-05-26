package org.agentic.flink.example;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.*;
import org.agentic.flink.stream.AgentExecutionStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Simple example demonstrating the agentic framework Usage: 1. Define tools 2. Configure agent 3.
 * Create event stream 4. Execute agent workflow
 */
public class SimpleAgentExample {

  public static void main(String[] args) throws Exception {

    // Setup Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration config = new Configuration();
    config.set(PipelineOptions.GENERIC_TYPES, false);
    config.set(PipelineOptions.AUTO_GENERATE_UIDS, false);
    env.configure(config);

    // Step 1: Define tools
    Map<String, ToolDefinition> toolRegistry = createToolRegistry();

    // Step 2: Configure agent
    AgentConfig agentConfig = createAgentConfig();

    // Step 3: Create agent execution stream
    AgentExecutionStream agentStream = new AgentExecutionStream(env, agentConfig, toolRegistry);

    // Step 4: Create input event stream (in real app, this would come from Kafka)
    DataStream<AgentEvent> inputEvents = createSampleEventStream(env);

    // Step 5: Execute agent workflow
    SingleOutputStreamOperator<AgentEvent> results = agentStream.createAgentStream(inputEvents);

    // Step 6: Print results
    results.print().name("agent-results");

    // Execute
    env.execute("Simple Agent Example");
  }

  private static Map<String, ToolDefinition> createToolRegistry() {
    Map<String, ToolDefinition> registry = new HashMap<>();

    // Tool 1: Calculator
    ToolDefinition calculator = new ToolDefinition("calculator", "Calculator", "Performs basic arithmetic operations");
    calculator.addInputParameter("operation", "string", "The operation to perform: add, subtract, multiply, divide", true);
    calculator.addInputParameter("operand1", "number", "First operand", true);
    calculator.addInputParameter("operand2", "number", "Second operand", true);
    calculator.setVersion("1.0");
    calculator.setRequiresApproval(false);
    registry.put("calculator", calculator);

    // Tool 2: Web Search
    ToolDefinition webSearch = new ToolDefinition("web_search", "Web Search", "Searches the web for information");
    webSearch.addInputParameter("query", "string", "Search query", true);
    webSearch.addInputParameter("max_results", "number", "Maximum number of results", false);
    webSearch.setVersion("1.0");
    webSearch.setRequiresApproval(false);
    registry.put("web_search", webSearch);

    // Tool 3: Data Analyzer
    ToolDefinition dataAnalyzer = new ToolDefinition("data_analyzer", "Data Analyzer", "Analyzes data and provides insights");
    dataAnalyzer.addInputParameter("data", "object", "Data to analyze", true);
    dataAnalyzer.addInputParameter("analysis_type", "string", "Type of analysis to perform", true);
    dataAnalyzer.setVersion("1.0");
    dataAnalyzer.setRequiresApproval(true); // Requires supervisor approval
    registry.put("data_analyzer", dataAnalyzer);

    return registry;
  }

  private static AgentConfig createAgentConfig() {
    AgentConfig config = new AgentConfig("agent-001", "Simple Tool Agent");
    config.setDescription("An agent that can use tools to complete tasks");

    // Add allowed tools
    config.addAllowedTool("calculator");
    config.addAllowedTool("web_search");
    config.addAllowedTool("data_analyzer");

    // LLM configuration
    config.setLlmModel("OLLAMA");
    config.setSystemPrompt("You are a helpful AI assistant that can use tools to complete tasks.");
    config.addLlmProperty("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    config.addLlmProperty("modelName", "llama3.1:latest");

    // Execution limits
    config.setMaxIterations(5);
    config.setExecutionTimeoutMs(300000L); // 5 minutes

    // Validation
    config.setEnableValidation(true);
    config.setValidationPrompt("Validate that the result is correct and complete.");

    // Auto-correction
    config.setEnableAutoCorrection(true);
    config.setMaxCorrectionAttempts(2);

    // Supervisor (disabled for simple example)
    config.setRequireSupervisor(false);

    return config;
  }

  private static DataStream<AgentEvent> createSampleEventStream(StreamExecutionEnvironment env) {
    // Create sample events demonstrating the workflow
    return env.fromElements(
            // Event 1: Start flow
            createFlowStartEvent("flow-001", "user-001", "agent-001"),

            // Event 2: Request calculator tool
            createToolCallEvent(
                "flow-001",
                "user-001",
                "agent-001",
                "calculator",
                Map.of("operation", "add", "operand1", 5, "operand2", 3)),

            // Event 3: Start second flow
            createFlowStartEvent("flow-002", "user-002", "agent-001"),

            // Event 4: Request web search tool
            createToolCallEvent(
                "flow-002",
                "user-002",
                "agent-001",
                "web_search",
                Map.of("query", "Apache Flink CEP examples", "max_results", 10)))
        .name("sample-input-events");
  }

  private static AgentEvent createFlowStartEvent(String flowId, String userId, String agentId) {
    AgentEvent event = new AgentEvent(flowId, userId, agentId, AgentEventType.FLOW_STARTED);
    event.setCurrentStage("STARTED");
    return event;
  }

  private static AgentEvent createToolCallEvent(
      String flowId,
      String userId,
      String agentId,
      String toolId,
      Map<String, Object> parameters) {
    AgentEvent event =
        new AgentEvent(flowId, userId, agentId, AgentEventType.TOOL_CALL_REQUESTED);
    event.setCurrentStage("TOOL_CALL");
    event.putData("toolId", toolId);
    event.putData("parameters", parameters);
    return event;
  }
}
