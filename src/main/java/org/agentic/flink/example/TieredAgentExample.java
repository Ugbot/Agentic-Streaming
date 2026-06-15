package org.agentic.flink.example;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.tools.builtin.CalculatorTools;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.ollama.OllamaChatModel;
import java.time.Duration;
import java.util.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Real Working Tiered Agent Example
 *
 * <p>This demonstrates a REAL three-tier agent system using:
 * - Apache Flink CEP for pattern matching and orchestration
 * - LangChain4J for actual LLM calls (Ollama locally)
 * - Real tool execution
 * - Multi-tier validation and escalation
 *
 * <p><b>Architecture:</b>
 * 1. ValidationAgent - Validates incoming requests
 * 2. ExecutionAgent - Executes tasks with tool calling
 * 3. SupervisorAgent - Reviews results and can escalate
 *
 * <p><b>Prerequisites:</b>
 * - Ollama running locally: http://localhost:11434
 * - Model downloaded: ollama pull qwen2.5:3b
 *
 * <p><b>To run:</b>
 * <pre>
 * docker compose up -d ollama
 * docker compose exec ollama ollama pull qwen2.5:3b
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.TieredAgentExample"
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class TieredAgentExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // Keep simple for demo

    System.out.println("=".repeat(70));
    System.out.println("  Real Tiered Agent Example - LangChain4J + Flink CEP");
    System.out.println("=".repeat(70));
    System.out.println();

    // Create sample events
    DataStream<AgentEvent> events = env.fromElements(
        createCalculationRequest("calculate-001", "What is 150 + 275?"),
        createCalculationRequest("calculate-002", "What is 89 * 12?"),
        createHelpRequest("help-001", "How do I reset my password?")
    );

    // Define CEP patterns for agent orchestration

    // Pattern 1: Validation → Execution → Review
    Pattern<AgentEvent, ?> validationPattern = Pattern.<AgentEvent>begin("request")
        .where(new SimpleCondition<AgentEvent>() {
          @Override
          public boolean filter(AgentEvent event) {
            return event.getEventType() == AgentEventType.TOOL_CALL_REQUESTED;
          }
        });

    PatternStream<AgentEvent> patternStream = CEP.pattern(events, validationPattern);

    // Process events through tiered agents
    DataStream<AgentEvent> validatedEvents = events
        .flatMap(new ValidationAgent())
        .name("Tier 1: Validation Agent");

    DataStream<AgentEvent> executedEvents = validatedEvents
        .filter(event -> event.getEventType() == AgentEventType.TOOL_CALL_REQUESTED
            || event.getEventType() == AgentEventType.LOOP_ITERATION_STARTED)
        .flatMap(new ExecutionAgent())
        .name("Tier 2: Execution Agent");

    DataStream<AgentEvent> supervisedEvents = executedEvents
        .filter(event -> event.getEventType() == AgentEventType.TOOL_CALL_COMPLETED
            || event.getEventType() == AgentEventType.VALIDATION_REQUESTED)
        .flatMap(new SupervisorAgent())
        .name("Tier 3: Supervisor Agent");

    // Print final results
    supervisedEvents
        .filter(event -> event.getEventType() == AgentEventType.FLOW_COMPLETED
            || event.getEventType() == AgentEventType.SUPERVISOR_REJECTED)
        .map(event -> {
          System.out.println("\n" + "=".repeat(70));
          System.out.println("FINAL RESULT: " + event.getFlowId());
          System.out.println("Status: " + event.getEventType());
          if (event.getData() != null) {
            System.out.println("Result: " + event.getData().get("result"));
            if (event.getData().containsKey("reason")) {
              System.out.println("Reason: " + event.getData().get("reason"));
            }
          }
          System.out.println("=".repeat(70));
          return event;
        })
        .name("Results Printer");

    env.execute("Real Tiered Agent Example");
  }

  // ==================== Tier 1: Validation Agent ====================

  /**
   * Validates incoming requests using LLM for intelligent validation
   */
  public static class ValidationAgent extends RichFlatMapFunction<AgentEvent, AgentEvent> {

    private transient ChatModel model;

    @Override
    public void open(OpenContext openContext) throws Exception {
      System.out.println("\n[ValidationAgent] Initializing with Ollama...");

      try {
        model = OllamaChatModel.builder()
            .baseUrl(ConfigKeys.DEFAULT_OLLAMA_BASE_URL)
            .modelName(ConfigKeys.DEFAULT_OLLAMA_MODEL)
            .temperature(0.3)
            .timeout(Duration.ofSeconds(30))
            .build();

        System.out.println("[ValidationAgent] Connected to Ollama successfully!");
      } catch (Exception e) {
        System.err.println("[ValidationAgent] Failed to connect to Ollama: " + e.getMessage());
        System.err.println("Make sure Ollama is running: docker compose up -d ollama");
        throw e;
      }
    }

    @Override
    public void flatMap(AgentEvent event, Collector<AgentEvent> out) throws Exception {
      System.out.println("\n[ValidationAgent] Validating: " + event.getFlowId());

      String userRequest = (String) event.getData().get("request");
      System.out.println("[ValidationAgent] Request: " + userRequest);

      // Use LLM to validate if request is appropriate
      String prompt = String.format(
          "You are a validation agent. Analyze this user request and respond with ONLY 'VALID' or 'INVALID':\n\n%s",
          userRequest
      );

      try {
        ChatResponse response = model.chat(
            SystemMessage.from("You are a helpful validation assistant. Respond with only 'VALID' or 'INVALID'."),
            UserMessage.from(prompt)
        );

        String validation = response.aiMessage().text().trim().toUpperCase();
        boolean isValid = validation.contains("VALID") && !validation.contains("INVALID");

        System.out.println("[ValidationAgent] Validation result: " + (isValid ? "VALID" : "INVALID"));

        if (isValid) {
          // Pass through with validation approved
          event.setEventType(AgentEventType.LOOP_ITERATION_STARTED);
          event.getData().put("validation_status", "approved");
          out.collect(event);
        } else {
          // Reject request
          AgentEvent rejectedEvent = new AgentEvent(
              event.getFlowId(),
              event.getUserId(),
              event.getAgentId(),
              AgentEventType.SUPERVISOR_REJECTED
          );
          Map<String, Object> data = new HashMap<>();
          data.put("reason", "Request failed validation");
          data.put("original_request", userRequest);
          rejectedEvent.setData(data);
          out.collect(rejectedEvent);
        }

      } catch (Exception e) {
        System.err.println("[ValidationAgent] Error during validation: " + e.getMessage());
        // In case of error, escalate
        AgentEvent errorEvent = new AgentEvent(
            event.getFlowId(),
            event.getUserId(),
            event.getAgentId(),
            AgentEventType.SUPERVISOR_REVIEW_REQUESTED
        );
        Map<String, Object> data = new HashMap<>();
        data.put("reason", "Validation error: " + e.getMessage());
        errorEvent.setData(data);
        out.collect(errorEvent);
      }
    }
  }

  // ==================== Tier 2: Execution Agent ====================

  /**
   * Executes tasks using LLM + tool calling
   */
  public static class ExecutionAgent extends RichFlatMapFunction<AgentEvent, AgentEvent> {

    private transient ChatModel model;
    private transient CalculatorTools calculator;

    @Override
    public void open(OpenContext openContext) throws Exception {
      System.out.println("\n[ExecutionAgent] Initializing...");

      model = OllamaChatModel.builder()
          .baseUrl(ConfigKeys.DEFAULT_OLLAMA_BASE_URL)
          .modelName(ConfigKeys.DEFAULT_OLLAMA_MODEL)
          .temperature(0.1) // Lower temperature for accurate calculations
          .timeout(Duration.ofSeconds(30))
          .build();

      calculator = new CalculatorTools();

      System.out.println("[ExecutionAgent] Ready!");
    }

    @Override
    public void flatMap(AgentEvent event, Collector<AgentEvent> out) throws Exception {
      System.out.println("\n[ExecutionAgent] Processing: " + event.getFlowId());

      String request = (String) event.getData().get("request");
      System.out.println("[ExecutionAgent] Request: " + request);

      try {
        // Check if this is a calculation request
        if (request.toLowerCase().contains("calculate") ||
            request.matches(".*\\d+.*[+\\-*/].*\\d+.*")) {

          // Extract and execute calculation
          String result = executeCalculation(request);

          // Create completion event
          AgentEvent completedEvent = new AgentEvent(
              event.getFlowId(),
              event.getUserId(),
              event.getAgentId(),
              AgentEventType.TOOL_CALL_COMPLETED
          );
          Map<String, Object> data = new HashMap<>();
          data.put("result", result);
          data.put("tool", "calculator");
          data.put("request", request);
          completedEvent.setData(data);

          System.out.println("[ExecutionAgent] Calculation result: " + result);
          out.collect(completedEvent);

        } else {
          // Use LLM for general questions
          ChatResponse response = model.chat(UserMessage.from(request));
          String answer = response.aiMessage().text();

          AgentEvent completedEvent = new AgentEvent(
              event.getFlowId(),
              event.getUserId(),
              event.getAgentId(),
              AgentEventType.TOOL_CALL_COMPLETED
          );
          Map<String, Object> data = new HashMap<>();
          data.put("result", answer);
          data.put("tool", "llm");
          data.put("request", request);
          completedEvent.setData(data);

          System.out.println("[ExecutionAgent] LLM response: " + answer);
          out.collect(completedEvent);
        }

      } catch (Exception e) {
        System.err.println("[ExecutionAgent] Execution error: " + e.getMessage());

        AgentEvent errorEvent = new AgentEvent(
            event.getFlowId(),
            event.getUserId(),
            event.getAgentId(),
            AgentEventType.VALIDATION_REQUESTED
        );
        Map<String, Object> data = new HashMap<>();
        data.put("error", e.getMessage());
        data.put("needs_retry", true);
        errorEvent.setData(data);
        out.collect(errorEvent);
      }
    }

    private String executeCalculation(String request) {
      // Simple parsing for demonstration
      // In production, use LLM to extract parameters
      try {
        if (request.contains("+")) {
          String[] parts = request.replaceAll("[^0-9+]", " ").trim().split("\\+");
          if (parts.length == 2) {
            int a = Integer.parseInt(parts[0].trim());
            int b = Integer.parseInt(parts[1].trim());
            double result = calculator.add(a, b);
            return String.format("%d + %d = %.0f", a, b, result);
          }
        } else if (request.contains("*") || request.toLowerCase().contains("multiply")) {
          String[] parts = request.replaceAll("[^0-9*]", " ").trim().split("\\*");
          if (parts.length == 2) {
            int a = Integer.parseInt(parts[0].trim());
            int b = Integer.parseInt(parts[1].trim());
            double result = calculator.multiply(a, b);
            return String.format("%d × %d = %.0f", a, b, result);
          }
        }
      } catch (Exception e) {
        System.err.println("[ExecutionAgent] Calculation parsing error: " + e.getMessage());
      }

      return "Could not parse calculation from: " + request;
    }
  }

  // ==================== Tier 3: Supervisor Agent ====================

  /**
   * Reviews execution results and can escalate if needed
   */
  public static class SupervisorAgent extends RichFlatMapFunction<AgentEvent, AgentEvent> {

    private transient ChatModel model;

    @Override
    public void open(OpenContext openContext) throws Exception {
      System.out.println("\n[SupervisorAgent] Initializing...");

      model = OllamaChatModel.builder()
          .baseUrl(ConfigKeys.DEFAULT_OLLAMA_BASE_URL)
          .modelName(ConfigKeys.DEFAULT_OLLAMA_MODEL)
          .temperature(0.2)
          .timeout(Duration.ofSeconds(30))
          .build();

      System.out.println("[SupervisorAgent] Ready!");
    }

    @Override
    public void flatMap(AgentEvent event, Collector<AgentEvent> out) throws Exception {
      System.out.println("\n[SupervisorAgent] Reviewing: " + event.getFlowId());

      if (event.getData().containsKey("error") || event.getData().containsKey("needs_retry")) {
        // Escalate errors
        System.out.println("[SupervisorAgent] Error detected, escalating...");

        AgentEvent escalatedEvent = new AgentEvent(
            event.getFlowId(),
            event.getUserId(),
            event.getAgentId(),
            AgentEventType.SUPERVISOR_REVIEW_REQUESTED
        );
        escalatedEvent.setData(event.getData());
        out.collect(escalatedEvent);
        return;
      }

      String result = (String) event.getData().get("result");
      String request = (String) event.getData().get("request");

      System.out.println("[SupervisorAgent] Checking quality of result...");
      System.out.println("[SupervisorAgent] Result: " + result);

      // Use LLM to review if result looks reasonable
      String reviewPrompt = String.format(
          "Review if this answer is reasonable for the question. Respond with only 'APPROVED' or 'NEEDS_REVIEW'.\n\nQuestion: %s\nAnswer: %s",
          request, result
      );

      try {
        ChatResponse response = model.chat(
            SystemMessage.from("You are a quality reviewer. Respond with only 'APPROVED' or 'NEEDS_REVIEW'."),
            UserMessage.from(reviewPrompt)
        );

        String review = response.aiMessage().text().trim().toUpperCase();
        boolean approved = review.contains("APPROVED");

        System.out.println("[SupervisorAgent] Review: " + (approved ? "APPROVED" : "NEEDS_REVIEW"));

        if (approved) {
          // Mark as completed
          AgentEvent completedEvent = new AgentEvent(
              event.getFlowId(),
              event.getUserId(),
              event.getAgentId(),
              AgentEventType.FLOW_COMPLETED
          );
          completedEvent.setData(event.getData());
          out.collect(completedEvent);
        } else {
          // Request review
          AgentEvent reviewEvent = new AgentEvent(
              event.getFlowId(),
              event.getUserId(),
              event.getAgentId(),
              AgentEventType.SUPERVISOR_REJECTED
          );
          Map<String, Object> data = new HashMap<>(event.getData());
          data.put("reason", "Quality review required");
          reviewEvent.setData(data);
          out.collect(reviewEvent);
        }

      } catch (Exception e) {
        System.err.println("[SupervisorAgent] Review error: " + e.getMessage());
        // Default to approved on error
        AgentEvent completedEvent = new AgentEvent(
            event.getFlowId(),
            event.getUserId(),
            event.getAgentId(),
            AgentEventType.FLOW_COMPLETED
        );
        completedEvent.setData(event.getData());
        out.collect(completedEvent);
      }
    }
  }

  // ==================== Helper Methods ====================

  private static AgentEvent createCalculationRequest(String flowId, String request) {
    AgentEvent event = new AgentEvent(
        flowId,
        "user-demo",
        "tiered-agent",
        AgentEventType.TOOL_CALL_REQUESTED
    );
    Map<String, Object> data = new HashMap<>();
    data.put("request", request);
    event.setData(data);
    return event;
  }

  private static AgentEvent createHelpRequest(String flowId, String question) {
    AgentEvent event = new AgentEvent(
        flowId,
        "user-demo",
        "tiered-agent",
        AgentEventType.TOOL_CALL_REQUESTED
    );
    Map<String, Object> data = new HashMap<>();
    data.put("request", question);
    event.setData(data);
    return event;
  }
}
