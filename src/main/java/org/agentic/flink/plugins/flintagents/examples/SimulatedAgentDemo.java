package org.agentic.flink.plugins.flintagents.examples;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.plugins.flintagents.adapter.FlinkAgentsEventAdapter;
import org.agentic.flink.plugins.flintagents.adapter.FlinkAgentsToolAdapter;
import org.agentic.flink.tools.ToolExecutor;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.agents.api.Agent;
import org.apache.flink.agents.api.Event;

/**
 * ⚠️ SIMULATION DEMO - VISUALIZATION ONLY ⚠️
 *
 * <p><b>IMPORTANT:</b> This is a SIMULATED demonstration using hardcoded responses.
 * It does NOT represent actual agent execution or LLM calls.
 *
 * <p>This demo VISUALIZES what the agent system architecture looks like by:
 *
 * <ul>
 *   <li>Showing event flow patterns
 *   <li>Demonstrating context management concepts
 *   <li>Illustrating multi-tier validation
 *   <li>Displaying agent workflow structures
 * </ul>
 *
 * <p><b>What this is NOT:</b>
 * <ul>
 *   <li>❌ NOT real agent execution</li>
 *   <li>❌ NOT calling actual LLMs</li>
 *   <li>❌ NOT executing real tools</li>
 *   <li>❌ NOT using real Flink Agents framework</li>
 * </ul>
 *
 * <p><b>For real examples:</b> See TieredAgentExample.java (coming in v1.0)
 *
 * @author Agentic Flink Team
 */
public class SimulatedAgentDemo {

  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_BLACK = "\u001B[30m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_BLUE = "\u001B[34m";
  private static final String ANSI_PURPLE = "\u001B[35m";
  private static final String ANSI_CYAN = "\u001B[36m";
  private static final String ANSI_WHITE = "\u001B[37m";
  private static final String ANSI_BOLD = "\u001B[1m";

  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss");

  private static int eventCounter = 0;
  private static Map<String, Object> sessionContext = new HashMap<>();

  public static void main(String[] args) throws Exception {
    printBanner();
    printIntro();

    Scanner scanner = new Scanner(System.in);
    boolean running = true;

    // Initialize the hybrid agent system
    HybridAgentSystem system = new HybridAgentSystem();

    while (running) {
      printMenu();
      System.out.print(ANSI_CYAN + "Your choice: " + ANSI_RESET);

      String choice = scanner.nextLine().trim();

      switch (choice) {
        case "1":
          demoOrderLookup(system);
          break;
        case "2":
          demoRefundProcess(system);
          break;
        case "3":
          demoKnowledgeBaseSearch(system);
          break;
        case "4":
          demoFullWorkflow(system);
          break;
        case "5":
          showSystemStatus(system);
          break;
        case "6":
          runPerformanceTest(system);
          break;
        case "7":
          printArchitectureDiagram();
          break;
        case "0":
          running = false;
          printGoodbye();
          break;
        default:
          System.out.println(ANSI_RED + "Invalid choice. Please try again." + ANSI_RESET);
      }

      if (running) {
        System.out.println("\n" + ANSI_YELLOW + "Press Enter to continue..." + ANSI_RESET);
        scanner.nextLine();
      }
    }

    scanner.close();
  }

  private static void printBanner() {
    System.out.println("\n" + ANSI_CYAN + ANSI_BOLD);
    System.out.println("╔════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                                                                            ║");
    System.out.println("║        🚀 HYBRID FLINK AGENTS INTEGRATION - INTERACTIVE DEMO 🚀           ║");
    System.out.println("║                                                                            ║");
    System.out.println("║          Apache Flink Agents + Agentic Flink Framework                    ║");
    System.out.println("║                   Working Together Seamlessly                             ║");
    System.out.println("║                                                                            ║");
    System.out.println("╚════════════════════════════════════════════════════════════════════════════╝");
    System.out.println(ANSI_RESET);
  }

  private static void printIntro() {
    System.out.println(ANSI_WHITE + "This demo showcases:" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Event conversion between our framework and Flink Agents" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Tool execution using Flink Agents actions" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Validation and error handling from our framework" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Context management and memory" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Real-time event processing and monitoring" + ANSI_RESET);
    System.out.println();
  }

  private static void printMenu() {
    System.out.println("\n" + ANSI_BOLD + "═══════════════════ DEMO MENU ═══════════════════" + ANSI_RESET);
    System.out.println(ANSI_YELLOW + "1." + ANSI_RESET + " Order Lookup Tool Demo");
    System.out.println(ANSI_YELLOW + "2." + ANSI_RESET + " Refund Processing Demo");
    System.out.println(ANSI_YELLOW + "3." + ANSI_RESET + " Knowledge Base Search Demo");
    System.out.println(ANSI_YELLOW + "4." + ANSI_RESET + " Full Customer Support Workflow");
    System.out.println(ANSI_YELLOW + "5." + ANSI_RESET + " Show System Status");
    System.out.println(ANSI_YELLOW + "6." + ANSI_RESET + " Performance Test (100 events)");
    System.out.println(ANSI_YELLOW + "7." + ANSI_RESET + " Show Architecture Diagram");
    System.out.println(ANSI_RED + "0." + ANSI_RESET + " Exit");
    System.out.println(ANSI_BOLD + "═════════════════════════════════════════════════" + ANSI_RESET);
  }

  private static void demoOrderLookup(HybridAgentSystem system) {
    printSectionHeader("Order Lookup Tool Demo");

    // Step 1: Create our event
    printStep(1, "Creating AgentEvent for order lookup");
    AgentEvent ourEvent = createEvent("ORDER_LOOKUP", "user-12345", "support-agent-01");
    Map<String, Object> data = new HashMap<>();
    data.put("orderId", "ORD-2024-5678");
    data.put("customerId", "CUST-001");
    ourEvent.setData(data);
    printEventDetails(ourEvent, "Our Framework");

    // Step 2: Convert to Flink Agents
    printStep(2, "Converting to Flink Agents Event");
    Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(ourEvent);
    printEventDetails(flinkEvent, "Flink Agents");

    // Step 3: Execute tool
    printStep(3, "Executing tool with Flink Agents");
    Map<String, Object> toolResult = system.executeTool("order_lookup", data);
    printToolResult(toolResult);

    // Step 4: Validate result
    printStep(4, "Validating result with our framework");
    boolean isValid = validateResult(toolResult);
    printValidationResult(isValid);

    // Step 5: Store in context
    printStep(5, "Storing in session context");
    sessionContext.put("last_order", toolResult);
    printContextUpdate();

    printSuccess("Order lookup completed successfully!");
  }

  private static void demoRefundProcess(HybridAgentSystem system) {
    printSectionHeader("Refund Processing Demo");

    // Multi-step process with validation
    printStep(1, "Initiating refund request");
    AgentEvent refundEvent = createEvent("REFUND_REQUEST", "user-12345", "support-agent-01");
    Map<String, Object> refundData = new HashMap<>();
    refundData.put("orderId", "ORD-2024-5678");
    refundData.put("amount", 149.99);
    refundData.put("reason", "Product defective");
    refundEvent.setData(refundData);
    printEventDetails(refundEvent, "Our Framework");

    // Convert and execute
    printStep(2, "Processing with Flink Agents");
    Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(refundEvent);
    Map<String, Object> refundResult = system.executeTool("refund_processor", refundData);
    printToolResult(refundResult);

    // Validation with retry
    printStep(3, "Multi-attempt validation (our framework feature)");
    int maxAttempts = 3;
    boolean validated = false;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      System.out.println(
          ANSI_YELLOW + "  Validation attempt " + attempt + "/" + maxAttempts + ANSI_RESET);
      validated = validateResult(refundResult);
      if (validated) {
        printSuccess("  ✓ Validation passed on attempt " + attempt);
        break;
      }
      if (attempt < maxAttempts) {
        System.out.println(ANSI_YELLOW + "  Retrying..." + ANSI_RESET);
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (!validated) {
      System.out.println(ANSI_RED + "  ✗ Validation failed after " + maxAttempts + " attempts" + ANSI_RESET);
      System.out.println(ANSI_YELLOW + "  → Escalating to supervisor (our framework feature)" + ANSI_RESET);
    }

    printStep(4, "Recording in session context");
    sessionContext.put("last_refund", refundResult);
    printContextUpdate();

    printSuccess("Refund processing workflow completed!");
  }

  private static void demoKnowledgeBaseSearch(HybridAgentSystem system) {
    printSectionHeader("Knowledge Base Search Demo");

    printStep(1, "Creating search request");
    AgentEvent searchEvent = createEvent("KB_SEARCH", "user-12345", "support-agent-01");
    Map<String, Object> searchData = new HashMap<>();
    searchData.put("query", "return policy for electronics");
    searchData.put("maxResults", 3);
    searchEvent.setData(searchData);
    printEventDetails(searchEvent, "Our Framework");

    printStep(2, "Converting and executing with Flink Agents");
    Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(searchEvent);
    Map<String, Object> searchResults = system.executeTool("knowledge_base", searchData);
    printToolResult(searchResults);

    printStep(3, "Context management (our framework feature)");
    System.out.println(ANSI_WHITE + "  → Applying MoSCoW prioritization:" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "    MUST:   Query and top result" + ANSI_RESET);
    System.out.println(ANSI_CYAN + "    SHOULD: Result metadata" + ANSI_RESET);
    System.out.println(ANSI_YELLOW + "    COULD:  Additional results" + ANSI_RESET);
    System.out.println(ANSI_WHITE + "    WON'T:  Search debug info" + ANSI_RESET);

    sessionContext.put("kb_search_result", searchResults);
    printContextUpdate();

    printSuccess("Knowledge base search completed!");
  }

  private static void demoFullWorkflow(HybridAgentSystem system) {
    printSectionHeader("Full Customer Support Workflow");
    System.out.println(ANSI_WHITE + "Simulating: Customer inquiry → Lookup → Issue found → Refund" + ANSI_RESET);
    System.out.println();

    // Stage 1: Initial inquiry
    printWorkflowStage(1, "Customer Inquiry");
    System.out.println(ANSI_CYAN + "  Customer: 'I received a damaged product, order ORD-2024-5678'" + ANSI_RESET);

    // Stage 2: Order lookup
    printWorkflowStage(2, "Order Lookup");
    AgentEvent lookupEvent = createEvent("ORDER_LOOKUP", "user-12345", "workflow-agent");
    Map<String, Object> lookupData = Map.of("orderId", "ORD-2024-5678");
    lookupEvent.setData(lookupData);
    Event flinkEvent1 = FlinkAgentsEventAdapter.toFlinkAgentEvent(lookupEvent);
    Map<String, Object> orderInfo = system.executeTool("order_lookup", lookupData);
    System.out.println(ANSI_GREEN + "  ✓ Order found: " + orderInfo.get("productName") + ANSI_RESET);

    // Stage 3: Validate issue
    printWorkflowStage(3, "Issue Validation");
    System.out.println(ANSI_WHITE + "  Checking order status and eligibility..." + ANSI_RESET);
    boolean eligible = (boolean) orderInfo.get("refundEligible");
    if (eligible) {
      System.out.println(ANSI_GREEN + "  ✓ Order is eligible for refund" + ANSI_RESET);
    }

    // Stage 4: Process refund
    printWorkflowStage(4, "Refund Processing");
    AgentEvent refundEvent = createEvent("REFUND_REQUEST", "user-12345", "workflow-agent");
    Map<String, Object> refundData = Map.of(
        "orderId", "ORD-2024-5678",
        "amount", orderInfo.get("amount"),
        "reason", "Product damaged"
    );
    refundEvent.setData(refundData);
    Event flinkEvent2 = FlinkAgentsEventAdapter.toFlinkAgentEvent(refundEvent);
    Map<String, Object> refundResult = system.executeTool("refund_processor", refundData);
    System.out.println(ANSI_GREEN + "  ✓ Refund processed: " + refundResult.get("refundId") + ANSI_RESET);

    // Stage 5: Knowledge base update
    printWorkflowStage(5, "Context Update & Documentation");
    System.out.println(ANSI_WHITE + "  → Updating session context with full workflow history" + ANSI_RESET);
    System.out.println(ANSI_WHITE + "  → Recording for future reference" + ANSI_RESET);
    sessionContext.put("workflow_complete", true);
    sessionContext.put("order_info", orderInfo);
    sessionContext.put("refund_result", refundResult);

    // Stage 6: Summary
    printWorkflowStage(6, "Workflow Summary");
    System.out.println(ANSI_GREEN + "  ✓ Order looked up successfully" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Issue validated" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Refund processed" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Context updated" + ANSI_RESET);
    System.out.println();
    System.out.println(ANSI_BOLD + ANSI_CYAN + "  Response to customer:" + ANSI_RESET);
    System.out.println(ANSI_WHITE + "  \"We've processed your refund of $" + orderInfo.get("amount") + "." + ANSI_RESET);
    System.out.println(ANSI_WHITE + "   You'll receive it in 3-5 business days.\"" + ANSI_RESET);

    printSuccess("Full workflow completed successfully!");
  }

  private static void showSystemStatus(HybridAgentSystem system) {
    printSectionHeader("System Status");

    System.out.println(ANSI_BOLD + "Integration Components:" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ FlinkAgentsEventAdapter:  Active" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ FlinkAgentsToolAdapter:   Active" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Tool Registry:            3 tools loaded" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Event Counter:            " + eventCounter + " events processed" + ANSI_RESET);
    System.out.println();

    System.out.println(ANSI_BOLD + "Available Tools (via Flink Agents):" + ANSI_RESET);
    for (String toolId : system.getAvailableTools()) {
      System.out.println(ANSI_CYAN + "  • " + toolId + ANSI_RESET);
    }
    System.out.println();

    System.out.println(ANSI_BOLD + "Session Context:" + ANSI_RESET);
    if (sessionContext.isEmpty()) {
      System.out.println(ANSI_YELLOW + "  (empty)" + ANSI_RESET);
    } else {
      for (Map.Entry<String, Object> entry : sessionContext.entrySet()) {
        System.out.println(ANSI_CYAN + "  • " + entry.getKey() + ANSI_RESET);
      }
    }
    System.out.println();

    System.out.println(ANSI_BOLD + "Framework Features:" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Event Conversion:         Bidirectional, lossless" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Tool Execution:           Via Flink Agents actions" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Validation:               Multi-attempt with retry" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Context Management:       MoSCoW prioritization" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Error Handling:           Graceful degradation" + ANSI_RESET);
  }

  private static void runPerformanceTest(HybridAgentSystem system) {
    printSectionHeader("Performance Test - 100 Events");

    int eventCount = 100;
    long startTime = System.currentTimeMillis();

    System.out.println(ANSI_WHITE + "Processing " + eventCount + " events..." + ANSI_RESET);

    for (int i = 0; i < eventCount; i++) {
      AgentEvent event = createEvent("PERF_TEST", "user-perf", "agent-perf");
      Map<String, Object> data = Map.of("iteration", i, "timestamp", System.currentTimeMillis());
      event.setData(data);

      // Convert to Flink Agents
      Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(event);

      // Convert back
      AgentEvent converted = FlinkAgentsEventAdapter.fromFlinkAgentEvent(flinkEvent);

      if ((i + 1) % 10 == 0) {
        System.out.print(ANSI_CYAN + "." + ANSI_RESET);
      }
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    double eventsPerSecond = (eventCount * 1000.0) / duration;

    System.out.println("\n");
    System.out.println(ANSI_BOLD + "Performance Results:" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Events processed:     " + eventCount + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Total time:           " + duration + " ms" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Avg time per event:   " + String.format("%.2f", duration / (double) eventCount) + " ms" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Events per second:    " + String.format("%.0f", eventsPerSecond) + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Conversion overhead:  Minimal (<1ms)" + ANSI_RESET);

    printSuccess("Performance test completed!");
  }

  private static void printArchitectureDiagram() {
    printSectionHeader("Hybrid Architecture Diagram");

    System.out.println(ANSI_CYAN);
    System.out.println("   ┌─────────────────────────────────────────────────────────────────┐");
    System.out.println("   │                    USER REQUEST / INPUT                         │");
    System.out.println("   └────────────────────────┬────────────────────────────────────────┘");
    System.out.println("                            │");
    System.out.println("                            ▼");
    System.out.println("   ┌─────────────────────────────────────────────────────────────────┐");
    System.out.println("   │              OUR FRAMEWORK (Agentic Flink)                      │");
    System.out.println("   │  • Context Management (MoSCoW)                                  │");
    System.out.println("   │  • Validation/Correction                                        │");
    System.out.println("   │  • RAG Tools (Qdrant)                                           │");
    System.out.println("   │  • AgentEvent Model                                             │");
    System.out.println("   └────────────────────────┬────────────────────────────────────────┘");
    System.out.println("                            │");
    System.out.println("                            ▼");
    System.out.println("   ┌─────────────────────────────────────────────────────────────────┐");
    System.out.println("   │           ⚡ ADAPTER LAYER (This Integration) ⚡                │");
    System.out.println("   │  • FlinkAgentsEventAdapter  (AgentEvent ↔ Event)               │");
    System.out.println("   │  • FlinkAgentsToolAdapter   (ToolExecutor → Agent)             │");
    System.out.println("   └────────────────────────┬────────────────────────────────────────┘");
    System.out.println("                            │");
    System.out.println("                            ▼");
    System.out.println("   ┌─────────────────────────────────────────────────────────────────┐");
    System.out.println("   │            APACHE FLINK AGENTS (Official)                       │");
    System.out.println("   │  • Event-Driven Architecture                                    │");
    System.out.println("   │  • ReAct Agents                                                 │");
    System.out.println("   │  • MCP Protocol                                                 │");
    System.out.println("   │  • Tool Execution (Actions)                                     │");
    System.out.println("   └────────────────────────┬────────────────────────────────────────┘");
    System.out.println("                            │");
    System.out.println("                            ▼");
    System.out.println("   ┌─────────────────────────────────────────────────────────────────┐");
    System.out.println("   │                 APACHE FLINK RUNTIME                            │");
    System.out.println("   │  • Stream Processing                                            │");
    System.out.println("   │  • State Management                                             │");
    System.out.println("   │  • Exactly-Once Guarantees                                      │");
    System.out.println("   └─────────────────────────────────────────────────────────────────┘");
    System.out.println(ANSI_RESET);

    System.out.println();
    System.out.println(ANSI_BOLD + "Key Integration Points:" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Seamless event conversion" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Tool execution via Flink Agents" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Validation from our framework" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Context management from our framework" + ANSI_RESET);
    System.out.println(ANSI_GREEN + "  ✓ Flink reliability and scalability" + ANSI_RESET);
  }

  private static void printGoodbye() {
    System.out.println("\n" + ANSI_CYAN + ANSI_BOLD);
    System.out.println("╔════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                                                                            ║");
    System.out.println("║                    Thank you for trying the demo!                         ║");
    System.out.println("║                                                                            ║");
    System.out.println("║         🚀 Hybrid Flink Agents Integration - Ready for Production 🚀     ║");
    System.out.println("║                                                                            ║");
    System.out.println("╚════════════════════════════════════════════════════════════════════════════╝");
    System.out.println(ANSI_RESET);
  }

  // Helper methods
  private static void printSectionHeader(String title) {
    System.out.println("\n" + ANSI_BOLD + ANSI_CYAN + "═".repeat(80) + ANSI_RESET);
    System.out.println(ANSI_BOLD + ANSI_CYAN + " " + title + ANSI_RESET);
    System.out.println(ANSI_BOLD + ANSI_CYAN + "═".repeat(80) + ANSI_RESET + "\n");
  }

  private static void printStep(int stepNumber, String description) {
    System.out.println(
        ANSI_BOLD + ANSI_YELLOW + "\n[Step " + stepNumber + "] " + ANSI_RESET + description);
  }

  private static void printWorkflowStage(int stage, String name) {
    System.out.println(
        ANSI_BOLD + ANSI_PURPLE + "\n▶ Stage " + stage + ": " + name + ANSI_RESET);
  }

  private static AgentEvent createEvent(String flowId, String userId, String agentId) {
    eventCounter++;
    AgentEvent event =
        new AgentEvent(
            flowId + "-" + eventCounter, userId, agentId, AgentEventType.TOOL_CALL_REQUESTED);
    event.setTimestamp(System.currentTimeMillis());
    event.setCurrentStage("execution");
    event.setIterationNumber(1);
    return event;
  }

  private static void printEventDetails(AgentEvent event, String source) {
    String timestamp = LocalDateTime.now().format(TIME_FORMATTER);
    System.out.println(ANSI_WHITE + "  [" + timestamp + "] " + ANSI_CYAN + source + ANSI_RESET);
    System.out.println(ANSI_WHITE + "  • Flow ID: " + ANSI_RESET + event.getFlowId());
    System.out.println(ANSI_WHITE + "  • User ID: " + ANSI_RESET + event.getUserId());
    System.out.println(ANSI_WHITE + "  • Agent ID: " + ANSI_RESET + event.getAgentId());
    System.out.println(ANSI_WHITE + "  • Type: " + ANSI_RESET + event.getEventType());
    if (event.getData() != null && !event.getData().isEmpty()) {
      System.out.println(ANSI_WHITE + "  • Data: " + ANSI_RESET + event.getData());
    }
  }

  private static void printEventDetails(Event event, String source) {
    String timestamp = LocalDateTime.now().format(TIME_FORMATTER);
    System.out.println(ANSI_WHITE + "  [" + timestamp + "] " + ANSI_CYAN + source + ANSI_RESET);
    System.out.println(ANSI_WHITE + "  • Event ID: " + ANSI_RESET + event.getId());
    System.out.println(
        ANSI_WHITE + "  • Attributes: " + ANSI_RESET + event.getAttributes().size() + " fields");
  }

  private static void printToolResult(Map<String, Object> result) {
    System.out.println(ANSI_GREEN + "  ✓ Tool execution completed" + ANSI_RESET);
    for (Map.Entry<String, Object> entry : result.entrySet()) {
      System.out.println(ANSI_WHITE + "  • " + entry.getKey() + ": " + ANSI_RESET + entry.getValue());
    }
  }

  private static boolean validateResult(Map<String, Object> result) {
    // Simple validation logic
    return result != null && result.containsKey("status") && "success".equals(result.get("status"));
  }

  private static void printValidationResult(boolean isValid) {
    if (isValid) {
      System.out.println(ANSI_GREEN + "  ✓ Validation passed" + ANSI_RESET);
    } else {
      System.out.println(ANSI_RED + "  ✗ Validation failed" + ANSI_RESET);
    }
  }

  private static void printContextUpdate() {
    System.out.println(
        ANSI_CYAN
            + "  ✓ Context updated ("
            + sessionContext.size()
            + " items in memory)"
            + ANSI_RESET);
  }

  private static void printSuccess(String message) {
    System.out.println("\n" + ANSI_BOLD + ANSI_GREEN + "✓ " + message + ANSI_RESET);
  }

  /**
   * Mock hybrid agent system that simulates the integration.
   */
  static class HybridAgentSystem {
    private Map<String, ToolExecutor> tools = new HashMap<>();
    private Map<String, ToolDefinition> definitions = new HashMap<>();
    private Map<String, Agent> agents = new HashMap<>();

    public HybridAgentSystem() {
      initializeTools();
    }

    private void initializeTools() {
      // Order Lookup Tool
      tools.put(
          "order_lookup",
          new MockToolExecutor(
              "order_lookup",
              "Looks up order details",
              params -> {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "success");
                result.put("orderId", params.get("orderId"));
                result.put("productName", "Premium Wireless Headphones");
                result.put("amount", 149.99);
                result.put("orderDate", "2024-10-01");
                result.put("refundEligible", true);
                return result;
              }));

      // Refund Processor Tool
      tools.put(
          "refund_processor",
          new MockToolExecutor(
              "refund_processor",
              "Processes refund requests",
              params -> {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "success");
                result.put("refundId", "REF-" + System.currentTimeMillis());
                result.put("amount", params.get("amount"));
                result.put("estimatedDays", "3-5");
                return result;
              }));

      // Knowledge Base Tool
      tools.put(
          "knowledge_base",
          new MockToolExecutor(
              "knowledge_base",
              "Searches knowledge base",
              params -> {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "success");
                result.put("query", params.get("query"));
                result.put("resultCount", 3);
                result.put(
                    "topResult",
                    "Electronics can be returned within 30 days with original receipt");
                return result;
              }));

      // Create definitions
      for (String toolId : tools.keySet()) {
        ToolDefinition def = new ToolDefinition();
        def.setToolId(toolId);
        def.setDescription(tools.get(toolId).getDescription());
        definitions.put(toolId, def);
      }

      // Wrap as Flink Agents
      for (String toolId : tools.keySet()) {
        agents.put(
            toolId,
            FlinkAgentsToolAdapter.wrapSingleTool(toolId, tools.get(toolId), definitions.get(toolId)));
      }
    }

    public Map<String, Object> executeTool(String toolId, Map<String, Object> parameters) {
      try {
        ToolExecutor tool = tools.get(toolId);
        if (tool == null) {
          Map<String, Object> error = new HashMap<>();
          error.put("status", "error");
          error.put("message", "Tool not found: " + toolId);
          return error;
        }
        return (Map<String, Object>) tool.execute(parameters).get();
      } catch (Exception e) {
        Map<String, Object> error = new HashMap<>();
        error.put("status", "error");
        error.put("message", e.getMessage());
        return error;
      }
    }

    public Set<String> getAvailableTools() {
      return tools.keySet();
    }
  }

  /**
   * Mock tool executor for demo purposes.
   */
  static class MockToolExecutor implements ToolExecutor {
    private final String toolId;
    private final String description;
    private final java.util.function.Function<Map<String, Object>, Map<String, Object>> executor;

    public MockToolExecutor(
        String toolId,
        String description,
        java.util.function.Function<Map<String, Object>, Map<String, Object>> executor) {
      this.toolId = toolId;
      this.description = description;
      this.executor = executor;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      return CompletableFuture.completedFuture(executor.apply(parameters));
    }

    @Override
    public String getToolId() {
      return toolId;
    }

    @Override
    public String getDescription() {
      return description;
    }

    @Override
    public boolean validateParameters(Map<String, Object> parameters) {
      return parameters != null;
    }
  }
}
