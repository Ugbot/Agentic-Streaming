package org.agentic.flink.example;

import org.agentic.flink.compensation.CompensationAction;
import org.agentic.flink.compensation.CompensationHandler;
import org.agentic.flink.compensation.CompensationResult;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Compensation Example - Saga Pattern for Rollback
 *
 * <p>This example demonstrates how compensation (rollback) works when an agent
 * workflow fails partway through execution.
 *
 * <p><b>Scenario:</b>
 * <pre>
 * 1. Create user account → SUCCESS (compensation: delete-user)
 * 2. Send welcome email → SUCCESS (compensation: send-cancellation-email)
 * 3. Charge credit card → FAILURE
 * 4. COMPENSATION TRIGGERED:
 *    - Undo: send-cancellation-email
 *    - Undo: delete-user
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class CompensationExample {

  public static void main(String[] args) throws Exception {
    System.out.println("=".repeat(80));
    System.out.println("  Compensation Example - Saga Rollback Pattern");
    System.out.println("=".repeat(80));
    System.out.println();

    // ==================== Step 1: Setup Tool Registry ====================

    System.out.println("🔧 Step 1: Setting up tools with compensation...\n");

    ToolRegistry toolRegistry = ToolRegistry.builder()
        .registerTool("create-user", new MockUserTool("create"))
        .registerTool("delete-user", new MockUserTool("delete"))
        .registerTool("send-email", new MockEmailTool("send"))
        .registerTool("send-cancellation", new MockEmailTool("cancel"))
        .build();

    System.out.println("✅ Tools registered: " + toolRegistry.getToolNames());
    System.out.println();

    // ==================== Step 2: Simulate Successful Operations ====================

    System.out.println("📝 Step 2: Simulating workflow execution...\n");

    List<CompensationAction> compensationActions = new ArrayList<>();

    // Operation 1: Create user (with compensation)
    System.out.println("1️⃣  Operation: create-user");
    CompensationAction comp1 = new CompensationAction(
        "delete-user",
        "delete-user",
        Map.of("user_id", "user-123"));
    compensationActions.add(comp1);
    System.out.println("   ✅ SUCCESS - Compensation registered: delete-user");
    System.out.println();

    // Operation 2: Send welcome email (with compensation)
    System.out.println("2️⃣  Operation: send-welcome-email");
    CompensationAction comp2 = new CompensationAction(
        "send-cancellation",
        "send-cancellation",
        Map.of("user_id", "user-123", "email", "user@example.com"));
    compensationActions.add(comp2);
    System.out.println("   ✅ SUCCESS - Compensation registered: send-cancellation");
    System.out.println();

    // Operation 3: Charge credit card (FAILS)
    System.out.println("3️⃣  Operation: charge-credit-card");
    System.out.println("   ❌ FAILURE - Payment declined!");
    System.out.println();

    // ==================== Step 3: Trigger Compensation ====================

    System.out.println("🔄 Step 3: Triggering compensation (rollback)...\n");
    System.out.println("Executing " + compensationActions.size() + " compensation actions in REVERSE order:");
    System.out.println("-".repeat(80));

    // Create failed event
    AgentEvent failedEvent = new AgentEvent(
        "flow-001",
        "user-001",
        "checkout-agent",
        AgentEventType.FLOW_FAILED
    );
    failedEvent.setErrorMessage("Payment declined");

    // Create compensation handler
    CompensationHandler handler = new CompensationHandler(toolRegistry);

    // Execute compensation
    CompletableFuture<CompensationResult> future = handler.compensate(failedEvent, compensationActions);
    CompensationResult result = future.get();

    System.out.println("-".repeat(80));
    System.out.println();

    // ==================== Step 4: Show Results ====================

    System.out.println("📊 Step 4: Compensation Results:\n");

    if (result.isSuccess()) {
      System.out.println("✅ COMPENSATION SUCCESSFUL!");
    } else {
      System.out.println("❌ COMPENSATION FAILED!");
    }

    System.out.println();
    System.out.println("Statistics:");
    System.out.println("  - Total actions: " + result.getActionResults().size());
    System.out.println("  - Succeeded: " + result.getSuccessCount());
    System.out.println("  - Failed: " + result.getFailureCount());
    System.out.println();

    System.out.println("Action Details:");
    for (int i = 0; i < result.getActionResults().size(); i++) {
      var actionResult = result.getActionResults().get(i);
      String status = actionResult.isSuccess() ? "✓" : "✗";
      System.out.println(String.format("  %s Action %d: %s - %s",
          status, i + 1, actionResult.getActionName(),
          actionResult.isSuccess() ? "Success" : actionResult.getErrorMessage()));
    }

    System.out.println();
    System.out.println("=".repeat(80));
    System.out.println("  Compensation Pattern Demonstrated! 🎉");
    System.out.println("  - Forward operations tracked ✓");
    System.out.println("  - Failure detected ✓");
    System.out.println("  - Rollback executed in reverse ✓");
    System.out.println("  - System returned to consistent state ✓");
    System.out.println("=".repeat(80));
  }

  // ==================== Mock Tool Implementations ====================

  static class MockUserTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    private final String operation;

    public MockUserTool(String operation) {
      this.operation = operation;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      return CompletableFuture.supplyAsync(() -> {
        String userId = (String) parameters.get("user_id");
        String result = operation.equals("create")
            ? "User created: " + userId
            : "User deleted: " + userId;
        System.out.println("      🔧 " + result);
        return result;
      });
    }

    @Override
    public String getToolId() {
      return operation + "-user";
    }

    @Override
    public String getDescription() {
      return operation.equals("create")
          ? "Creates a user account"
          : "Deletes a user account (compensation)";
    }
  }

  static class MockEmailTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    private final String operation;

    public MockEmailTool(String operation) {
      this.operation = operation;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      return CompletableFuture.supplyAsync(() -> {
        String email = (String) parameters.get("email");
        String result = operation.equals("send")
            ? "Welcome email sent to: " + email
            : "Cancellation email sent to: " + email;
        System.out.println("      📧 " + result);
        return result;
      });
    }

    @Override
    public String getToolId() {
      return operation + "-email";
    }

    @Override
    public String getDescription() {
      return operation.equals("send")
          ? "Sends a welcome email"
          : "Sends a cancellation email (compensation)";
    }
  }
}
