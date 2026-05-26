package org.agentic.flink.example;

import org.agentic.flink.tools.ToolExecutor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple calculator tool for demonstration purposes.
 *
 * <p>Performs basic arithmetic operations (add, subtract, multiply, divide).
 *
 * @author Agentic Flink Team
 */
public class SimpleCalculatorTool implements ToolExecutor {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SimpleCalculatorTool.class);

  private final String operation;

  public SimpleCalculatorTool(String operation) {
    this.operation = operation;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        double a = getDoubleParameter(parameters, "a");
        double b = getDoubleParameter(parameters, "b");

        LOG.info("Executing {} operation: {} {} {}", operation, a, operation, b);

        double result;
        switch (operation) {
          case "add":
            result = a + b;
            break;
          case "subtract":
            result = a - b;
            break;
          case "multiply":
            result = a * b;
            break;
          case "divide":
            if (b == 0) {
              throw new IllegalArgumentException("Cannot divide by zero");
            }
            result = a / b;
            break;
          default:
            throw new IllegalArgumentException("Unknown operation: " + operation);
        }

        LOG.info("Result: {}", result);
        return result;

      } catch (Exception e) {
        LOG.error("Calculator tool execution failed", e);
        throw new RuntimeException("Calculator error: " + e.getMessage(), e);
      }
    });
  }

  private double getDoubleParameter(Map<String, Object> parameters, String name) {
    Object value = parameters.get(name);
    if (value == null) {
      throw new IllegalArgumentException("Missing required parameter: " + name);
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException(
        "Parameter " + name + " must be a number, got: " + value.getClass());
  }

  @Override
  public String getToolId() {
    return "calculator-" + operation;
  }

  @Override
  public String getDescription() {
    return "Performs " + operation + " operation on two numbers (a, b)";
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    return parameters != null
        && parameters.containsKey("a")
        && parameters.containsKey("b");
  }
}
