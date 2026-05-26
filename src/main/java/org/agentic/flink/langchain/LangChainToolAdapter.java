package org.agentic.flink.langchain;

import org.agentic.flink.core.ToolDefinition;
import org.agentic.flink.tools.ToolExecutor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter that bridges LangChain4j @Tool annotated methods with the Flink Agent framework.
 *
 * <p>This adapter allows methods annotated with @Tool to be invoked as ToolExecutor implementations,
 * enabling seamless integration with the existing CEP saga orchestration.
 *
 * <p>Example:
 *
 * <pre>
 * // Tool definition with @Tool annotation
 * public class CalculatorTools {
 *   @Tool("Adds two numbers")
 *   public double add(@P("First number") double a, @P("Second number") double b) {
 *     return a + b;
 *   }
 * }
 *
 * // Usage with adapter
 * ToolAnnotationRegistry registry = new ToolAnnotationRegistry("org.agentic.flink.tools");
 * LangChainToolAdapter adapter = new LangChainToolAdapter("calculator_add", registry);
 * CompletableFuture&lt;Object&gt; result = adapter.execute(Map.of("a", 5.0, "b", 3.0));
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class LangChainToolAdapter implements ToolExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(LangChainToolAdapter.class);

  private final String toolId;
  private final ToolAnnotationRegistry registry;
  private transient Method cachedMethod;
  private transient Object cachedInstance;

  /**
   * Creates a new adapter for a specific tool.
   *
   * @param toolId The tool identifier
   * @param registry The registry containing tool definitions and instances
   */
  public LangChainToolAdapter(String toolId, ToolAnnotationRegistry registry) {
    this.toolId = toolId;
    this.registry = registry;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return invokeToolMethod(parameters);
          } catch (Exception e) {
            LOG.error("Failed to execute @Tool method for toolId: {}", toolId, e);
            throw new RuntimeException("Tool execution failed: " + e.getMessage(), e);
          }
        });
  }

  /**
   * Invokes the @Tool annotated method with the provided parameters.
   *
   * @param parameters Input parameters
   * @return The result of the method invocation
   */
  private Object invokeToolMethod(Map<String, Object> parameters) throws Exception {
    // Get or cache the method and instance
    if (cachedMethod == null || cachedInstance == null) {
      cachedMethod = registry.getToolMethod(toolId);
      cachedInstance = registry.getToolInstance(toolId);

      if (cachedMethod == null) {
        throw new IllegalStateException("No method found for tool: " + toolId);
      }
      if (cachedInstance == null) {
        throw new IllegalStateException("No instance found for tool: " + toolId);
      }
    }

    // Extract method parameters in order
    Object[] args = extractMethodArguments(cachedMethod, parameters);

    // Invoke the method
    LOG.debug("Invoking @Tool method: {} with {} arguments", cachedMethod.getName(), args.length);
    Object result = cachedMethod.invoke(cachedInstance, args);

    LOG.debug("@Tool method {} completed successfully", cachedMethod.getName());
    return result;
  }

  /**
   * Extracts method arguments from the parameter map, matching parameter names to method
   * parameters.
   *
   * @param method The method to invoke
   * @param parameters The parameter map
   * @return Array of arguments in the correct order
   */
  private Object[] extractMethodArguments(Method method, Map<String, Object> parameters) {
    java.lang.reflect.Parameter[] methodParams = method.getParameters();
    List<Object> args = new ArrayList<>();

    for (java.lang.reflect.Parameter param : methodParams) {
      String paramName = param.getName();
      Object value = parameters.get(paramName);

      if (value == null) {
        LOG.warn(
            "Parameter '{}' not found in input parameters for tool '{}'", paramName, toolId);
        // Try to provide a default value based on type
        value = getDefaultValue(param.getType());
      } else {
        // Convert value to correct type if needed
        value = convertParameterValue(value, param.getType());
      }

      args.add(value);
    }

    return args.toArray();
  }

  /**
   * Converts a parameter value to the target type.
   *
   * @param value The value to convert
   * @param targetType The target type
   * @return Converted value
   */
  private Object convertParameterValue(Object value, Class<?> targetType) {
    // Already correct type
    if (targetType.isInstance(value)) {
      return value;
    }

    // Number conversions
    if (value instanceof Number) {
      Number num = (Number) value;
      if (targetType == int.class || targetType == Integer.class) {
        return num.intValue();
      } else if (targetType == long.class || targetType == Long.class) {
        return num.longValue();
      } else if (targetType == double.class || targetType == Double.class) {
        return num.doubleValue();
      } else if (targetType == float.class || targetType == Float.class) {
        return num.floatValue();
      }
    }

    // String conversions
    if (value instanceof String) {
      String str = (String) value;
      if (targetType == int.class || targetType == Integer.class) {
        return Integer.parseInt(str);
      } else if (targetType == long.class || targetType == Long.class) {
        return Long.parseLong(str);
      } else if (targetType == double.class || targetType == Double.class) {
        return Double.parseDouble(str);
      } else if (targetType == float.class || targetType == Float.class) {
        return Float.parseFloat(str);
      } else if (targetType == boolean.class || targetType == Boolean.class) {
        return Boolean.parseBoolean(str);
      }
    }

    // Default: return as-is
    return value;
  }

  /**
   * Gets a default value for a primitive type.
   *
   * @param type The type
   * @return Default value (0, false, null)
   */
  private Object getDefaultValue(Class<?> type) {
    if (type == int.class || type == Integer.class) {
      return 0;
    } else if (type == long.class || type == Long.class) {
      return 0L;
    } else if (type == double.class || type == Double.class) {
      return 0.0;
    } else if (type == float.class || type == Float.class) {
      return 0.0f;
    } else if (type == boolean.class || type == Boolean.class) {
      return false;
    }
    return null;
  }

  @Override
  public String getToolId() {
    return toolId;
  }

  @Override
  public String getDescription() {
    ToolDefinition def = registry.getToolDefinition(toolId);
    return def != null ? def.getDescription() : "No description available";
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    if (parameters == null) {
      return false;
    }

    try {
      Method method = registry.getToolMethod(toolId);
      if (method == null) {
        return false;
      }

      // Check that all required parameters are present
      java.lang.reflect.Parameter[] methodParams = method.getParameters();
      for (java.lang.reflect.Parameter param : methodParams) {
        String paramName = param.getName();
        if (!parameters.containsKey(paramName) && !param.getType().isPrimitive()) {
          // Missing non-primitive parameter
          LOG.warn("Missing required parameter: {} for tool: {}", paramName, toolId);
          // Don't fail validation - we'll provide defaults
        }
      }

      return true;
    } catch (Exception e) {
      LOG.error("Parameter validation failed for tool: {}", toolId, e);
      return false;
    }
  }
}
