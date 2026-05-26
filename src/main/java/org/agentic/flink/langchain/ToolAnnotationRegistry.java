package org.agentic.flink.langchain;

import org.agentic.flink.core.ToolDefinition;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry that automatically discovers tools annotated with LangChain4j @Tool annotation.
 *
 * <p>Scans classpath for methods with @Tool annotation and converts them to ToolDefinition objects
 * that can be used by the existing agent framework.
 *
 * <p>Example usage:
 *
 * <pre>
 * public class CalculatorTools {
 *   @Tool("Performs addition of two numbers")
 *   public double add(@P("First number") double a, @P("Second number") double b) {
 *     return a + b;
 *   }
 * }
 *
 * // Register automatically
 * ToolAnnotationRegistry registry = new ToolAnnotationRegistry("org.agentic.flink.tools");
 * Map&lt;String, ToolDefinition&gt; tools = registry.getToolDefinitions();
 * </pre>
 *
 * @author Agentic Flink Team
 */
public class ToolAnnotationRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ToolAnnotationRegistry.class);

  private final Map<String, ToolDefinition> toolDefinitions = new HashMap<>();
  private final Map<String, Object> toolInstances = new HashMap<>();
  private final String basePackage;

  /**
   * Creates a new ToolAnnotationRegistry that scans the specified package.
   *
   * @param basePackage The base package to scan for @Tool annotations
   */
  public ToolAnnotationRegistry(String basePackage) {
    this.basePackage = basePackage;
    scanForTools();
  }

  /**
   * Scans classpath for @Tool annotated methods and registers them.
   */
  private void scanForTools() {
    LOG.info("Scanning package '{}' for @Tool annotations...", basePackage);

    try {
      Reflections reflections = new Reflections(basePackage, Scanners.MethodsAnnotated);
      Set<Method> annotatedMethods = reflections.getMethodsAnnotatedWith(Tool.class);

      LOG.info("Found {} @Tool annotated methods", annotatedMethods.size());

      for (Method method : annotatedMethods) {
        try {
          registerTool(method);
        } catch (Exception e) {
          LOG.error(
              "Failed to register tool from method: {}.{}",
              method.getDeclaringClass().getName(),
              method.getName(),
              e);
        }
      }

      LOG.info("Successfully registered {} tools from annotations", toolDefinitions.size());
    } catch (Exception e) {
      LOG.error("Failed to scan for @Tool annotations in package: {}", basePackage, e);
    }
  }

  /**
   * Registers a single @Tool annotated method.
   *
   * @param method The method to register
   */
  private void registerTool(Method method) throws Exception {
    Tool toolAnnotation = method.getAnnotation(Tool.class);
    if (toolAnnotation == null) {
      return;
    }

    Class<?> declaringClass = method.getDeclaringClass();
    String toolId = generateToolId(declaringClass, method);
    String toolName = method.getName();

    // Handle @Tool annotation - value() can return String or String[]
    String description;
    String[] values = toolAnnotation.value();
    if (values != null && values.length > 0) {
      description = values[0];
    } else {
      description = "No description provided";
    }

    LOG.debug(
        "Registering tool: {} from {}.{}", toolId, declaringClass.getSimpleName(), method.getName());

    // Create ToolDefinition
    ToolDefinition toolDef = new ToolDefinition(toolId, toolName, description);
    toolDef.setVersion("1.0");
    toolDef.setRequiresApproval(false); // Can be overridden
    toolDef.setExecutorClass("org.agentic.flink.langchain.LangChainToolAdapter");

    // Extract parameters from method
    Parameter[] parameters = method.getParameters();
    for (Parameter param : parameters) {
      P paramAnnotation = param.getAnnotation(P.class);
      String paramName = param.getName();
      String paramDescription = paramAnnotation != null ? paramAnnotation.value() : paramName;
      String paramType = getParameterType(param.getType());
      boolean required = true; // Default to required, can be enhanced

      toolDef.addInputParameter(paramName, paramType, paramDescription, required);
    }

    // Set output schema based on return type
    String returnType = getParameterType(method.getReturnType());
    Map<String, Object> outputSchema = new HashMap<>();
    outputSchema.put("type", returnType);
    toolDef.setOutputSchema(outputSchema);

    // Create instance of the declaring class (if not already created)
    String classKey = declaringClass.getName();
    if (!toolInstances.containsKey(classKey)) {
      try {
        Object instance = declaringClass.getDeclaredConstructor().newInstance();
        toolInstances.put(classKey, instance);
        LOG.debug("Created instance of {}", classKey);
      } catch (Exception e) {
        LOG.error("Failed to instantiate class: {}", classKey, e);
        throw new IllegalStateException(
            "Cannot create instance of " + classKey + ". Ensure it has a no-arg constructor.", e);
      }
    }

    // Store tool definition
    toolDefinitions.put(toolId, toolDef);

    // Store method and instance in executor config for later retrieval
    toolDef.getExecutorConfig().put("methodName", method.getName());
    toolDef.getExecutorConfig().put("className", declaringClass.getName());

    LOG.info(
        "Registered @Tool: {} - {} ({})", toolId, toolName, declaringClass.getSimpleName());
  }

  /**
   * Generates a unique tool ID from class and method name.
   *
   * @param clazz The declaring class
   * @param method The method
   * @return A unique tool ID
   */
  private String generateToolId(Class<?> clazz, Method method) {
    String className = clazz.getSimpleName().toLowerCase();
    String methodName = method.getName();

    // Remove common suffixes
    className = className.replace("tools", "").replace("executor", "").replace("tool", "");

    // Format: className_methodName
    if (className.isEmpty()) {
      return methodName.toLowerCase();
    }
    return className + "_" + methodName.toLowerCase();
  }

  /**
   * Maps Java parameter types to tool parameter type strings.
   *
   * @param type The Java class type
   * @return A string representation of the type
   */
  private String getParameterType(Class<?> type) {
    if (type == String.class) {
      return "string";
    } else if (type == int.class
        || type == Integer.class
        || type == long.class
        || type == Long.class
        || type == double.class
        || type == Double.class
        || type == float.class
        || type == Float.class) {
      return "number";
    } else if (type == boolean.class || type == Boolean.class) {
      return "boolean";
    } else if (type.isArray() || Iterable.class.isAssignableFrom(type)) {
      return "array";
    } else if (type == void.class || type == Void.class) {
      return "void";
    } else {
      return "object";
    }
  }

  /**
   * Gets all discovered tool definitions.
   *
   * @return Map of tool ID to ToolDefinition
   */
  public Map<String, ToolDefinition> getToolDefinitions() {
    return new HashMap<>(toolDefinitions);
  }

  /**
   * Gets a specific tool definition by ID.
   *
   * @param toolId The tool ID
   * @return The ToolDefinition, or null if not found
   */
  public ToolDefinition getToolDefinition(String toolId) {
    return toolDefinitions.get(toolId);
  }

  /**
   * Gets the instance associated with a tool (for method invocation).
   *
   * @param toolId The tool ID
   * @return The tool instance, or null if not found
   */
  public Object getToolInstance(String toolId) {
    ToolDefinition toolDef = toolDefinitions.get(toolId);
    if (toolDef == null) {
      return null;
    }

    String className = toolDef.getExecutorConfig().get("className");
    return className != null ? toolInstances.get(className) : null;
  }

  /**
   * Gets the method associated with a tool (for invocation).
   *
   * @param toolId The tool ID
   * @return The Method, or null if not found
   */
  public Method getToolMethod(String toolId) throws Exception {
    ToolDefinition toolDef = toolDefinitions.get(toolId);
    if (toolDef == null) {
      return null;
    }

    String className = toolDef.getExecutorConfig().get("className");
    String methodName = toolDef.getExecutorConfig().get("methodName");

    if (className == null || methodName == null) {
      return null;
    }

    // Find the method by scanning class methods (need to match by name and @Tool annotation)
    Class<?> clazz = Class.forName(className);
    for (Method method : clazz.getDeclaredMethods()) {
      if (method.getName().equals(methodName) && method.isAnnotationPresent(Tool.class)) {
        return method;
      }
    }

    return null;
  }

  /**
   * Checks if a tool with the given ID is registered.
   *
   * @param toolId The tool ID
   * @return true if the tool is registered
   */
  public boolean hasTool(String toolId) {
    return toolDefinitions.containsKey(toolId);
  }

  /**
   * Gets the number of registered tools.
   *
   * @return The count of registered tools
   */
  public int getToolCount() {
    return toolDefinitions.size();
  }

  /**
   * Gets the base package being scanned.
   *
   * @return The base package
   */
  public String getBasePackage() {
    return basePackage;
  }
}
