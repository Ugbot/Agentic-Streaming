# Creating Tools

This guide covers how to create tools that agents can invoke during workflow execution. Agentic Flink supports two approaches: LangChain4J `@Tool` annotations for simpler cases and the `ToolExecutor` interface for full control over execution.

## Approach 1: LangChain4J @Tool Annotations

The annotation-based approach requires the least boilerplate. You write a plain Java class with annotated methods, and the framework discovers and registers them automatically via classpath scanning.

### Defining a Tool Class

Each public method annotated with `@Tool` becomes a callable tool. Use `@P` to describe parameters.

```java
package org.agentic.flink.tools.builtin;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

public class WeatherTools {

    @Tool("Retrieves the current temperature for a city")
    public double getTemperature(
            @P("City name") String city,
            @P("Temperature unit: celsius or fahrenheit") String unit) {
        // Call your weather API here
        double tempCelsius = fetchTemperature(city);
        if ("fahrenheit".equalsIgnoreCase(unit)) {
            return tempCelsius * 9.0 / 5.0 + 32.0;
        }
        return tempCelsius;
    }

    @Tool("Checks whether it is currently raining in a city")
    public boolean isRaining(@P("City name") String city) {
        return fetchRainStatus(city);
    }

    private double fetchTemperature(String city) {
        // Real implementation against a weather service
        throw new UnsupportedOperationException("Implement against your weather API");
    }

    private boolean fetchRainStatus(String city) {
        throw new UnsupportedOperationException("Implement against your weather API");
    }
}
```

Requirements for tool classes:

- Must have a public no-argument constructor (the registry instantiates the class via reflection).
- Each `@Tool` method must be public.
- `@Tool("description")` provides the description the LLM sees when deciding whether to call the tool.
- `@P("description")` documents each parameter for the LLM.
- Supported parameter types: `String`, `int`/`Integer`, `long`/`Long`, `double`/`Double`, `float`/`Float`, `boolean`/`Boolean`. Complex objects are mapped as `"object"` in the schema.

### How Tool IDs Are Generated

The `ToolAnnotationRegistry` generates a tool ID from the class name and method name. The class name is lowercased with common suffixes (`Tools`, `Tool`, `Executor`) stripped, then joined to the lowercased method name with an underscore:

| Class | Method | Generated Tool ID |
|---|---|---|
| `CalculatorTools` | `add` | `calculator_add` |
| `StringTools` | `toUpperCase` | `string_touppercase` |
| `WeatherTools` | `getTemperature` | `weather_gettemperature` |

### Registering Annotated Tools

`ToolAnnotationRegistry` scans a base package for `@Tool`-annotated methods and builds `ToolDefinition` objects:

```java
// Scan the package containing your tool classes
ToolAnnotationRegistry registry =
    new ToolAnnotationRegistry("org.agentic.flink.tools.builtin");

// Inspect discovered tools
Map<String, ToolDefinition> tools = registry.getToolDefinitions();
System.out.println("Discovered " + registry.getToolCount() + " tools");

// Check for a specific tool
boolean hasAdd = registry.hasTool("calculator_add");
```

### Invoking Annotated Tools

The `LangChainToolAdapter` bridges annotated methods into the `ToolExecutor` interface so the rest of the framework can call them uniformly:

```java
ToolAnnotationRegistry registry =
    new ToolAnnotationRegistry("org.agentic.flink.tools.builtin");

LangChainToolAdapter adapter = new LangChainToolAdapter("calculator_add", registry);

Map<String, Object> params = new HashMap<>();
params.put("a", 15.5);
params.put("b", 7.3);

CompletableFuture<Object> result = adapter.execute(params);
double sum = (double) result.get(); // 22.8
```

The adapter handles parameter type conversion (e.g., `String` to `double`) and provides default values for missing primitives.

## Approach 2: ToolExecutor Interface

The `ToolExecutor` interface gives you full control over asynchronous execution, parameter validation, and error handling. Use this when you need custom serialization logic, connection management, or complex validation.

### The ToolExecutor Interface

```java
public interface ToolExecutor extends Serializable {
    CompletableFuture<Object> execute(Map<String, Object> parameters);
    String getToolId();
    String getDescription();
    default boolean validateParameters(Map<String, Object> parameters) {
        return parameters != null;
    }
}
```

Key points:

- `execute()` returns a `CompletableFuture` -- tools run asynchronously.
- `Serializable` is required because Flink serializes operators across the cluster. Mark non-serializable fields (connections, clients) as `transient` and reinitialize them lazily.
- `validateParameters()` is called before execution; override it to reject bad input early.

### Using AbstractToolExecutor

`AbstractToolExecutor` provides logging, parameter extraction helpers, and stores the tool ID and description:

```java
package org.agentic.flink.tools;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class HttpLookupTool extends AbstractToolExecutor {

    private static final long serialVersionUID = 1L;

    public HttpLookupTool() {
        super("http-lookup", "Fetches a URL and returns the response body");
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
        return CompletableFuture.supplyAsync(() -> {
            logExecution(parameters);

            String url = getRequiredParameter(parameters, "url", String.class);
            int timeoutMs = getOptionalParameter(parameters, "timeout_ms", Integer.class, 5000);

            // Perform the HTTP request (use your preferred HTTP client)
            return performGet(url, timeoutMs);
        });
    }

    @Override
    public boolean validateParameters(Map<String, Object> parameters) {
        return parameters != null
            && parameters.containsKey("url")
            && parameters.get("url") instanceof String;
    }

    private String performGet(String url, int timeoutMs) {
        // Real HTTP implementation
        throw new UnsupportedOperationException("Implement with HttpClient or OkHttp");
    }
}
```

### Implementing ToolExecutor Directly

For cases where you do not need the base class helpers:

```java
package org.agentic.flink.example;

import org.agentic.flink.tools.ToolExecutor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SimpleCalculatorTool implements ToolExecutor {

    private static final long serialVersionUID = 1L;
    private final String operation;

    public SimpleCalculatorTool(String operation) {
        this.operation = operation;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
        return CompletableFuture.supplyAsync(() -> {
            double a = ((Number) parameters.get("a")).doubleValue();
            double b = ((Number) parameters.get("b")).doubleValue();

            switch (operation) {
                case "add":      return a + b;
                case "subtract": return a - b;
                case "multiply": return a * b;
                case "divide":
                    if (b == 0) throw new IllegalArgumentException("Cannot divide by zero");
                    return a / b;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        });
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
```

## Registering Tools with Agents

### Using ToolRegistry

`ToolRegistry` is the central registry that agents consult at runtime. Use its builder to register `ToolExecutor` implementations:

```java
import org.agentic.flink.tool.ToolRegistry;

ToolRegistry registry = ToolRegistry.builder()
    .registerTool("calculator-add", new SimpleCalculatorTool("add"))
    .registerTool("calculator-subtract", new SimpleCalculatorTool("subtract"))
    .registerTool("http-lookup", new HttpLookupTool())
    .build();

// Verify tools are available
registry.validateRequiredTools(Set.of("calculator-add", "http-lookup"));

// Retrieve an executor
Optional<ToolExecutor> executor = registry.getExecutor("calculator-add");
```

You can also register with an explicit description:

```java
ToolRegistry registry = ToolRegistry.builder()
    .registerTool("my-tool", "Does something useful", myToolExecutor)
    .build();
```

### Wiring Tools into an Agent

Pass the `ToolRegistry` to your agent via the `AgentBuilder` DSL:

```java
import org.agentic.flink.dsl.Agent;

Agent agent = Agent.builder()
    .withId("assistant")
    .withSystemPrompt("You are a helpful assistant with calculator and search tools.")
    .withTools("calculator-add", "calculator-subtract", "http-lookup")
    .withRequiredTools("calculator-add")  // Agent fails to start without this tool
    .withToolTimeout(Duration.ofSeconds(10))
    .build();
```

- `withTools(...)` declares which tools the agent is allowed to call.
- `withRequiredTools(...)` causes a startup validation failure if any listed tool is missing from the registry.
- `withToolTimeout(...)` sets the per-call timeout for tool execution.

### Combining Both Approaches

You can use annotation-scanned tools alongside manually registered `ToolExecutor` implementations:

```java
// Discover annotated tools
ToolAnnotationRegistry annotationRegistry =
    new ToolAnnotationRegistry("org.agentic.flink.tools.builtin");

// Build ToolRegistry with both annotation-discovered and manual tools
ToolRegistry.ToolRegistryBuilder builder = ToolRegistry.builder();

// Add annotation-discovered tools via LangChainToolAdapter
for (Map.Entry<String, ToolDefinition> entry :
        annotationRegistry.getToolDefinitions().entrySet()) {
    builder.registerTool(
        entry.getKey(),
        new LangChainToolAdapter(entry.getKey(), annotationRegistry));
}

// Add manually implemented tools
builder.registerTool("http-lookup", new HttpLookupTool());

ToolRegistry registry = builder.build();
```

## Testing Tools

### Testing @Tool Annotated Methods

Annotated tool classes are plain Java objects, so test them directly:

```java
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class CalculatorToolsTest {

    private final CalculatorTools calculator = new CalculatorTools();

    @Test
    void addReturnsSumOfInputs() {
        double result = calculator.add(3.5, 2.5);
        assertEquals(6.0, result, 0.0001);
    }

    @Test
    void divideByZeroThrows() {
        assertThrows(ArithmeticException.class, () -> calculator.divide(1.0, 0.0));
    }
}
```

### Testing the Annotation Registry

Verify that your tools are discovered correctly:

```java
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ToolAnnotationRegistryTest {

    @Test
    void discoversAllCalculatorTools() {
        ToolAnnotationRegistry registry =
            new ToolAnnotationRegistry("org.agentic.flink.tools.builtin");

        assertTrue(registry.hasTool("calculator_add"));
        assertTrue(registry.hasTool("calculator_subtract"));
        assertTrue(registry.hasTool("calculator_multiply"));
        assertTrue(registry.hasTool("calculator_divide"));
        assertTrue(registry.getToolCount() >= 4);
    }

    @Test
    void toolDefinitionHasCorrectMetadata() {
        ToolAnnotationRegistry registry =
            new ToolAnnotationRegistry("org.agentic.flink.tools.builtin");

        ToolDefinition def = registry.getToolDefinition("calculator_add");
        assertNotNull(def);
        assertEquals("add", def.getName());
        assertEquals("Performs addition of two numbers", def.getDescription());
    }
}
```

### Testing ToolExecutor Implementations

Test execution, validation, and error handling:

```java
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

class SimpleCalculatorToolTest {

    @Test
    void executeReturnsCorrectResult() throws Exception {
        SimpleCalculatorTool tool = new SimpleCalculatorTool("add");
        Map<String, Object> params = Map.of("a", 10.0, "b", 5.0);

        Object result = tool.execute(params).get();
        assertEquals(15.0, (double) result, 0.0001);
    }

    @Test
    void validateParametersRejectsMissingKeys() {
        SimpleCalculatorTool tool = new SimpleCalculatorTool("add");

        assertFalse(tool.validateParameters(Map.of("a", 1.0)));
        assertFalse(tool.validateParameters(null));
        assertTrue(tool.validateParameters(Map.of("a", 1.0, "b", 2.0)));
    }

    @Test
    void divideByZeroWrapsInExecutionException() {
        SimpleCalculatorTool tool = new SimpleCalculatorTool("divide");
        Map<String, Object> params = Map.of("a", 10.0, "b", 0.0);

        assertThrows(ExecutionException.class, () -> tool.execute(params).get());
    }

    @Test
    void toolIdReflectsOperation() {
        assertEquals("calculator-add", new SimpleCalculatorTool("add").getToolId());
        assertEquals("calculator-multiply", new SimpleCalculatorTool("multiply").getToolId());
    }
}
```

### Testing ToolRegistry Wiring

```java
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.Set;

class ToolRegistryTest {

    @Test
    void registeredToolsAreRetrievable() {
        ToolRegistry registry = ToolRegistry.builder()
            .registerTool("calc-add", new SimpleCalculatorTool("add"))
            .registerTool("calc-sub", new SimpleCalculatorTool("subtract"))
            .build();

        assertTrue(registry.hasTool("calc-add"));
        assertTrue(registry.hasTool("calc-sub"));
        assertFalse(registry.hasTool("calc-multiply"));

        assertTrue(registry.getExecutor("calc-add").isPresent());
    }

    @Test
    void validateRequiredToolsThrowsOnMissing() {
        ToolRegistry registry = ToolRegistry.builder()
            .registerTool("calc-add", new SimpleCalculatorTool("add"))
            .build();

        assertThrows(IllegalStateException.class,
            () -> registry.validateRequiredTools(Set.of("calc-add", "nonexistent")));
    }
}
```

## Which Approach to Choose

| Criteria | @Tool Annotations | ToolExecutor Interface |
|---|---|---|
| Boilerplate | Minimal | More code required |
| Parameter handling | Automatic type conversion | Manual extraction |
| Async control | Wrapped by LangChainToolAdapter | Full control over CompletableFuture |
| Validation | Basic (presence check) | Custom validateParameters() |
| Serialization | Handled by adapter | You manage transient fields |
| Discovery | Automatic via classpath scan | Manual registration |
| Best for | Simple, stateless tools | Stateful tools, connection management, complex validation |

For most tools, start with `@Tool` annotations. Switch to `ToolExecutor` when you need custom async behavior, connection pooling, or fine-grained validation logic.

## File Locations

- `ToolExecutor` interface: `src/main/java/org/agentic/flink/tools/ToolExecutor.java`
- `AbstractToolExecutor` base class: `src/main/java/org/agentic/flink/tools/AbstractToolExecutor.java`
- `ToolAnnotationRegistry`: `src/main/java/org/agentic/flink/langchain/ToolAnnotationRegistry.java`
- `LangChainToolAdapter`: `src/main/java/org/agentic/flink/langchain/LangChainToolAdapter.java`
- `ToolRegistry`: `src/main/java/org/agentic/flink/tool/ToolRegistry.java`
- Built-in tools: `src/main/java/org/agentic/flink/tools/builtin/`
- Example: `src/main/java/org/agentic/flink/example/SimpleCalculatorTool.java`
