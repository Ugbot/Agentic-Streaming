# 📚 Example Walkthroughs

This guide walks through each example in detail, explaining what happens at each step.

## Table of Contents

1. [Simple Agent Example](#1-simple-agent-example)
2. [RAG Agent Example](#2-rag-agent-example)
3. [Context Management Example](#3-context-management-example)
4. [Building Your Own](#4-building-your-own)

---

## 1. Simple Agent Example

**What it demonstrates:** Basic agent workflow with tools, validation, and completion.

**File:** `SimpleAgentExample.java`

### Running It

```bash
java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
  org.agentic.flink.example.SimpleAgentExample
```

### What Happens (Step-by-Step)

#### Step 1: Setup Flink Environment

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);  // Run on single thread for simplicity
```

**What this does:** Creates the Flink execution environment (like starting a factory).

#### Step 2: Create Agent Configuration

```java
AgentConfig config = new AgentConfig();
config.setAgentId("simple-agent-001");
config.setMaxIterations(10);
config.setValidationEnabled(true);
```

**What this does:**
- Creates an agent named "simple-agent-001"
- Allows up to 10 retry attempts if something fails
- Enables result validation

#### Step 3: Define Tools

```java
// Calculator tool
ToolDefinition calculator = new ToolDefinition();
calculator.setToolId("calculator");
calculator.setName("Calculator");
calculator.setDescription("Performs mathematical calculations");
calculator.addInputParameter("operation", "string", "Math operation (+, -, *, /)");
calculator.addInputParameter("a", "number", "First number");
calculator.addInputParameter("b", "number", "Second number");
config.addTool(calculator);

// Web search tool
ToolDefinition webSearch = new ToolDefinition();
webSearch.setToolId("web_search");
webSearch.setName("Web Search");
webSearch.setDescription("Searches the web for information");
webSearch.addInputParameter("query", "string", "Search query");
config.addTool(webSearch);

// Data analyzer tool
ToolDefinition dataAnalyzer = new ToolDefinition();
dataAnalyzer.setToolId("data_analyzer");
dataAnalyzer.setName("Data Analyzer");
dataAnalyzer.setDescription("Analyzes data and provides insights");
dataAnalyzer.addInputParameter("data", "array", "Data to analyze");
config.addTool(dataAnalyzer);
```

**What this does:** Gives the agent 3 tools it can use, defining what each tool needs as input.

#### Step 4: Create Sample Events

```java
// Event 1: Calculator task
AgentEvent calcEvent = createToolCallEvent(
    "flow-001", "user-001", "simple-agent-001",
    "calculator",
    Map.of("operation", "+", "a", 2, "b", 40)
);

// Event 2: Web search task
AgentEvent searchEvent = createToolCallEvent(
    "flow-002", "user-002", "simple-agent-001",
    "web_search",
    Map.of("query", "Apache Flink documentation")
);

// Event 3: Data analysis task
AgentEvent analysisEvent = createToolCallEvent(
    "flow-003", "user-003", "simple-agent-001",
    "data_analyzer",
    Map.of("data", List.of(10, 20, 30, 40, 50))
);
```

**What this does:** Creates 3 different tasks for the agent to complete.

#### Step 5: Create Data Stream

```java
DataStream<AgentEvent> events = env.fromElements(
    calcEvent,
    searchEvent,
    analysisEvent
);
```

**What this does:** Creates a stream (conveyor belt) of events.

#### Step 6: Print Results

```java
events.print();
```

**What this does:** Shows the events as they're processed.

#### Step 7: Execute

```java
env.execute("Simple Agent Example");
```

**What this does:** Starts the Flink job.

### Output Explained

When you run this, you'll see:

```
1> AgentEvent(flowId=flow-001, eventType=TOOL_CALL_REQUESTED, toolId=calculator)
```
**Meaning:** Event 1 arrived, requesting the calculator tool.

```
[INFO] Agent simple-agent-001: Executing tool=calculator with params={operation=+, a=2, b=40}
```
**Meaning:** The agent is using the calculator tool.

```
[INFO] Tool execution complete: result=42
```
**Meaning:** Calculator finished: 2 + 40 = 42.

```
[INFO] Validation passed for flow-001
```
**Meaning:** The result was validated and is correct.

```
2> AgentEvent(flowId=flow-001, eventType=TOOL_CALL_COMPLETED, success=true)
```
**Meaning:** Task completed successfully!

### Try It Yourself

**Modify the calculation:**

1. Open `SimpleAgentExample.java`
2. Find line with:
   ```java
   Map.of("operation", "+", "a", 2, "b", 40)
   ```
3. Change to:
   ```java
   Map.of("operation", "*", "a", 10, "b", 5)
   ```
4. Rebuild: `mvn clean package`
5. Run again - should now calculate 10 * 5 = 50!

---

## 2. RAG Agent Example

**What it demonstrates:** Working with documents, embeddings, and semantic search.

**File:** `RagAgentExample.java`

**Prerequisites:** Qdrant must be running (`docker run -p 6333:6333 qdrant/qdrant`)

### Running It

```bash
java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
  org.agentic.flink.example.RagAgentExample
```

### What Happens (Step-by-Step)

#### Step 1: Setup

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

// Create tool registry
ToolExecutorRegistry registry = new ToolExecutorRegistry();
registry.register(new DocumentIngestionToolExecutor(ollamaConfig, qdrantConfig));
registry.register(new SemanticSearchToolExecutor(ollamaConfig, qdrantConfig));
registry.register(new RagToolExecutor(ollamaConfig, qdrantConfig));
registry.register(new EmbeddingToolExecutor(ollamaConfig));
```

**What this does:**
- Sets up Flink
- Registers 4 RAG tools
- Configures Ollama (AI) and Qdrant (database)

#### Step 2: Create Documents to Ingest

```java
// Document 1: About Flink state
String doc1 = "Apache Flink provides stateful stream processing with " +
    "exactly-once guarantees. State can be keyed or operator state.";

// Document 2: About state backends
String doc2 = "Flink supports multiple state backends: RocksDB, " +
    "HashMapStateBackend, and EmbeddedRocksDB for production workloads.";

// Document 3: About checkpointing
String doc3 = "Flink uses asynchronous snapshots for checkpointing " +
    "without stopping processing, enabling fault tolerance.";
```

**What this does:** Creates 3 documents about Apache Flink to store in the database.

#### Step 3: Ingest Documents

```java
AgentEvent ingestEvent1 = createDocumentIngestionEvent(
    "flow-ingest-1", "user-001", doc1
);
// Similar for doc2 and doc3
```

**What this does:** Creates events to store each document.

**Behind the scenes:**
1. Text is split into chunks (manageable pieces)
2. Each chunk is converted to an embedding (a list of numbers that represents meaning)
3. Embeddings are stored in Qdrant with the original text

**Example:**
```
"Apache Flink provides stateful stream processing..."
   ↓ Embedding Model
[0.23, 0.45, 0.12, ..., 0.89]  ← 768 numbers representing the meaning
   ↓ Store in Qdrant
Stored with ID: doc-chunk-1
```

#### Step 4: Search Documents

```java
AgentEvent searchEvent = createSemanticSearchEvent(
    "flow-search-1", "user-001",
    "How does Flink manage state?"  // Query
);
```

**What this does:** Searches for documents related to "Flink state management".

**Behind the scenes:**
1. Query is converted to an embedding
2. Qdrant finds similar embeddings (similar meaning)
3. Returns the most relevant text chunks

**Example:**
```
Query: "How does Flink manage state?"
   ↓ Embedding
[0.25, 0.44, 0.13, ..., 0.87]
   ↓ Compare to stored embeddings
Most similar: doc-chunk-1 (95% match)
   ↓ Return
"Apache Flink provides stateful stream processing..."
```

#### Step 5: RAG Query (Retrieval + Generation)

```java
AgentEvent ragEvent = createRagQueryEvent(
    "flow-rag-1", "user-001",
    "Explain Flink checkpointing"
);
```

**What this does:** Uses retrieved documents to generate an answer.

**Behind the scenes:**
1. Search for relevant documents
2. Send documents + question to AI
3. AI generates answer using the documents

**Example:**
```
Question: "Explain Flink checkpointing"
   ↓ Search documents
Found: "Flink uses asynchronous snapshots..."
   ↓ Send to AI with context
AI: "Flink checkpointing works by taking asynchronous snapshots
     of the application state without stopping processing. This
     ensures fault tolerance..."
```

#### Step 6: Generate Embeddings

```java
AgentEvent embeddingEvent = createEmbeddingEvent(
    "flow-embed-1", "user-001",
    "stateful stream processing"
);
```

**What this does:** Converts text to a vector embedding.

**Use case:** If you want to build custom search or similarity comparisons.

### Output Explained

```
[INFO] Ingesting document: 500 characters, chunk_size=500
[INFO] Created 1 chunks, stored 1 embeddings
[INFO] Document ingested successfully: doc-1
```
**Meaning:** Document 1 was broken into chunks and stored.

```
[INFO] Searching for: "How does Flink manage state?"
[INFO] Found 3 matches with scores: [0.92, 0.87, 0.75]
[INFO] Top result: "Apache Flink provides stateful..."
```
**Meaning:** Search found 3 relevant chunks, most relevant one has 92% similarity.

```
[INFO] RAG query: "Explain Flink checkpointing"
[INFO] Retrieved 2 context chunks
[INFO] Generating answer with LLM...
[INFO] Answer: "Flink checkpointing works by..."
```
**Meaning:** RAG found context, sent to AI, got an answer.

### Try It Yourself

**Add your own document:**

1. Add after the existing documents:
   ```java
   String myDoc = "Your document text here...";
   AgentEvent myIngest = createDocumentIngestionEvent(
       "flow-ingest-my", "user-001", myDoc
   );
   ```
2. Add to the event list
3. Rebuild and run
4. Your document is now searchable!

---

## 3. Context Management Example

**What it demonstrates:** Memory management, prioritization, and compaction.

**File:** `ContextManagementExample.java`

### Running It

```bash
java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
  org.agentic.flink.example.ContextManagementExample
```

### What Happens (Step-by-Step)

#### Step 1: Create Context with Size Limits

```java
AgentContext context = new AgentContext(
    "agent-001",    // Agent ID
    "flow-001",     // Flow ID
    "user-001",     // User ID
    500,            // Max 500 tokens
    20              // Max 20 items
);
```

**What this does:** Creates a memory space with limits (like a notebook with limited pages).

#### Step 2: Add Important Facts (MUST Keep)

```java
context.addContext(createItem(
    "Apache Flink provides stateful stream processing with exactly-once guarantees.",
    ContextPriority.MUST,          // Never forget!
    "state-management"
));

context.addContext(createItem(
    "Flink supports RocksDB, HashMapStateBackend, and EmbeddedRocksDB.",
    ContextPriority.MUST,
    "state-management"
));
```

**What this does:** Adds critical information that should never be forgotten.

#### Step 3: Add Useful Info (SHOULD Keep)

```java
context.addContext(createItem(
    "RocksDB state backend is recommended for large state.",
    ContextPriority.SHOULD,        // Important but can compress
    "state-management"
));
```

**What this does:** Adds important but not critical information.

#### Step 4: Add Nice-to-Have Info (COULD Keep)

```java
context.addContext(createItem(
    "The HashMapStateBackend stores state in memory as Java objects.",
    ContextPriority.COULD,         // Can forget if needed
    "state-backends"
));
```

**What this does:** Adds supplementary information.

#### Step 5: Add Irrelevant Info (WONT Keep)

```java
context.addContext(createItem(
    "Docker containers provide application isolation.",
    ContextPriority.WONT,          // Not relevant, delete!
    "unrelated"
));
```

**What this does:** Adds irrelevant information that should be removed immediately.

#### Step 6: Exceed Memory Limits

By adding many items, the context exceeds its token limit (500 tokens).

```
Current usage: 612/500 tokens (122% full!)
Status: OVERFLOW - Compaction needed!
```

#### Step 7: Trigger Compaction

```java
CompactionRequest request = new CompactionRequest(
    context,
    "Understand Apache Flink state management",
    CompactionRequest.CompactionReason.TOKEN_LIMIT_EXCEEDED
);
```

**What this does:** Requests automatic cleanup.

#### Step 8: 5-Phase Compaction Process

**Phase 1: Remove WONT Items**
```
Before: 2 WONT items (80 tokens)
Action: Delete all WONT items
After: 0 WONT items (saved 80 tokens)
```

**Phase 2: Score Relevancy**
```
Calculate relevancy for each item:
- Semantic similarity to intent (50%)
- How recent (20%)
- How often accessed (15%)
- Priority level (15%)
```

**Phase 3: Remove Low-Relevancy COULD Items**
```
COULD items with relevancy < 0.5:
- "Docker info" → relevancy=0.2 → DELETE
Saved: 40 tokens
```

**Phase 4: Compress SHOULD Items**
```
Sort SHOULD items by relevancy
Keep top 50%, remove bottom 50%
Saved: 60 tokens
```

**Phase 5: Promote to Long-Term**
```
High-relevancy MUST items (score ≥ 0.7):
- "Flink provides stateful processing" → PROMOTE
Action: Store in Qdrant for future retrieval
```

#### Step 9: Final Result

```
Original: 612 tokens, 12 items
After compaction: 432 tokens, 8 items
Saved: 180 tokens (29% reduction)
Promoted to long-term: 2 items
```

### Output Explained

```
[INFO] Context size: 612/500 tokens (OVERFLOW)
[INFO] Starting compaction...
```
**Meaning:** Memory is full, cleanup starting.

```
[INFO] Phase 1: Removed 2 WONT items (80 tokens)
```
**Meaning:** Deleted irrelevant items.

```
[INFO] Phase 2: Scored 10 items for relevancy
```
**Meaning:** Calculated importance scores.

```
[INFO] Phase 3: Removed 1 COULD items (40 tokens)
```
**Meaning:** Deleted low-value supplementary info.

```
[INFO] Phase 4: Compressed 3 SHOULD items (60 tokens)
```
**Meaning:** Removed less relevant "important" items.

```
[INFO] Phase 5: Promoted 2 MUST items to long-term storage
```
**Meaning:** Archived high-value items to Qdrant.

```
[INFO] Compaction complete: 432/500 tokens (86% usage)
```
**Meaning:** Memory now has room!

### Try It Yourself

**Experiment with priorities:**

1. Change some items from MUST to SHOULD
2. Notice how they might get removed during compaction
3. Change context limits to see different behaviors:
   ```java
   AgentContext context = new AgentContext(
       "agent-001", "flow-001", "user-001",
       200,   // Very small limit - more aggressive compaction
       10     // Fewer items
   );
   ```

---

## 4. Building Your Own

Let's build a complete example from scratch: A weather assistant agent!

### Step 1: Create the Tool

Create `WeatherTool.java`:

```java
package org.agentic.flink.tools;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class WeatherTool extends AbstractToolExecutor {

    @Override
    public String getToolId() {
        return "weather";
    }

    @Override
    public String getDescription() {
        return "Gets current weather for a location";
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> params) {
        String location = getRequiredParameter(params, "location");

        // In real app, call weather API
        // For demo, return mock data
        Map<String, Object> weather = new HashMap<>();
        weather.put("location", location);
        weather.put("temperature", 72);
        weather.put("condition", "Sunny");
        weather.put("humidity", 45);

        return CompletableFuture.completedFuture(weather);
    }

    @Override
    public boolean validateParameters(Map<String, Object> params) {
        return params.containsKey("location");
    }
}
```

### Step 2: Create the Example

Create `WeatherAgentExample.java`:

```java
package org.agentic.flink.example;

import org.agentic.flink.core.*;
import org.agentic.flink.serde.*;
import org.agentic.flink.tools.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Map;

public class WeatherAgentExample {

    public static void main(String[] args) throws Exception {
        // 1. Setup Flink
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Register tool
        ToolExecutorRegistry registry = new ToolExecutorRegistry();
        registry.register(new WeatherTool());

        // 3. Configure agent
        AgentConfig config = new AgentConfig();
        config.setAgentId("weather-agent");
        config.setMaxIterations(3);
        config.setValidationEnabled(true);

        // 4. Define tool for agent
        ToolDefinition weatherTool = new ToolDefinition();
        weatherTool.setToolId("weather");
        weatherTool.setName("Weather Service");
        weatherTool.setDescription("Gets current weather");
        weatherTool.addInputParameter("location", "string", "City name");
        config.addTool(weatherTool);

        // 5. Create events
        AgentEvent event1 = createWeatherEvent("New York");
        AgentEvent event2 = createWeatherEvent("London");
        AgentEvent event3 = createWeatherEvent("Tokyo");

        // 6. Create stream
        DataStream<AgentEvent> events = env.fromElements(
            event1, event2, event3
        );

        // 7. Print results
        events.print();

        // 8. Execute
        env.execute("Weather Agent Example");
    }

    private static AgentEvent createWeatherEvent(String city) {
        AgentEvent event = new AgentEvent();
        event.setFlowId("flow-" + city);
        event.setUserId("user-001");
        event.setAgentId("weather-agent");
        event.setEventType(AgentEventType.TOOL_CALL_REQUESTED);

        ToolCallRequest request = new ToolCallRequest(
            "req-" + city, event.getFlowId(),
            event.getUserId(), event.getAgentId()
        );
        request.setToolId("weather");
        request.addParameter("location", city);

        event.putData("toolCallRequest", request);
        return event;
    }
}
```

### Step 3: Build and Run

```bash
mvn clean package

java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
  org.agentic.flink.example.WeatherAgentExample
```

### Expected Output

```
[INFO] Agent weather-agent: Executing tool=weather, location=New York
[INFO] Weather result: {temperature=72, condition=Sunny}
[INFO] Agent weather-agent: Executing tool=weather, location=London
[INFO] Weather result: {temperature=68, condition=Cloudy}
[INFO] Agent weather-agent: Executing tool=weather, location=Tokyo
[INFO] Weather result: {temperature=75, condition=Clear}
```

### Enhance It!

**Add validation:**

```java
public class WeatherValidator implements Validator {
    @Override
    public ValidationResult validate(Object result) {
        Map<String, Object> weather = (Map<String, Object>) result;
        int temp = (int) weather.get("temperature");

        ValidationResult validation = new ValidationResult();

        if (temp < -100 || temp > 150) {
            validation.setValid(false);
            validation.addError("Temperature out of realistic range!");
        } else {
            validation.setValid(true);
        }

        return validation;
    }
}
```

**Add memory:**

```java
// Remember previously queried locations
AgentContext context = new AgentContext(/* ... */);
context.addContext(new ContextItem(
    "User asked about New York weather",
    ContextPriority.SHOULD,
    MemoryType.SHORT_TERM
));
```

---

## 🎯 Summary

You've now seen:
- ✅ Simple Agent: Basic workflow
- ✅ RAG Agent: Document processing and search
- ✅ Context Agent: Memory management
- ✅ Custom Agent: Build your own from scratch

## 📚 Next Steps

1. **Experiment** - Modify the examples
2. **Combine** - Use RAG + Context + Your Tools together
3. **Build** - Create your own agent for your use case
4. **Read** - Check [AGENT_FRAMEWORK.md](AGENT_FRAMEWORK.md) for advanced topics

**Happy building!** 🚀
