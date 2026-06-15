# 🚀 Getting Started with Agentic Flink

**Welcome!** This guide will take you from zero to running your first AI agent in about 15 minutes.

## 📋 What You'll Learn

By the end of this guide, you'll:
- ✅ Have all tools installed
- ✅ Run your first AI agent
- ✅ Understand what's happening
- ✅ Know how to modify the agent
- ✅ Be ready to build your own

## Part 1: Setup (10 minutes)

### Step 1: Install Java

Java is the programming language this framework uses.

**Check if you have Java:**
```bash
java -version
```

**If you see version 11 or higher, you're good!** Skip to Step 2.

**If not, install Java:**
1. Go to https://adoptium.net/
2. Download Java 11 (or higher) for your operating system
3. Run the installer
4. Verify: `java -version`

💡 **Tip:** You should see something like `openjdk version "11.0.20"`

### Step 2: Install Maven

Maven builds Java projects (like npm for Node.js or pip for Python).

**Check if you have Maven:**
```bash
mvn -version
```

**If you see version 3.6 or higher, you're good!** Skip to Step 3.

**If not, install Maven:**

**On Mac:**
```bash
brew install maven
```

**On Linux:**
```bash
sudo apt-get install maven  # Ubuntu/Debian
# or
sudo yum install maven      # CentOS/RHEL
```

**On Windows:**
1. Download from https://maven.apache.org/download.cgi
2. Extract to `C:\Program Files\Maven`
3. Add `C:\Program Files\Maven\bin` to your PATH

### Step 3: Install Ollama

Ollama lets you run AI models locally (like ChatGPT on your computer!).

**Download and install:**
- Go to https://ollama.ai
- Download for your operating system
- Run the installer

**Verify installation:**
```bash
ollama --version
```

**Start Ollama:**
```bash
ollama serve
```

Leave this terminal window open! Ollama needs to keep running.

**In a NEW terminal, download AI models:**
```bash
# Download a conversational AI model (like ChatGPT)
ollama pull llama2:latest

# Download an embedding model (for understanding text)
ollama pull nomic-embed-text
```

This might take 5-10 minutes depending on your internet speed. The models are a few GB each.

💡 **What's happening?** You're downloading AI models to your computer so you don't need internet or paid APIs to run agents!

### Step 4: (Optional) Install Qdrant

Qdrant is a vector database for storing document embeddings. You only need this for RAG examples.

**Using Docker (easiest):**
```bash
docker run -d -p 6333:6333 qdrant/qdrant
```

**Without Docker:**
Download from https://qdrant.tech/documentation/guides/installation/

💡 **Skip this if you just want to try the basic agent first!**

## Part 2: Build the Project (2 minutes)

### Step 1: Navigate to the project

```bash
cd /Users/bengamble/Agentic-Flink
```

### Step 2: Build it

```bash
mvn clean package
```

**What you'll see:**
```
[INFO] Scanning for projects...
[INFO] Building agentic-flink 1.0.0-SNAPSHOT
[INFO] Compiling 64 source files...
[INFO] BUILD SUCCESS
```

This produces **two** jars under `target/`:
- `agentic-flink-1.0.0-SNAPSHOT.jar` — a thin jar of just the framework classes (what other
  Maven modules depend on, so they get clean transitive dependencies).
- `agentic-flink-1.0.0-SNAPSHOT-uber.jar` — the fat, everything-bundled jar you run directly
  with `java -cp` or submit with `flink run` (used throughout this guide).

**If you get errors:**
- Check Java version: `java -version` (must be 11+)
- Check Maven version: `mvn -version` (must be 3.6+)
- Make sure you're in the right directory: `ls pom.xml` should show the file

## Part 3: Run Your First Agent (3 minutes)

### The Simple Agent Example

This example shows a basic agent that:
1. Receives task requests
2. Decides which tools to use
3. Executes the tools
4. Validates the results
5. Completes or retries

**Run it:**
```bash
java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  org.agentic.flink.example.SimpleAgentExample
```

### Understanding the Output

You'll see a lot of output! Let's break it down:

```
[INFO] Starting Agentic Flink Example
```
The framework is starting up.

```
[INFO] Creating agent with ID: agent-001
```
Your agent is being created with a unique ID.

```
[INFO] Agent agent-001: Received event type=TOOL_CALL_REQUESTED
[INFO] Agent agent-001: Executing tool=calculator
```
The agent received a task and decided to use the calculator tool.

```
[INFO] Tool execution result: 42
```
The tool ran and returned a result.

```
[INFO] Agent agent-001: Validation passed
```
The agent checked that the result is correct.

```
[INFO] Agent agent-001: Flow completed successfully
```
The agent finished the task!

### What Just Happened?

Let's trace the flow:

```
1. AgentEvent created → Task arrives
2. Tool selected → "I need the calculator"
3. Tool executed → Calculator runs: 2 + 40 = 42
4. Validation → "Is 42 correct? Yes!"
5. Completion → Task done!
```

## Part 4: Understanding the Code

Let's look at the simple example code to understand what's happening.

### Creating an Agent

```java
// Create a configuration for your agent
AgentConfig config = new AgentConfig();
config.setAgentId("agent-001");           // Give it a name
config.setMaxIterations(10);              // Max retry attempts
config.setValidationEnabled(true);        // Check results
```

**What this does:**
- `agentId`: Unique identifier for this agent
- `maxIterations`: How many times to retry if something fails
- `validationEnabled`: Whether to verify results

### Defining Tools

```java
// Define a calculator tool
ToolDefinition calculator = new ToolDefinition();
calculator.setToolId("calculator");
calculator.setName("Calculator");
calculator.setDescription("Performs mathematical calculations");

// Add input schema - what parameters does the tool need?
calculator.addInputParameter("operation", "string", "The math operation (+, -, *, /)");
calculator.addInputParameter("a", "number", "First number");
calculator.addInputParameter("b", "number", "Second number");

// Add this tool to the agent's config
config.addTool(calculator);
```

**What this does:**
- Defines what the tool is called
- Describes what it does
- Specifies what inputs it needs

### Creating Events

```java
// Create a task for the agent
AgentEvent event = new AgentEvent();
event.setFlowId("flow-123");              // Unique workflow ID
event.setUserId("user-001");              // Who requested this
event.setAgentId("agent-001");            // Which agent handles it
event.setEventType(AgentEventType.TOOL_CALL_REQUESTED);

// Add the tool request details
ToolCallRequest request = new ToolCallRequest();
request.setToolId("calculator");
request.addParameter("operation", "+");
request.addParameter("a", 2);
request.addParameter("b", 40);

event.putData("toolCallRequest", request);
```

**What this does:**
- Creates a task (event) for the agent
- Specifies which tool to use
- Provides the parameters the tool needs

## Part 5: Modify the Agent

Let's make your first modification!

### Challenge: Change the Calculation

**Goal:** Make the calculator compute 100 + 500 instead of 2 + 40.

**Edit the file:**
```bash
# Open the example in your favorite editor
nano src/main/java/org/agentic/flink/example/SimpleAgentExample.java
# or
code src/main/java/org/agentic/flink/example/SimpleAgentExample.java
```

**Find this code:**
```java
request.addParameter("a", 2);
request.addParameter("b", 40);
```

**Change it to:**
```java
request.addParameter("a", 100);
request.addParameter("b", 500);
```

**Rebuild and run:**
```bash
mvn clean package
java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  org.agentic.flink.example.SimpleAgentExample
```

**Expected output:**
```
[INFO] Tool execution result: 600
```

🎉 **Congratulations!** You just modified an agent!

## Part 6: Try Other Examples

### RAG Agent (Documents and Search)

This agent can read documents, remember them, and answer questions.

**Prerequisites:** Qdrant must be running (see Step 4 of setup)

**Run it:**
```bash
java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  org.agentic.flink.example.RagAgentExample
```

**What it does:**
1. Ingests 3 documents about Apache Flink
2. Stores them as embeddings in Qdrant
3. Searches for "state management"
4. Retrieves relevant sections
5. Uses AI to answer questions using the documents

### Context Management Example

This shows how agents manage memory.

**Run it:**
```bash
java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  org.agentic.flink.example.ContextManagementExample
```

**What it does:**
1. Creates a context with many items
2. Exceeds memory limits
3. Automatically compacts (keeps important, removes unimportant)
4. Stores high-value items in long-term memory

## Part 7: Build Your Own Agent

Now you're ready to create your own agent! Here's a template:

### Step 1: Create Your Tool

```java
package org.agentic.flink.tools;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MyFirstTool extends AbstractToolExecutor {

    @Override
    public String getToolId() {
        return "my-first-tool";
    }

    @Override
    public String getDescription() {
        return "This is my first custom tool!";
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
        // Get the input parameter
        String input = getRequiredParameter(parameters, "input");

        // Do something with it
        String result = "Hello, " + input + "!";

        // Return the result
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public boolean validateParameters(Map<String, Object> parameters) {
        return parameters.containsKey("input");
    }
}
```

### Step 2: Create Your Agent

```java
package org.agentic.flink.example;

import org.agentic.flink.core.*;
import org.agentic.flink.tools.MyFirstTool;
// ... other imports

public class MyFirstAgentExample {

    public static void main(String[] args) throws Exception {
        // 1. Setup Flink
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create agent config
        AgentConfig config = new AgentConfig();
        config.setAgentId("my-agent");
        config.setMaxIterations(5);

        // 3. Register your tool
        ToolExecutorRegistry registry = new ToolExecutorRegistry();
        registry.register(new MyFirstTool());

        // 4. Define the tool
        ToolDefinition toolDef = new ToolDefinition();
        toolDef.setToolId("my-first-tool");
        toolDef.setName("My First Tool");
        toolDef.setDescription("Greets someone");
        toolDef.addInputParameter("input", "string", "Name to greet");
        config.addTool(toolDef);

        // 5. Create an event
        AgentEvent event = new AgentEvent();
        event.setFlowId("flow-001");
        event.setUserId("user-001");
        event.setAgentId("my-agent");
        event.setEventType(AgentEventType.TOOL_CALL_REQUESTED);

        // 6. Create tool request
        ToolCallRequest request = new ToolCallRequest(
            "request-001", "flow-001", "user-001", "my-agent");
        request.setToolId("my-first-tool");
        request.addParameter("input", "World");
        event.putData("toolCallRequest", request);

        // 7. Create data stream
        DataStream<AgentEvent> events = env.fromElements(event);

        // 8. Execute
        events.print();
        env.execute("My First Agent");
    }
}
```

### Step 3: Build and Run

```bash
mvn clean package

java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  org.agentic.flink.example.MyFirstAgentExample
```

## 🎯 Next Steps

Now that you've got the basics, you can:

1. **Learn the concepts** - Read [CONCEPTS.md](CONCEPTS.md) to understand how everything works
2. **Study examples** - Check out [EXAMPLES.md](EXAMPLES.md) for detailed walkthroughs
3. **Build more tools** - Create tools that connect to your systems
4. **Add validation** - Make your agents check their work
5. **Enable memory** - Give your agents long-term memory with RAG

## 📚 Additional Resources

- **[CONCEPTS.md](CONCEPTS.md)** - Deep dive into core concepts
- **[EXAMPLES.md](EXAMPLES.md)** - Detailed example walkthroughs
- **[AGENT_FRAMEWORK.md](AGENT_FRAMEWORK.md)** - Complete framework documentation
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues and solutions

## ❓ Common Questions

### Why is my agent not responding?

**Check:**
1. Is Ollama running? `curl http://localhost:11434`
2. Did you download the models? `ollama list`
3. Check the logs for errors

### The build failed, what do I do?

**Most common causes:**
1. Java version too old: `java -version` (need 11+)
2. Maven not found: `mvn -version`
3. Internet connection (Maven downloads dependencies)

**Solution:**
```bash
# Clean and rebuild
mvn clean
mvn package
```

### Can I use OpenAI instead of Ollama?

**Yes!** Just change the config:

```java
LLMConfig config = new LLMConfig();
config.setAiModel(AiModel.OPENAI);

Map<String, String> props = new HashMap<>();
props.put("apiKey", "your-api-key-here");
props.put("modelName", "gpt-5.4-mini");
config.setProperties(props);
```

### How do I debug my agent?

**Enable detailed logging:**
```bash
# Add to src/main/resources/log4j2.properties
logger.agent.name = org.agentic.flink
logger.agent.level = DEBUG
```

## 🎉 You Did It!

You've successfully:
- ✅ Set up the development environment
- ✅ Built the project
- ✅ Run your first AI agent
- ✅ Modified an agent
- ✅ Understood the code structure

**Now go build something amazing!** 🚀

---

**Next:** Read [CONCEPTS.md](CONCEPTS.md) to understand how agents work under the hood.
