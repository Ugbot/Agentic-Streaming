# 🔧 Troubleshooting Guide

**Having issues? This guide covers common problems and their solutions.**

## 📋 Table of Contents

1. [Setup & Installation Issues](#setup--installation-issues)
2. [Build & Compilation Issues](#build--compilation-issues)
3. [Runtime Errors](#runtime-errors)
4. [Ollama & LLM Issues](#ollama--llm-issues)
5. [Qdrant & RAG Issues](#qdrant--rag-issues)
6. [Performance Issues](#performance-issues)
7. [Context & Memory Issues](#context--memory-issues)
8. [Debugging Tips](#debugging-tips)

---

## Setup & Installation Issues

### ❌ Problem: "java: command not found"

**Symptom:**
```bash
$ java -version
-bash: java: command not found
```

**Cause:** Java is not installed or not in your PATH.

**Solution:**

1. **Check if Java is installed:**
   ```bash
   # On Mac/Linux
   which java

   # On Windows
   where java
   ```

2. **Install Java 11 or higher:**
   - Download from https://adoptium.net/
   - Choose your OS and Java 11+
   - Run the installer

3. **Verify installation:**
   ```bash
   java -version
   # Should show: openjdk version "11.0.x" or higher
   ```

4. **If still not working, add to PATH:**

   **On Mac/Linux:**
   ```bash
   # Add to ~/.bashrc or ~/.zshrc
   export JAVA_HOME=/path/to/java
   export PATH=$JAVA_HOME/bin:$PATH

   # Reload
   source ~/.bashrc  # or source ~/.zshrc
   ```

   **On Windows:**
   - Open "Environment Variables"
   - Add JAVA_HOME = `C:\Program Files\Java\jdk-11`
   - Add `%JAVA_HOME%\bin` to PATH

---

### ❌ Problem: "mvn: command not found"

**Symptom:**
```bash
$ mvn -version
-bash: mvn: command not found
```

**Cause:** Maven is not installed or not in your PATH.

**Solution:**

1. **Install Maven:**

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
   - Download from https://maven.apache.org/download.cgi
   - Extract to `C:\Program Files\Maven`
   - Add `C:\Program Files\Maven\bin` to PATH

2. **Verify:**
   ```bash
   mvn -version
   # Should show Maven version 3.6 or higher
   ```

---

### ❌ Problem: "Ollama server not running"

**Symptom:**
```
Error: Failed to connect to Ollama at http://localhost:11434
Connection refused
```

**Cause:** Ollama is not running.

**Solution:**

1. **Start Ollama:**
   ```bash
   ollama serve
   ```

   **Leave this terminal open!** Ollama needs to keep running.

2. **In a new terminal, verify:**
   ```bash
   curl http://localhost:11434
   # Should return: "Ollama is running"
   ```

3. **If port 11434 is in use:**
   ```bash
   # Find what's using the port
   lsof -i :11434  # Mac/Linux
   netstat -ano | findstr :11434  # Windows

   # Kill the process or run Ollama on different port
   OLLAMA_HOST=0.0.0.0:11435 ollama serve
   ```

---

## Build & Compilation Issues

### ❌ Problem: "BUILD FAILURE - Dependencies could not be resolved"

**Symptom:**
```
[ERROR] Failed to execute goal on project agentic-flink:
Could not resolve dependencies for project org.agentic.flink:agentic-flink:jar:1.0.0-SNAPSHOT
```

**Cause:** Maven cannot download dependencies (network issue or repository problem).

**Solution:**

1. **Check internet connection:**
   ```bash
   ping maven.apache.org
   ```

2. **Clear Maven cache and retry:**
   ```bash
   rm -rf ~/.m2/repository
   mvn clean package
   ```

3. **Try with different Maven repository:**
   Add to `pom.xml`:
   ```xml
   <repositories>
       <repository>
           <id>central</id>
           <url>https://repo.maven.apache.org/maven2</url>
       </repository>
   </repositories>
   ```

4. **Use Maven with debug output:**
   ```bash
   mvn clean package -X
   ```

---

### ❌ Problem: "Package does not exist" compilation errors

**Symptom:**
```
[ERROR] /path/to/file.java:[10,34] package org.agentic.flink.core does not exist
```

**Cause:** Import path is incorrect or files are missing.

**Solution:**

1. **Verify project structure:**
   ```bash
   ls -la src/main/java/org/agentic/flink/
   ```

2. **Reimport Maven project:**
   ```bash
   mvn clean install
   ```

3. **If using IDE (IntelliJ/Eclipse):**
   - IntelliJ: File → Invalidate Caches / Restart
   - Eclipse: Project → Clean

4. **Verify Java version:**
   ```bash
   mvn -version
   # Check Java version matches requirements
   ```

---

### ❌ Problem: "OutOfMemoryError during build"

**Symptom:**
```
[ERROR] The build process ran out of memory
java.lang.OutOfMemoryError: Java heap space
```

**Cause:** Maven doesn't have enough memory to build.

**Solution:**

1. **Increase Maven memory:**
   ```bash
   export MAVEN_OPTS="-Xmx2g -XX:MaxMetaspaceSize=512m"
   mvn clean package
   ```

2. **On Windows:**
   ```cmd
   set MAVEN_OPTS=-Xmx2g -XX:MaxMetaspaceSize=512m
   mvn clean package
   ```

3. **Or create `.mvn/jvm.config`:**
   ```bash
   mkdir -p .mvn
   echo "-Xmx2g -XX:MaxMetaspaceSize=512m" > .mvn/jvm.config
   ```

---

## Runtime Errors

### ❌ Problem: "ClassNotFoundException" when running

**Symptom:**
```
Exception in thread "main" java.lang.ClassNotFoundException:
org.agentic.flink.example.SimpleAgentExample
```

**Cause:** Class is not in the JAR or wrong classpath.

**Solution:**

1. **Verify the JAR was built:**
   ```bash
   ls -lh target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
   # Should be ~38MB
   ```

2. **Check if class exists in JAR:**
   ```bash
   jar tf target/agentic-flink-1.0.0-SNAPSHOT-uber.jar | grep SimpleAgentExample
   ```

3. **Rebuild with clean:**
   ```bash
   mvn clean package
   ```

4. **Run with correct classpath:**
   ```bash
   java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
     org.agentic.flink.example.SimpleAgentExample
   ```

---

### ❌ Problem: "Could not find or load main class"

**Symptom:**
```
Error: Could not find or load main class org.agentic.flink.example.SimpleAgentExample
```

**Cause:** Classpath is incorrect or JAR is corrupted.

**Solution:**

1. **Use correct syntax:**
   ```bash
   # Correct
   java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar org.agentic.flink.example.SimpleAgentExample

   # Wrong (missing -cp)
   java target/agentic-flink-1.0.0-SNAPSHOT-uber.jar org.agentic.flink.example.SimpleAgentExample
   ```

2. **Check package name:**
   ```bash
   # Look inside the JAR
   unzip -l target/agentic-flink-1.0.0-SNAPSHOT-uber.jar | grep SimpleAgentExample
   ```

3. **Rebuild:**
   ```bash
   mvn clean package -DskipTests
   ```

---

### ❌ Problem: "NoSuchMethodError" at runtime

**Symptom:**
```
java.lang.NoSuchMethodError: 'void org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.setParallelism(int)'
```

**Cause:** Version conflict between dependencies.

**Solution:**

1. **Check dependency tree:**
   ```bash
   mvn dependency:tree
   ```

2. **Look for version conflicts:**
   ```bash
   mvn dependency:tree | grep flink
   ```

3. **Force specific version in `pom.xml`:**
   ```xml
   <dependencyManagement>
       <dependencies>
           <dependency>
               <groupId>org.apache.flink</groupId>
               <artifactId>flink-streaming-java</artifactId>
               <version>1.17.2</version>
           </dependency>
       </dependencies>
   </dependencyManagement>
   ```

4. **Clean and rebuild:**
   ```bash
   mvn clean install -U
   ```

---

## Ollama & LLM Issues

### ❌ Problem: "Model not found" error

**Symptom:**
```
Error: model 'llama2:latest' not found
Try pulling it first with: ollama pull llama2:latest
```

**Cause:** The AI model hasn't been downloaded.

**Solution:**

1. **Pull the model:**
   ```bash
   ollama pull llama2:latest
   ```

2. **Wait for download (may take 5-10 minutes):**
   ```
   pulling manifest
   pulling 8934d96d3f08... 100% ▕████████████▏ 3.8 GB
   pulling 8c17c2ebb0ea... 100% ▕████████████▏ 7.0 KB
   pulling 7c23fb36d801... 100% ▕████████████▏ 4.8 KB
   pulling 2e0493f67d0c... 100% ▕████████████▏ 59 B
   pulling fa304d675061... 100% ▕████████████▏ 91 B
   pulling 42ba7f8a01dd... 100% ▕████████████▏ 557 B
   success
   ```

3. **Verify model is available:**
   ```bash
   ollama list
   # Should show llama2:latest
   ```

4. **If you want a different model:**
   ```bash
   # Smaller, faster model
   ollama pull phi

   # Code-focused model
   ollama pull codellama

   # More powerful model
   ollama pull mistral
   ```

---

### ❌ Problem: "Ollama timeout" or very slow responses

**Symptom:**
```
[WARN] Ollama request timed out after 60000ms
Tool execution failed: Request timeout
```

**Cause:** Model is too large for your hardware or Ollama is overloaded.

**Solution:**

1. **Check Ollama is responsive:**
   ```bash
   curl http://localhost:11434/api/tags
   ```

2. **Try a smaller model:**
   ```bash
   ollama pull phi  # 2.7B parameters, much faster
   ```

   Update your code:
   ```java
   LLMConfig config = new LLMConfig();
   config.setModelName("phi:latest");  // Use smaller model
   ```

3. **Increase timeout in your code:**
   ```java
   config.setTimeoutMs(120000);  // 2 minutes instead of 1
   ```

4. **Check system resources:**
   ```bash
   # Mac/Linux
   top
   # Look for high CPU/memory usage

   # Windows
   taskmgr
   ```

5. **Restart Ollama:**
   ```bash
   # Kill Ollama
   pkill ollama

   # Restart
   ollama serve
   ```

---

### ❌ Problem: "Connection refused" to Ollama

**Symptom:**
```
Failed to connect to http://localhost:11434
java.net.ConnectException: Connection refused
```

**Cause:** Ollama is not running or running on different port.

**Solution:**

1. **Check if Ollama is running:**
   ```bash
   curl http://localhost:11434
   ```

2. **If not running, start it:**
   ```bash
   ollama serve
   ```

3. **If running on different host/port, configure:**
   ```java
   LLMConfig config = new LLMConfig();
   config.setBaseUrl("http://your-server:11434");
   ```

4. **Check firewall:**
   ```bash
   # Allow port 11434
   # Mac/Linux
   sudo ufw allow 11434

   # Windows Firewall - allow port 11434
   ```

---

## Qdrant & RAG Issues

### ❌ Problem: "Cannot connect to Qdrant"

**Symptom:**
```
Error: Failed to connect to Qdrant at localhost:6333
Connection refused
```

**Cause:** Qdrant is not running.

**Solution:**

1. **Start Qdrant with Docker:**
   ```bash
   docker run -d -p 6333:6333 qdrant/qdrant
   ```

2. **Verify it's running:**
   ```bash
   curl http://localhost:6333
   # Should return Qdrant version info
   ```

3. **Check Docker status:**
   ```bash
   docker ps | grep qdrant
   ```

4. **View Qdrant logs:**
   ```bash
   docker logs <container-id>
   ```

5. **Access Qdrant dashboard:**
   Open http://localhost:6333/dashboard in your browser

---

### ❌ Problem: "Collection not found" in Qdrant

**Symptom:**
```
Error: Collection 'agent-knowledge' not found in Qdrant
```

**Cause:** The collection hasn't been created yet.

**Solution:**

1. **Check existing collections:**
   ```bash
   curl http://localhost:6333/collections
   ```

2. **Create the collection manually:**
   ```bash
   curl -X PUT http://localhost:6333/collections/agent-knowledge \
     -H 'Content-Type: application/json' \
     -d '{
       "vectors": {
         "size": 768,
         "distance": "Cosine"
       }
     }'
   ```

3. **Or let the code create it automatically:**
   ```java
   QdrantConfig config = new QdrantConfig();
   config.setAutoCreateCollection(true);
   config.setVectorSize(768);  // Match your embedding model
   ```

---

### ❌ Problem: "Embedding dimension mismatch"

**Symptom:**
```
Error: Vector dimension mismatch. Expected 768, got 384
```

**Cause:** Embedding model produces different dimension than Qdrant collection expects.

**Solution:**

1. **Check your embedding model dimension:**
   ```bash
   # For nomic-embed-text (Ollama)
   # Dimension: 768

   # For sentence-transformers/all-MiniLM-L6-v2
   # Dimension: 384
   ```

2. **Recreate Qdrant collection with correct dimension:**
   ```bash
   # Delete old collection
   curl -X DELETE http://localhost:6333/collections/agent-knowledge

   # Create with correct dimension
   curl -X PUT http://localhost:6333/collections/agent-knowledge \
     -H 'Content-Type: application/json' \
     -d '{
       "vectors": {
         "size": 384,
         "distance": "Cosine"
       }
     }'
   ```

3. **Or update your code to match:**
   ```java
   config.setVectorSize(384);  // Match your model
   ```

---

## Performance Issues

### ❌ Problem: "Agent is very slow"

**Symptom:**
- Simple operations take > 10 seconds
- High CPU usage
- System freezes

**Diagnosis:**

1. **Check where time is spent:**
   ```bash
   # Enable debug logging
   # Add to log4j2.properties:
   logger.agent.level = DEBUG
   ```

2. **Common bottlenecks:**
   - LLM inference (Ollama)
   - Vector search (Qdrant)
   - Context compaction
   - Network calls

**Solutions:**

**For Ollama slowness:**
```java
// Use smaller/faster model
config.setModelName("phi:latest");  // Instead of llama2

// Reduce max tokens
config.setMaxTokens(500);  // Instead of 2000

// Disable validation for non-critical operations
config.setValidationEnabled(false);
```

**For Qdrant slowness:**
```java
// Reduce search results
searchParams.put("max_results", 3);  // Instead of 10

// Use faster distance metric
collectionConfig.setDistance("Dot");  // Instead of Cosine
```

**For context compaction:**
```java
// Increase token limit (compact less often)
context.setMaxTokens(8000);  // Instead of 4000

// Disable compaction for short conversations
config.setEnableContextCompaction(false);
```

---

### ❌ Problem: "OutOfMemoryError during execution"

**Symptom:**
```
java.lang.OutOfMemoryError: Java heap space
Exception in thread "main"
```

**Cause:** Not enough memory allocated to Java process.

**Solution:**

1. **Increase heap size:**
   ```bash
   java -Xmx4g -Xms1g \
     -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
     org.agentic.flink.example.SimpleAgentExample
   ```

2. **For persistent fix, create run script:**
   ```bash
   #!/bin/bash
   java -Xmx4g -Xms1g \
        -XX:+UseG1GC \
        -XX:MaxMetaspaceSize=512m \
        -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
        org.agentic.flink.example.SimpleAgentExample
   ```

3. **Monitor memory usage:**
   ```java
   // Add to your code
   Runtime runtime = Runtime.getRuntime();
   long memory = runtime.totalMemory() - runtime.freeMemory();
   System.out.println("Used memory: " + (memory / 1024 / 1024) + "MB");
   ```

4. **Reduce memory usage:**
   ```java
   // Smaller context windows
   context.setMaxTokens(2000);

   // More aggressive compaction
   config.setCompactionThreshold(0.8);  // Compact at 80% full

   // Disable long-term memory if not needed
   config.setEnableLongTermMemory(false);
   ```

---

## Context & Memory Issues

### ❌ Problem: "Context keeps getting lost"

**Symptom:**
- Agent doesn't remember previous conversation
- "I don't have that information" when it should

**Cause:** Context is being cleared or not properly maintained.

**Diagnosis:**

1. **Check context size:**
   ```java
   LOG.info("Context size: {} tokens, {} items",
       context.getCurrentTokens(),
       context.getItems().size());
   ```

2. **Check compaction settings:**
   ```java
   LOG.info("Compaction threshold: {}, enabled: {}",
       config.getCompactionThreshold(),
       config.isCompactionEnabled());
   ```

**Solutions:**

1. **Increase context limits:**
   ```java
   config.setMaxContextTokens(8000);  // More room
   ```

2. **Set important items as MUST:**
   ```java
   context.addContext(new ContextItem(
       "User's name is John",
       ContextPriority.MUST,  // Never forget this!
       MemoryType.LONG_TERM
   ));
   ```

3. **Disable compaction temporarily:**
   ```java
   config.setEnableContextCompaction(false);
   ```

4. **Enable long-term memory:**
   ```java
   config.setEnableLongTermMemory(true);
   config.setEnableInverseRag(true);  // Store important context in Qdrant
   ```

---

### ❌ Problem: "Context growing too large"

**Symptom:**
```
[WARN] Context size: 15000/4000 tokens (overflow!)
[WARN] Compaction unable to free enough space
```

**Cause:** Too much context being added, not enough being removed.

**Solution:**

1. **Enable automatic compaction:**
   ```java
   config.setEnableContextCompaction(true);
   config.setCompactionThreshold(0.8);  // Compact at 80%
   ```

2. **Use proper priorities:**
   ```java
   // Critical - never remove
   ContextPriority.MUST

   // Important - remove if needed
   ContextPriority.SHOULD

   // Nice to have - remove first
   ContextPriority.COULD

   // Not needed - remove immediately
   ContextPriority.WONT
   ```

3. **Set TTL (time to live):**
   ```java
   ContextItem item = new ContextItem(...);
   item.setTtl(Duration.ofHours(1));  // Remove after 1 hour
   ```

4. **Manual cleanup:**
   ```java
   // Remove old items
   context.removeItemsOlderThan(Duration.ofHours(24));

   // Remove by tag
   context.removeItemsByTag("temporary");
   ```

---

## Debugging Tips

### 🔍 Enable Debug Logging

**Add to `src/main/resources/log4j2.properties`:**

```properties
# Debug everything
rootLogger.level = DEBUG

# Or specific packages
logger.agent.name = org.agentic.flink
logger.agent.level = DEBUG

logger.langchain.name = org.agentic.flink.langchain
logger.langchain.level = DEBUG

logger.context.name = org.agentic.flink.context
logger.context.level = DEBUG
```

---

### 🔍 Add Diagnostic Logging

**In your code:**

```java
// Log events
LOG.debug("Processing event: flowId={}, type={}, data={}",
    event.getFlowId(),
    event.getEventType(),
    event.getData());

// Log tool execution
LOG.debug("Executing tool: {} with params: {}",
    toolId, parameters);

// Log context state
LOG.debug("Context: {}/{} tokens, {} items, priority breakdown: {}",
    context.getCurrentTokens(),
    context.getMaxTokens(),
    context.getItems().size(),
    context.getPriorityBreakdown());

// Log validation
LOG.debug("Validation result: valid={}, errors={}, score={}",
    result.isValid(),
    result.getErrors(),
    result.getScore());
```

---

### 🔍 Use Flink Web UI

**Enable Flink's web UI for monitoring:**

```java
Configuration conf = new Configuration();
conf.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, true);

StreamExecutionEnvironment env =
    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
```

**Then open:** http://localhost:8081

**You can see:**
- Running jobs
- Task manager stats
- Backpressure
- Checkpoints
- Metrics

---

### 🔍 Test Components Individually

**Test tool in isolation:**

```java
@Test
public void testCalculatorTool() {
    CalculatorTool tool = new CalculatorTool();

    Map<String, Object> params = Map.of(
        "operation", "+",
        "a", 2,
        "b", 40
    );

    CompletableFuture<Object> result = tool.execute(params);
    assertEquals(42, result.get());
}
```

**Test LLM connection:**

```java
@Test
public void testOllamaConnection() {
    LangChainAsyncClient client = new LangChainAsyncClient(...);
    List<ChatMessage> messages = List.of(
        new UserMessage("Say 'hello'")
    );

    CompletableFuture<Response<AiMessage>> response =
        client.generate(messages, config);

    assertNotNull(response.get().content());
}
```

---

### 🔍 Common Log Messages Explained

**Normal operation:**

```
[INFO] Agent agent-001: Received event type=TOOL_CALL_REQUESTED
→ Agent got a task

[INFO] Agent agent-001: Executing tool=calculator
→ Agent is using a tool

[INFO] Tool execution completed: result=42
→ Tool finished successfully

[INFO] Validation passed for flow-001
→ Result was checked and is correct

[INFO] Agent agent-001: Flow completed successfully
→ Task is done!
```

**Warnings (usually okay):**

```
[WARN] Context compaction triggered: 4200/4000 tokens
→ Memory is full, cleaning up (automatic, expected)

[WARN] Tool execution took 5234ms (slow)
→ Tool is slower than expected (might be okay)

[WARN] Correction attempt 2/3 for flow-001
→ Second attempt to fix a mistake (expected if validation failed)
```

**Errors (need attention):**

```
[ERROR] Tool execution failed: Connection timeout
→ Tool couldn't complete (check tool service)

[ERROR] Validation failed after 3 attempts, escalating to supervisor
→ Agent couldn't fix the problem (human review needed)

[ERROR] Failed to restore state from Redis
→ Memory system issue (check Redis)
```

---

## 📚 Additional Resources

**If you're still stuck:**

1. **Read the docs:**
   - [GETTING_STARTED.md](GETTING_STARTED.md) - Setup guide
   - [CONCEPTS.md](CONCEPTS.md) - How things work
   - [EXAMPLES.md](EXAMPLES.md) - Working examples
   - [AGENT_FRAMEWORK.md](AGENT_FRAMEWORK.md) - Complete reference

2. **Check the examples:**
   - `SimpleAgentExample.java` - Basic workflow
   - `RagAgentExample.java` - Document search
   - `ContextManagementExample.java` - Memory management

3. **Search for similar issues:**
   - Apache Flink documentation
   - LangChain4J documentation
   - Ollama documentation
   - Qdrant documentation

4. **Enable debug logging and read carefully:**
   - Often the logs tell you exactly what's wrong
   - Look for ERROR and WARN messages
   - Check stack traces for root cause

---

## 🆘 Emergency Checklist

**If nothing works, try this reset procedure:**

```bash
# 1. Stop everything
pkill java
pkill ollama
docker stop $(docker ps -q)

# 2. Clean build
cd /Users/bengamble/Agentic-Flink
rm -rf target/
rm -rf ~/.m2/repository/org/agentic
mvn clean

# 3. Rebuild
mvn package

# 4. Restart services
ollama serve &
docker run -d -p 6333:6333 qdrant/qdrant

# 5. Wait 10 seconds
sleep 10

# 6. Pull models
ollama pull llama2:latest
ollama pull nomic-embed-text

# 7. Test
java -cp target/agentic-flink-1.0.0-SNAPSHOT-uber.jar \
  org.agentic.flink.example.SimpleAgentExample
```

---

**Still having issues?**

Check the project's issue tracker or documentation for updates. The error message is usually your best friend - read it carefully!

**Happy debugging!** 🐛🔧
