# 🔗 Apache Flink Agents Integration Guide

**Status:** Integration plan ready | Waiting for Maven artifacts
**Last Updated:** 2025 (based on Flink Agents v0.1 release)

## 📖 What is Apache Flink Agents?

Apache Flink Agents is the **official** event-driven AI agent framework from the Apache Flink community. Released as v0.1 in late 2024, it provides native support for building intelligent, scalable AI agents on top of Apache Flink's distributed streaming engine.

### Key Features

| Feature | Description |
|---------|-------------|
| **Official Apache Project** | Part of the Flink ecosystem with community support |
| **Event-Driven Architecture** | Actions triggered by Events, enabling complex workflows |
| **ReAct Pattern** | Autonomous reasoning-acting loops with minimal configuration |
| **Workflow Pattern** | Explicit control over action sequences |
| **MCP Protocol** | Standard Model Context Protocol for tool calling |
| **Exactly-Once Consistency** | Flink checkpointing + write-ahead logging |
| **Dual Language Support** | Python and Java APIs |
| **Sub-Millisecond Latency** | Production-grade performance at scale |
| **Native Observability** | Meta-events for action start/end monitoring |

---

## 🎯 Why Integrate?

### What Flink Agents Brings

1. **Official Support** - Long-term roadmap aligned with Apache Flink
2. **ReAct Agents** - Autonomous decision-making without manual workflows
3. **MCP Protocol** - Standard tool calling, ecosystem compatibility
4. **Better Observability** - Built-in action tracking and meta-events
5. **Dynamic Topologies** - More flexible agent workflows
6. **Community** - Active development, examples, and support

### What We Keep (Our Secret Sauce)

1. **Advanced Context Management** - MoSCoW prioritization, 5-phase compaction, inverse RAG
2. **Validation/Correction Patterns** - Multi-attempt with supervisor escalation
3. **Comprehensive RAG** - Document ingestion, semantic search, embeddings (Qdrant)
4. **Battle-Tested Implementation** - Working code with real examples
5. **Beginner-Friendly Docs** - Complete learning path already created

---

## 🏗️ Integration Strategy: Hybrid Evolution

**Goal:** Use Flink Agents as the foundation while preserving our innovations.

```
┌─────────────────────────────────────────────────────────────┐
│                    Hybrid Architecture                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐         ┌──────────────────┐          │
│  │  Flink Agents    │         │  Our Extensions  │          │
│  │  (Official Core) │ ◄────►  │  (Innovations)   │          │
│  ├──────────────────┤         ├──────────────────┤          │
│  │ • ReAct Pattern  │         │ • Context Mgmt   │          │
│  │ • Workflows      │         │ • Validation     │          │
│  │ • Event Model    │         │ • RAG Tools      │          │
│  │ • MCP Protocol   │         │ • Compaction     │          │
│  │ • Observability  │         │ • Supervisor     │          │
│  └──────────────────┘         └──────────────────┘          │
│           ↓                            ↓                    │
│  ┌─────────────────────────────────────────────┐            │
│  │         Adapter Layer (Translation)         │            │
│  │  • Event conversion                         │            │
│  │  • Tool wrapping                            │            │
│  │  • Seamless interop                         │            │
│  └─────────────────────────────────────────────┘            │
│                          ↓                                  │
│              Apache Flink Streaming Engine                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔍 Comparison: Custom vs Flink Agents vs Hybrid

| Feature | Custom (Current) | Flink Agents | Hybrid (Planned) |
|---------|------------------|--------------|------------------|
| **Core Framework** | ✓ Custom | ✓ Official | ✓ Official |
| **Workflow Control** | ✓ Manual | ✓ Built-in | ✓ Best of both |
| **ReAct Autonomy** | ✗ Not available | ✓ Native | ✓ Native |
| **Context Management** | ✓✓ Advanced (MoSCoW) | ~ Basic | ✓✓ Advanced |
| **Validation/Correction** | ✓ Multi-attempt | ~ Manual | ✓ Multi-attempt |
| **RAG Tools** | ✓ Qdrant integrated | ~ Plugin-based | ✓ Full suite |
| **Supervisor Escalation** | ✓ Built-in | ~ Manual | ✓ Built-in |
| **Apache Community** | ✗ Independent | ✓ Official | ✓ Official |
| **Observability** | ~ Good | ✓✓ Excellent | ✓✓ Excellent |
| **MCP Protocol** | ✗ Not implemented | ✓ Native | ✓ Native |
| **Future-Proof** | ~ Uncertain | ✓ Apache roadmap | ✓ Apache roadmap |

**Legend:**
- ✓✓ = Excellent
- ✓ = Good
- ~ = Basic/Manual
- ✗ = Not available

---

## 📦 Apache Flink Agents Architecture

### Event-Driven Model

Flink Agents uses an event-centric orchestration model:

```java
// Agents are composed of Actions triggered by Events
@action(listenEvents = {UserRequestEvent.class})
public void handleUserRequest(UserRequestEvent event) {
    // Action logic here
    // Can emit new events
}
```

### Core Components

**1. Events**
Messages that trigger agent actions. Built-in and custom events supported.

**2. Actions**
Functions that execute when specific events occur. Annotated with `@action`.

**3. Agents**
Collections of actions that process events. Two types:
- **Workflow Agents** - Explicit action sequences
- **ReAct Agents** - Autonomous reasoning loops

**4. Resources**
LLM models, vector stores, tools available to agents.

**5. Meta-Events**
System events for observability (action start/end, tool calls, etc.)

### Example: Workflow Agent (Java)

```java
public class ReviewAnalysisAgent extends Agent {

    @Prompt
    public static Prompt reviewAnalysisPrompt() {
        return new Prompt("Analyze product reviews...");
    }

    @ChatModelSetup
    public static ResourceDescriptor reviewAnalysisModel() {
        return ResourceDescriptor.Builder.newBuilder(OllamaChatModelSetup.class)
            .addInitialArgument("model", "qwen3:8b")
            .addInitialArgument("prompt", "reviewAnalysisPrompt")
            .addInitialArgument("tools", List.of("notify_manager"))
            .build();
    }

    @action(listenEvents = {ReviewEvent.class})
    public void analyzeReview(ReviewEvent event) {
        // Agent analyzes review using LLM
        // Decides which tools to use
        // Emits result events
    }
}
```

### Example: ReAct Agent

ReAct (Reasoning + Acting) agents autonomously decide tool sequences:

```java
ReActAgent agent = ReActAgent.create()
    .model(ollamaModel)
    .tools(List.of("calculator", "weather", "search"))
    .build();

// Agent autonomously:
// 1. Reasons about the task
// 2. Decides which tool to use
// 3. Executes the tool
// 4. Reasons about the result
// 5. Repeats until complete
```

---

## 🚀 Implementation Roadmap

### Phase 1: Foundation (✅ Completed)

**Goal:** Prepare project for Flink Agents integration

**Actions Completed:**
- ✅ Added Flink Agents dependency placeholders in `pom.xml`
- ✅ Added Apache snapshot repository for early access
- ✅ Documented build-from-source instructions
- ✅ Created this integration guide

**Notes:**
- Flink Agents v0.1.0 artifacts not yet in Maven Central
- Can build from source: `git clone https://github.com/apache/flink-agents.git`
- When artifacts available, uncomment dependencies in pom.xml

---

### Phase 2: Adapter Layer (Planned)

**Goal:** Bridge between our events and Flink Agents events

**New Classes:**

```
org.agentic.flink.flintagents.adapter/
├── FlinkAgentsEventAdapter.java     # Convert AgentEvent ↔ Flink Agent Event
├── FlinkAgentsToolAdapter.java      # Wrap our tools as Flink Agent Actions
├── FlinkAgentsWorkflowBuilder.java  # Build workflows from our configs
└── FlinkAgentsReActWrapper.java     # Wrap ReAct agents with our extensions
```

**Example Adapter:**

```java
public class FlinkAgentsEventAdapter {

    // Convert our event to Flink Agents event
    public static Event toFlinkAgentEvent(AgentEvent ourEvent) {
        return Event.create()
            .setId(ourEvent.getFlowId())
            .setType(mapEventType(ourEvent.getEventType()))
            .setPayload(ourEvent.getData())
            .setTimestamp(ourEvent.getTimestamp())
            .build();
    }

    // Convert Flink Agents event back to our event
    public static AgentEvent fromFlinkAgentEvent(Event flinkEvent) {
        AgentEvent ourEvent = new AgentEvent();
        ourEvent.setFlowId(flinkEvent.getId());
        ourEvent.setEventType(mapEventType(flinkEvent.getType()));
        ourEvent.setData((Map<String, Object>) flinkEvent.getPayload());
        ourEvent.setTimestamp(flinkEvent.getTimestamp());
        return ourEvent;
    }
}
```

---

### Phase 3: ReAct Integration (Planned)

**Goal:** Add autonomous agent support using Flink Agents

**New Features:**

```java
// ReAct agent that uses our validation and context management
public class HybridReActAgent extends ReActAgent {

    private ValidationFunction validator;
    private AgentContext contextManager;

    @Override
    @action(listenEvents = {ToolExecutionCompletedEvent.class})
    public void validateToolResult(ToolExecutionCompletedEvent event) {
        // Use our validation logic
        ValidationResult result = validator.validate(event.getResult());

        if (!result.isValid()) {
            // Use our correction mechanism
            emit(new CorrectionRequestedEvent(result.getFeedback()));
        } else {
            // Continue ReAct loop
            emit(new ValidationPassedEvent());
        }
    }

    @Override
    @action(listenEvents = {ContextOverflowEvent.class})
    public void compactContext(ContextOverflowEvent event) {
        // Use our MoSCoW compaction
        contextManager.compact();
        emit(new ContextCompactedEvent());
    }
}
```

---

### Phase 4: Enhanced Observability (Planned)

**Goal:** Leverage Flink Agents meta-events for better monitoring

**Metrics Dashboard:**

```
┌──────────────────────────────────────────────────────────┐
│  Flink Agents + Our Extensions - Unified Dashboard       │
├──────────────────────────────────────────────────────────┤
│                                                            │
│  Tool Execution Times:                                    │
│    calculator:        avg 12ms  (p50: 10ms, p99: 45ms)   │
│    semantic_search:   avg 234ms (p50: 200ms, p99: 500ms) │
│    rag_query:         avg 567ms (p50: 450ms, p99: 1200ms)│
│                                                            │
│  ReAct Reasoning Steps:                                   │
│    avg per task: 3.2 steps                                │
│    max observed: 8 steps                                  │
│                                                            │
│  Validation Results:                                      │
│    ✓ Passed:    847 (94.1%)                               │
│    ✗ Failed:     53  (5.9%)                               │
│    → Corrected:  47  (88.7% of failures)                  │
│    → Escalated:   6  (11.3% to supervisor)                │
│                                                            │
│  Context Management:                                      │
│    Compactions:  12  (avg every 45 tasks)                 │
│    Avg tokens:   2.1K / 4K (52% usage)                    │
│    Inverse RAG:   8 high-value items archived             │
│                                                            │
└──────────────────────────────────────────────────────────┘
```

**Implementation:**

```java
public class EnhancedObservabilityCollector {

    @action(listenEvents = {ActionStartedEvent.class})
    public void onActionStart(ActionStartedEvent event) {
        // Track action start time
        metrics.recordActionStart(event.getActionId(), event.getTimestamp());
    }

    @action(listenEvents = {ActionCompletedEvent.class})
    public void onActionComplete(ActionCompletedEvent event) {
        // Track action duration
        long duration = event.getTimestamp() - getStartTime(event.getActionId());
        metrics.recordActionDuration(event.getActionId(), duration);

        // Track our validation metrics
        if (event.wasValidated()) {
            metrics.recordValidation(event.validationPassed());
        }
    }

    @action(listenEvents = {ContextCompactionEvent.class})
    public void onContextCompaction(ContextCompactionEvent event) {
        // Track context compaction metrics
        metrics.recordCompaction(
            event.getTokensBefore(),
            event.getTokensAfter(),
            event.getItemsRemoved()
        );
    }
}
```

---

### Phase 5: Port Unique Features (Planned)

**Goal:** Make our innovations work with Flink Agents

**1. Context Management as Actions**

```java
@action(listenEvents = {ContextOverflowEvent.class})
public void compactContextMoSCoW(ContextOverflowEvent event) {
    // Phase 1: Remove WONT items
    context.removeWontItems();

    // Phase 2: Score relevancy
    context.scoreRelevancy(event.getCurrentIntent());

    // Phase 3: Remove low-relevancy COULD
    context.removeLowRelevancyCould();

    // Phase 4: Compress SHOULD
    context.compressShouldItems();

    // Phase 5: Promote to long-term
    context.promoteToLongTerm();

    emit(new ContextCompactedEvent(context.getCurrentSize()));
}
```

**2. RAG Tools as Native Actions**

```java
@action(listenEvents = {SearchRequestEvent.class})
public void semanticSearch(SearchRequestEvent event) {
    // Our Qdrant integration
    List<String> results = qdrantClient.search(
        event.getQuery(),
        event.getMaxResults()
    );

    emit(new SearchResultsEvent(results));
}

@action(listenEvents = {DocumentIngestionEvent.class})
public void ingestDocument(DocumentIngestionEvent event) {
    // Our document chunking and embedding
    List<DocumentChunk> chunks = chunker.chunk(event.getDocument());
    List<Embedding> embeddings = embedder.embed(chunks);

    qdrantClient.upsert(embeddings);

    emit(new DocumentIngestedEvent(chunks.size()));
}
```

---

## 🎁 Benefits Summary

### Immediate Gains (When Integrated)

1. **Official Apache Support** - Community, roadmap, long-term maintenance
2. **ReAct Agents** - Autonomous reasoning without manual workflow coding
3. **MCP Protocol** - Standard tool calling, ecosystem compatibility
4. **Better Observability** - Meta-events, action tracking, real-time monitoring
5. **Future-Proof** - Flink Agents will evolve with Apache Flink

### We Retain

1. **Advanced Context Management** - MoSCoW, compaction, inverse RAG (unique)
2. **Validation/Correction** - Multi-attempt with supervisor escalation (unique)
3. **Comprehensive RAG** - Document ingestion, semantic search, embeddings
4. **Great Documentation** - Beginner-friendly guides we created
5. **Battle-Tested Code** - Working implementation with real examples

---

## 📖 Learning Resources

### Official Flink Agents Resources

- **Documentation**: https://nightlies.apache.org/flink/flink-agents-docs-release-0.1/
- **GitHub**: https://github.com/apache/flink-agents
- **FLIP-531**: https://cwiki.apache.org/confluence/display/FLINK/FLIP-531:+Initiate+Flink+Agents+as+a+new+Sub-Project

### Our Documentation

- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Setup and first agent
- **[CONCEPTS.md](CONCEPTS.md)** - Core concepts explained
- **[EXAMPLES.md](EXAMPLES.md)** - Detailed walkthroughs
- **[AGENT_FRAMEWORK.md](AGENT_FRAMEWORK.md)** - Complete reference
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues

---

## 🤝 Contributing Back to Apache

As we integrate Flink Agents, we plan to contribute our innovations back to the Apache community:

**Potential Contributions:**

1. **MoSCoW Context Management** - Advanced prioritization for context windows
2. **Validation/Correction Patterns** - Multi-attempt error correction framework
3. **Inverse RAG** - Automatic high-value context archival to vector stores
4. **5-Phase Compaction** - Intelligent memory management algorithm
5. **Supervisor Escalation** - Human-in-the-loop integration patterns

**Benefits:**
- Our work becomes part of official Flink Agents
- Broader adoption and testing
- Community maintenance and improvements
- Recognition in Apache ecosystem

---

## ⚠️ Current Status & Next Steps

### Current Status (As of Implementation)

- ✅ Integration plan complete
- ✅ POM prepared with dependency placeholders
- ✅ Apache snapshot repository added
- ✅ Integration documentation written
- ⏳ Waiting for Flink Agents artifacts in Maven Central

### When Artifacts Are Available

1. **Uncomment dependencies** in `pom.xml`
2. **Create adapter package** structure
3. **Implement event translation** layer
4. **Build ReAct agent examples** showing both patterns
5. **Port context management** to Flink Agents actions
6. **Update documentation** with working examples

### Try It Now (Build from Source)

If you want to experiment before Maven artifacts are available:

```bash
# 1. Clone Flink Agents
git clone https://github.com/apache/flink-agents.git
cd flink-agents

# 2. Build
./tools/build.sh

# 3. Install to local Maven
mvn install -DskipTests

# 4. Uncomment dependencies in our pom.xml
# 5. Rebuild our project
cd /Users/bengamble/Agentic-Flink
mvn clean package
```

---

## 💡 Example: Side-by-Side Comparison

### Before (Custom Framework)

```java
// Our custom event-driven framework
AgentConfig config = new AgentConfig("agent-001");
config.addTool("calculator");
config.setValidationEnabled(true);
config.setMaxCorrectionAttempts(3);

AgentExecutionStream stream = new AgentExecutionStream(env, config, toolRegistry);
DataStream<AgentEvent> results = stream.createAgentStream(events);

// Manual workflow configuration
// Custom validation/correction
// Our context management
```

### After (Hybrid with Flink Agents)

```java
// Option 1: Workflow Agent (explicit control)
Agent workflowAgent = AgentWorkflow.create()
    .model(ollamaModel)
    .tools(List.of("calculator"))
    .validator(ourValidationAction)      // 🎯 Our custom validation!
    .contextManager(ourContextManager)    // 🎯 Our context management!
    .supervisor(ourSupervisorAction)      // 🎯 Our supervisor escalation!
    .build();

// Option 2: ReAct Agent (autonomous)
Agent reactAgent = ReActAgent.create()
    .model(ollamaModel)
    .tools(List.of("calculator", "search", "weather"))
    .validator(ourValidationAction)      // 🎯 Same validation!
    .contextManager(ourContextManager)    // 🎯 Same context mgmt!
    .build();

// Run with Flink Agents runtime + our extensions
AgentRuntime runtime = new AgentRuntime(env);
DataStream<Event> results = runtime.execute(reactAgent, events);

// Official framework + our innovations = Best of both worlds ✨
```

---

## 🎯 Success Criteria

When integration is complete, we will have:

- ✅ **Backward Compatibility** - All existing examples still work
- ✅ **Feature Parity** - Everything we have now + ReAct agents + MCP
- ✅ **Performance** - No degradation, ideally improvements from Flink optimizations
- ✅ **Documentation** - Clear migration path and comparison guides
- ✅ **Community** - Pathway to contribute back to Apache Flink Agents
- ✅ **Observability** - Enhanced monitoring with meta-events
- ✅ **Future-Proof** - Aligned with Apache Flink roadmap

---

## 📅 Timeline Estimate

**Total: 3-4 weeks** (once Flink Agents artifacts are available)

- **Week 1**: Adapter layer + basic integration
- **Week 2**: ReAct agents + observability
- **Week 3**: Port unique features (context, validation, RAG)
- **Week 4**: Documentation + examples + testing

**Current:** Ready to go once Maven artifacts published! 🚀

---

## 🆘 Need Help?



**For Our Framework:**
- Check existing docs: [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
- Read integration examples in this guide

---

**Status:** Ready for integration when Flink Agents artifacts become available in Maven Central. All preparation work completed! 🎉
