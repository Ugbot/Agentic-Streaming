# Agentic-Flink & Apache Flink Agents: Integration Guide

**Last Updated:** October 2025
**Document Version:** 1.0
**Project Version:** Agentic-Flink 1.0.0-SNAPSHOT | Apache Flink Agents 0.1.0

---

## Executive Summary

This document explains how **Agentic-Flink** integrates with the official **Apache Flink Agents** framework, demonstrating how these complementary projects work together to deliver well-tested, event-driven AI agents at scale.

### Key Points

- **Complementary, Not Competing**: Both projects share the same vision—scalable, event-driven AI agents—but approach it from different angles
- **Independent Development**: Agentic-Flink was designed and built before Apache Flink Agents 0.1.0 was released (October 2025)
- **Integration Path**: Agentic-Flink is actively integrating with Flink Agents through a plugin architecture
- **Best of Both Worlds**: Combine Flink Agents' official patterns with Agentic-Flink's well-tested innovations

### Quick Comparison

| Aspect | Agentic-Flink | Apache Flink Agents |
|--------|---------------|---------------------|
| **Status** | Early-stage (112 tests passing) | Preview (0.1.0, not production-recommended) |
| **Focus** | LangChain4J integration, context management, storage | ReAct/Workflow patterns, MCP protocol, observability |
| **Origin** | Independent research project | Official Apache sub-project |
| **LLM Integration** | LangChain4J (Ollama, OpenAI, extensible) | Native LLM support (various providers) |
| **Storage** | Two-tier (Redis + PostgreSQL, 107 tests) | Not specified in core framework |
| **Context Management** | MoSCoW prioritization, 5-phase compaction | Event-based orchestration |
| **Unique Features** | Validation/correction patterns, tiered agents, RAG tools | MCP protocol, exactly-once action consistency |

**Recommendation**: Use both together—Flink Agents for official patterns and tooling, Agentic-Flink for well-tested storage, context management, and advanced features.

---

## Table of Contents

1. [Understanding Both Projects](#understanding-both-projects)
2. [Historical Context](#historical-context)
3. [Integration Architecture](#integration-architecture)
4. [Feature Comparison](#feature-comparison)
5. [Use Cases & When to Use What](#use-cases--when-to-use-what)
6. [Getting Started with Integration](#getting-started-with-integration)
7. [Current Status & Roadmap](#current-status--roadmap)
8. [Contributing to Apache Flink Agents](#contributing-to-apache-flink-agents)
9. [FAQ](#faq)

---

## Understanding Both Projects

### What is Agentic-Flink?

**Agentic-Flink** is an early-stage framework for building AI agents on Apache Flink with LangChain4J integration. It provides:

#### Core Capabilities
- **LangChain4J Integration**: Seamless connection to Ollama, OpenAI, and other LLM providers
- **Context Management**: MoSCoW prioritization (MUST/SHOULD/COULD/WONT) with automatic compaction
- **Two-Tier Storage**: Redis (hot, sub-millisecond) + PostgreSQL (warm, durable) with 107 passing tests
- **Tool Framework**: @Tool annotation support with automatic discovery and retry logic
- **Flink CEP Orchestration**: Pattern-based event processing for multi-agent workflows
- **Validation & Correction**: Multi-attempt validation with supervisor escalation
- **RAG Capabilities**: Document ingestion, semantic search, and embedding support

#### Status
- ✅ **Build**: SUCCESS (112 tests, 0 failures)
- ✅ **Well-Tested Components**: Storage, context management, tool framework
- ✅ **Working Examples**: 14 real examples including TieredAgentExample with live LLM calls
- ✅ **Infrastructure**: One-command Docker setup (PostgreSQL + Redis + Ollama)
- ⚠️ **Maturity**: Early-stage software - well-tested but still new, not battle-tested at scale

#### Philosophy
Comprehensive testing, real working code, transparent about current capabilities and limitations.

---

### What is Apache Flink Agents?

**Apache Flink Agents** is the official Apache Flink sub-project for building event-driven AI agents directly on Flink's streaming runtime. It provides:

#### Core Capabilities
- **Event-Driven Architecture**: Agents consume and process continuous event streams
- **Agent Patterns**:
  - **Workflow Agents**: Pre-defined execution flows with tools and prompts
  - **ReAct Agents**: Reasoning + Acting pattern for autonomous decision-making
- **MCP Protocol Support**: Model Context Protocol for tool and prompt integration
- **Exactly-Once Action Consistency**: Integrates Flink checkpointing with external write-ahead log
- **Multi-Language APIs**: Native Python and Java support
- **Rich Ecosystem**: Mainstream LLM integration, vector stores, MCP servers
- **Observability**: Event-centric orchestration with full event log visibility
- **Distributed Runtime**: Lightweight local development + full distributed Flink runtime

#### Status
- 🚧 **Version**: 0.1.0 (Preview release, October 2025)
- ⚠️ **Production Use**: Not recommended for production environments with high stability requirements
- 🔄 **API Stability**: Experimental APIs subject to non-backward compatible changes
- 📈 **Development**: Active development by Apache community (Alibaba Cloud, LinkedIn, Confluent)

#### Philosophy
Built to bridge stream processing and autonomous agents with enterprise-grade reliability and scale.

---

## Historical Context

### The Timeline

Understanding how these projects evolved helps explain their relationship:

```
2024 Q1-Q3
├─ Agentic-Flink: Origins - LangChain4J + Flink Experimentation
│  • Started as research into integrating LangChain4J with Apache Flink
│  • Exploring: How can LLMs work with streaming data at scale?
│  • Building: Basic tool execution and agent orchestration patterns
│  • Foundation: Flink CEP for event processing + LangChain4J for LLM calls
│
2024 Q4
├─ Agentic-Flink: Initial framework development begins
│  Focus: Solving production problems discovered during experimentation
│  • How to manage context windows effectively?
│  • How to store agent state reliably at scale?
│  • How to validate and correct agent outputs?
│  • How to orchestrate multi-step agent workflows?
│  Using: Apache Flink CEP + LangChain4J + custom patterns
│
2025 Q1-Q3
├─ Agentic-Flink: Core framework maturation
│  • Context management with MoSCoW prioritization (MUST/SHOULD/COULD/WONT)
│  • Two-tier storage architecture (Redis + PostgreSQL)
│  • Tool framework with LangChain4J @Tool annotations
│  • Validation/correction patterns with supervisor escalation
│  • 112 tests written, well-tested components emerge
│  • 14 working examples including TieredAgentExample
│
2025 May-August
├─ Apache Flink Agents: FLIP-531 proposal and development
│  • Official Apache sub-project initiated (separate from Agentic-Flink)
│  • MVP development (model support, tools, context management)
│  • Multi-agent communication patterns
│  • Community collaboration (Alibaba Cloud, LinkedIn, Confluent)
│
2025 October
├─ Apache Flink Agents: 0.1.0 preview release
│  • First public release of official Apache framework
│  • ReAct and Workflow patterns available
│  • MCP protocol support
│  • Preview status (not production-recommended yet)
│
2025 Q4 (Now)
├─ Agentic-Flink: Integration phase
│  • Recognition: Apache Flink Agents is the official path forward
│  • Plugin architecture implemented for seamless integration
│  • Event/tool adapters completed (bidirectional conversion)
│  • Hybrid approach designed: Flink Agents + Agentic-Flink innovations
│  • Documentation for both standalone and integrated use
│  • Preparation to contribute innovations back to Apache
│
2026 Q1-Q2 (Planned)
└─ Full Integration & Convergence
   • Flink Agents v1.0 stable release
   • Seamless hybrid architecture in production
   • Contribution of Agentic-Flink innovations back to Apache
   • MoSCoW context management proposed as FLIP
   • Validation/correction patterns shared with community
```

### Why Two Projects?

**Different Origins, Shared Vision:**

1. **Agentic-Flink** started in 2024 as independent research into **LangChain4J + Flink integration**:
   - **Origin**: Experimentation with connecting LLMs to streaming data using LangChain4J
   - **Evolution**: Discovered production challenges and built solutions organically
   - **Focus**: Context management, storage, validation, LangChain4J ecosystem
   - **Philosophy**: Build working solutions first, standardize later
   - **Key Questions Addressed**:
     - How to manage context windows effectively?
     - How to store agent state reliably at scale?
     - How to validate and correct agent outputs?
     - How to integrate LangChain4J tools with Flink streams?

2. **Apache Flink Agents** emerged in 2025 from enterprise needs identified by the Apache community:
   - **Origin**: Official Apache initiative to standardize agent patterns on Flink
   - **Collaboration**: Multiple companies (Alibaba Cloud, LinkedIn, Confluent) + Apache
   - **Focus**: ReAct/Workflow patterns, MCP protocol, Python APIs, governance
   - **Philosophy**: Design-first approach with community consensus (FLIP process)
   - **Key Goals**:
     - Standardize event-driven agent patterns
     - Provide official Apache-maintained framework
     - Enable enterprise adoption with governance and stability
     - Bridge data streaming and AI agents seamlessly

**The Result**: Two complementary approaches that benefit from integration—Agentic-Flink brings battle-tested implementations and LangChain4J expertise, while Flink Agents provides official patterns and Apache governance.

---

## Integration Architecture

### The Hybrid Approach

Agentic-Flink uses a **plugin architecture** to integrate with Apache Flink Agents while maintaining its independent capabilities:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Hybrid Architecture                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │        Apache Flink Agents (Foundation)                 │   │
│  │  • ReAct & Workflow patterns                            │   │
│  │  • MCP protocol support                                 │   │
│  │  • Exactly-once action consistency                      │   │
│  │  • Event-centric orchestration                          │   │
│  │  • Multi-language APIs (Python/Java)                    │   │
│  └───────────────────┬─────────────────────────────────────┘   │
│                      │                                           │
│                      ↓ Integration Layer                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │        Agentic-Flink Plugin Architecture                │   │
│  │  • FlinkAgentsEventAdapter (bidirectional conversion)   │   │
│  │  • FlinkAgentsToolAdapter (tool wrapping)               │   │
│  │  • ContextManagementAction (MoSCoW integration)         │   │
│  └───────────────────┬─────────────────────────────────────┘   │
│                      │                                           │
│                      ↓ Enhanced Features                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │        Agentic-Flink Innovations                        │   │
│  │  • MoSCoW context management                            │   │
│  │  • Two-tier storage (Redis + PostgreSQL)               │   │
│  │  • Validation/correction patterns                       │   │
│  │  • Advanced RAG with Qdrant                            │   │
│  │  • LangChain4J integration                              │   │
│  │  • Supervisor escalation patterns                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Component Integration

#### 1. Event Adapter (`FlinkAgentsEventAdapter.java`)

Provides bidirectional conversion between event formats:

```java
// Agentic-Flink Event → Flink Agents Event
public static Event toFlinkAgentsEvent(AgentEvent agentEvent) {
    return Event.builder()
        .flowId(agentEvent.getFlowId())
        .userId(agentEvent.getUserId())
        .agentId(agentEvent.getAgentId())
        .eventType(agentEvent.getEventType())
        .data(agentEvent.getData())
        .timestamp(agentEvent.getTimestamp())
        .build();
}

// Flink Agents Event → Agentic-Flink Event
public static AgentEvent fromFlinkAgentsEvent(Event flinkAgentsEvent) {
    return AgentEvent.builder()
        .flowId(flinkAgentsEvent.getFlowId())
        .userId(flinkAgentsEvent.getUserId())
        .agentId(flinkAgentsEvent.getAgentId())
        .eventType(flinkAgentsEvent.getEventType())
        .data(flinkAgentsEvent.getData())
        .timestamp(flinkAgentsEvent.getTimestamp())
        .build();
}
```

**Features:**
- Zero-copy conversion when possible
- Validation and error handling
- Support for both InputEvent and OutputEvent types
- Debug utilities for tracing conversions

#### 2. Tool Adapter (`FlinkAgentsToolAdapter.java`)

Wraps Agentic-Flink tools as Flink Agents Actions:

```java
public class ToolWrapperAgent extends Agent {
    private final ToolExecutor toolExecutor;

    @Override
    public void process(Event event) {
        if (event instanceof ToolRequestEvent) {
            ToolRequestEvent request = (ToolRequestEvent) event;

            // Execute using Agentic-Flink tool framework
            ToolExecutionResult result = toolExecutor.execute(
                request.getToolName(),
                request.getParameters()
            );

            // Emit Flink Agents response
            emit(new ToolResponseEvent(
                event.getFlowId(),
                request.getToolName(),
                result.getOutput(),
                result.getMetadata()
            ));
        }
    }
}
```

**Features:**
- Automatic tool registration from @Tool annotations
- Parameter validation and type conversion
- Error handling and retry logic
- MCP schema generation for tool definitions

#### 3. Context Management Integration

Agentic-Flink's MoSCoW context management can be used as a Flink Agents Action:

```java
public class ContextManagementAction extends Action {
    private final MoSCoWContextManager contextManager;
    private final RedisShortTermStore redisStore;
    private final PostgresConversationStore postgresStore;

    @Override
    public void execute(Event event) {
        // Load context from two-tier storage
        AgentContext context = loadContextFromStorage(event.getFlowId());

        // Apply MoSCoW prioritization and compaction
        AgentContext compacted = contextManager.compactContext(
            context,
            MAX_TOKEN_BUDGET
        );

        // Save back to storage
        saveContextToStorage(event.getFlowId(), compacted);

        // Emit context-updated event
        emit(new ContextUpdatedEvent(event.getFlowId(), compacted));
    }
}
```

---

## Feature Comparison

### Detailed Feature Matrix

| Feature | Agentic-Flink | Apache Flink Agents | Notes |
|---------|---------------|---------------------|-------|
| **Core Framework** |
| Event-Driven Architecture | ✅ Custom patterns | ✅ Official patterns | Compatible approaches |
| Flink CEP Integration | ✅ Production-ready | ✅ Native support | Both leverage Flink CEP |
| State Management | ✅ Two-tier storage | ✅ Flink state backend | Complementary |
| **Agent Patterns** |
| ReAct Pattern | ⚠️ Custom implementation | ✅ Official support | Use Flink Agents |
| Workflow Pattern | ⚠️ Custom implementation | ✅ Official support | Use Flink Agents |
| Tiered Agents | ✅ Production examples | ❌ Not specified | Agentic-Flink innovation |
| Multi-Agent Coordination | 🚧 In development | ✅ Native support | Use Flink Agents |
| **LLM Integration** |
| LangChain4J | ✅ Native integration | ❌ Not primary focus | Agentic-Flink strength |
| OpenAI | ✅ Via LangChain4J | ✅ Native support | Both support |
| Ollama (Local LLMs) | ✅ Production-ready | ⚠️ Community support | Agentic-Flink optimized |
| Custom LLM Providers | ✅ Via LangChain4J | ✅ Extensible | Both extensible |
| **Context & Memory** |
| MoSCoW Prioritization | ✅ Production-ready | ❌ Not specified | Agentic-Flink innovation |
| 5-Phase Compaction | ✅ Tested algorithm | ❌ Not specified | Agentic-Flink innovation |
| Token Budget Management | ✅ Automatic | ⚠️ Manual | Agentic-Flink automation |
| Temporal Relevancy | ✅ Implemented | ❌ Not specified | Agentic-Flink feature |
| **Storage & Persistence** |
| Redis (Hot Tier) | ✅ 5 tests, working | ⚠️ Custom integration | Agentic-Flink ready |
| PostgreSQL (Warm Tier) | ✅ 31 tests, working | ⚠️ Custom integration | Agentic-Flink ready |
| Exactly-Once Persistence | ✅ Via Flink checkpoints | ✅ With write-ahead log | Both support |
| Storage Abstraction | ✅ Multi-backend | ⚠️ Implementation-dependent | Agentic-Flink flexible |
| **Tool Framework** |
| Tool Registration | ✅ @Tool annotations | ✅ MCP protocol | Both support |
| Built-in Tools | ✅ Calculator, RAG, etc. | ⚠️ Community-provided | Agentic-Flink has library |
| Custom Tools | ✅ Easy via LangChain4J | ✅ MCP servers | Both extensible |
| Tool Validation | ✅ Multi-attempt | ⚠️ Implementation-dependent | Agentic-Flink automation |
| **Validation & Quality** |
| Output Validation | ✅ LLM-based validation | ⚠️ Custom implementation | Agentic-Flink pattern |
| Automatic Correction | ✅ Multi-attempt | ⚠️ Custom implementation | Agentic-Flink pattern |
| Supervisor Escalation | ✅ Human-in-loop | ⚠️ Custom implementation | Agentic-Flink pattern |
| Feedback Loops | ✅ Implemented | ⚠️ Custom implementation | Agentic-Flink feature |
| **RAG & Embeddings** |
| Document Ingestion | ✅ Working examples | ⚠️ Custom implementation | Agentic-Flink ready |
| Vector Search | 🚧 Qdrant integration | ✅ Multiple providers | Use Flink Agents |
| Semantic Search | ✅ Via LangChain4J | ✅ Native support | Both support |
| Embedding Models | ✅ Via LangChain4J | ✅ Multiple providers | Both support |
| **Protocols & Standards** |
| MCP Protocol | ⚠️ Via adapters | ✅ Native support | Use Flink Agents |
| Agent-to-Agent (A2A) | 🚧 Custom patterns | ⚠️ Emerging standard | Both evolving |
| **Production Features** |
| Testing | ✅ 112 tests | ⚠️ Preview (unstable APIs) | Agentic-Flink mature |
| Docker Setup | ✅ One-command | ⚠️ Custom setup | Agentic-Flink ready |
| Monitoring | 🚧 In development | ✅ Event logs | Use Flink Agents |
| Production Documentation | ✅ Comprehensive | ✅ Official docs | Both have docs |
| **APIs & Languages** |
| Java API | ✅ Native | ✅ Native | Both support |
| Python API | ❌ Java-focused | ✅ Native PyFlink | Use Flink Agents |
| Type Safety | ✅ Strong typing | ✅ Strong typing | Both support |

**Legend:**
- ✅ Production-ready or officially supported
- ⚠️ Possible but requires custom work
- 🚧 In development
- ❌ Not available or not a focus

---

## Use Cases & When to Use What

### Decision Matrix

#### Use Agentic-Flink Standalone When:

1. **You Need Storage Implementation Now**
   - Two-tier storage (Redis + PostgreSQL) with 107 passing tests
   - Functional for development and testing
   - Early-stage but tested

2. **Advanced Context Management is Critical**
   - MoSCoW prioritization for token budget management
   - 5-phase compaction algorithm
   - Automatic context pruning based on relevancy

3. **LangChain4J Integration is Preferred**
   - Already using LangChain4J ecosystem
   - Need Ollama for local development
   - Want @Tool annotation support

4. **Validation/Correction Patterns Required**
   - Multi-attempt validation with LLM review
   - Automatic correction loops
   - Supervisor escalation for edge cases

5. **Rapid Development is Priority**
   - 112 tests passing, actively developed codebase
   - One-command Docker setup
   - 14 working examples to learn from

**Example Use Case:**
> "Building a customer support agent prototype. Need storage, context management for token limits, and validation patterns. Early-stage software suitable for development and testing."

---

#### Use Apache Flink Agents Standalone When:

1. **Official Apache Support is Required**
   - Enterprise governance and stability requirements
   - Need Apache license and community support
   - Long-term Apache ecosystem commitment

2. **ReAct or Workflow Patterns are Ideal**
   - Standard agent patterns fit your use case
   - Don't need extensive customization
   - Want community-supported patterns

3. **MCP Protocol Integration is Key**
   - Already using MCP servers
   - Need standardized tool/prompt integration
   - Want ecosystem compatibility

4. **Python API is Required**
   - Team primarily uses Python
   - PyFlink experience exists
   - Want Python-native development

5. **Multi-Agent Communication is Central**
   - Complex agent-to-agent workflows
   - Need event-driven agent coordination
   - Want official patterns for agent mesh

**Example Use Case:**
> "We're building a multi-agent system for financial fraud detection with 20+ specialized agents communicating via events. We need official Apache patterns, Python APIs for our data science team, and MCP protocol for tool integration."

---

#### Use Both Together (Hybrid Approach) When:

1. **Best of Both Worlds Needed**
   - Official Flink Agents patterns + production-ready storage
   - ReAct agents + MoSCoW context management
   - MCP protocol + validation/correction patterns

2. **Gradual Migration Desired**
   - Starting with Agentic-Flink, moving to Flink Agents
   - Want to leverage existing Agentic-Flink investments
   - Need time to evaluate Flink Agents maturity

3. **Contributing Back to Apache**
   - Testing Agentic-Flink innovations with Flink Agents
   - Planning to propose features upstream
   - Want to influence Flink Agents roadmap

4. **Maximum Flexibility Required**
   - Complex, evolving requirements
   - Need multiple integration points
   - Want to pick best tool for each job

**Example Use Case:**
> "Building an agent platform using Flink Agents' ReAct patterns for reasoning, Agentic-Flink's storage for persistence, MoSCoW context management for efficiency, with plans to contribute validation patterns back to Apache."

---

### Example Architectures

#### Architecture 1: Agentic-Flink Standalone

```
┌─────────────────────────────────────────────────┐
│  Kafka → Flink CEP → LangChain4J → Tools        │
│    ↓                      ↓                      │
│  MoSCoW Context   →   Redis (Hot)               │
│    ↓                      ↓                      │
│  Validation       →   PostgreSQL (Warm)         │
└─────────────────────────────────────────────────┘
```

**Components:**
- Event source: Kafka or custom
- Orchestration: Flink CEP with custom patterns
- LLM: LangChain4J (Ollama, OpenAI, etc.)
- Context: MoSCoW prioritization
- Storage: Two-tier (Redis + PostgreSQL)
- Tools: @Tool annotations

**Best For:** Development and testing needing storage and context management

---

#### Architecture 2: Apache Flink Agents Standalone

```
┌─────────────────────────────────────────────────┐
│  Event Stream → ReAct Agent → MCP Tools         │
│    ↓                ↓              ↓             │
│  Flink State  → Native LLM → Vector Store       │
│    ↓                                             │
│  Event Logs (Observability)                     │
└─────────────────────────────────────────────────┘
```

**Components:**
- Event source: Flink DataStream
- Orchestration: ReAct or Workflow patterns
- LLM: Native Flink Agents integration
- Tools: MCP protocol
- Storage: Custom implementation
- Observability: Event-centric logging

**Best For:** New projects wanting official Apache patterns and Python support

---

#### Architecture 3: Hybrid Approach

```
┌────────────────────────────────────────────────────────────┐
│  Kafka → Flink Agents (ReAct) → Agentic-Flink Plugin      │
│    ↓           ↓                        ↓                   │
│  MCP Tools → LangChain4J → MoSCoW Context → Redis          │
│    ↓           ↓                        ↓         ↓         │
│  Event Logs → Validation → Correction → PostgreSQL         │
└────────────────────────────────────────────────────────────┘
```

**Components:**
- Event source: Kafka
- Orchestration: Flink Agents (ReAct pattern)
- LLM: Both Flink Agents native + LangChain4J
- Tools: MCP + @Tool annotations
- Context: MoSCoW prioritization (plugin)
- Storage: Two-tier via plugin
- Validation: Agentic-Flink patterns

**Best For:** Maximum capabilities with flexibility to evolve

---

## Getting Started with Integration

### Prerequisites

1. **Build Apache Flink Agents** (currently 0.1.0-SNAPSHOT):
   ```bash
   git clone https://github.com/apache/flink-agents.git
   cd flink-agents
   mvn clean install -DskipTests
   ```

2. **Clone Agentic-Flink**:
   ```bash
   git clone <agentic-flink-repo>
   cd Agentic-Flink
   ```

3. **Start Infrastructure**:
   ```bash
   docker compose up -d
   docker compose exec ollama ollama pull qwen2.5:3b
   ```

### Option 1: Standalone Agentic-Flink (No Flink Agents)

```bash
# Build without Flink Agents plugin
mvn clean compile

# Run example
mvn exec:java -Dexec.mainClass="org.agentic.flink.example.TieredAgentExample"
```

**What You Get:**
- Full Agentic-Flink capabilities
- No Flink Agents dependencies
- Production-ready components

---

### Option 2: Hybrid with Flink Agents Plugin

```bash
# Build WITH Flink Agents support
mvn clean compile -P flink-agents

# Run hybrid example
mvn exec:java -Dexec.mainClass="org.agentic.flink.plugins.flintagents.examples.FlinkAgentsIntegrationExample"
```

**What You Get:**
- Flink Agents ReAct/Workflow patterns
- Agentic-Flink storage and context management
- Full integration capabilities

---

### Code Example: Hybrid Integration

```java
import org.agentic.flink.plugins.flintagents.adapter.*;
import org.apache.flink.agents.*;

public class HybridAgentJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Create Flink Agents ReAct agent
        ReActAgent reactAgent = ReActAgent.builder()
            .model("gpt-5.4-mini")
            .tools(mcpServer("my-tools"))
            .build();

        // 2. Wrap with Agentic-Flink context management
        ContextManagementAction contextAction =
            new ContextManagementAction(
                new MoSCoWContextManager(),
                new RedisShortTermStore(),
                new PostgresConversationStore()
            );

        // 3. Create event stream
        DataStream<Event> events = env
            .fromSource(kafkaSource, "kafka-source");

        // 4. Process with Flink Agents
        DataStream<Event> reactResults = events
            .map(reactAgent::process)
            .name("ReAct Agent");

        // 5. Apply Agentic-Flink context management
        DataStream<Event> withContext = reactResults
            .map(contextAction::execute)
            .name("Context Management");

        // 6. Add validation (Agentic-Flink pattern)
        DataStream<Event> validated = withContext
            .flatMap(new ValidationAgent())
            .name("Validation");

        // 7. Store results (Agentic-Flink storage)
        validated.addSink(new PostgresSink());

        env.execute("Hybrid Agent Job");
    }
}
```

---

## Current Status & Roadmap

### Integration Status (October 2025)

| Component | Status | Notes |
|-----------|--------|-------|
| **Event Adapter** | ✅ Complete | Bidirectional conversion working |
| **Tool Adapter** | ✅ Complete | @Tool to MCP wrapping functional |
| **Context Integration** | ✅ Complete | MoSCoW works as Flink Agents Action |
| **Storage Integration** | ✅ Complete | Two-tier storage as plugin |
| **Example Code** | ✅ Complete | 4 integration examples ready |
| **Documentation** | ✅ Complete | This document + plugin README |
| **Maven Artifacts** | ⚠️ Waiting | Flink Agents not in Maven Central yet |
| **Production Testing** | 🚧 Pending | Waiting for Flink Agents v1.0 stable |

### Short-Term Roadmap (Q4 2025 - Q1 2026)

**Phase 1: Monitor Flink Agents Maturity**
- Track Flink Agents releases (0.1.0 → 0.2.0 → 1.0.0)
- Test integration with each release
- Report issues to Apache community
- Update adapters as APIs stabilize

**Phase 2: Expand Integration Examples**
- More hybrid architecture examples
- Performance benchmarking (standalone vs. hybrid)
- Migration guides from standalone to hybrid
- Best practices documentation

**Phase 3: Contribute Back to Apache**
- Propose MoSCoW context management as FLIP
- Share validation/correction patterns
- Contribute storage abstractions
- Collaborate on Python API integration

### Long-Term Vision (2026+)

**Convergence Path:**
1. **v1.0 (Q4 2025)**: Agentic-Flink standalone remains primary, Flink Agents optional plugin
2. **v1.5 (Q2 2026)**: Hybrid approach becomes recommended for new projects
3. **v2.0 (Q4 2026)**: Flink Agents foundation with Agentic-Flink as enhancements
4. **v3.0 (2027+)**: Full convergence—Agentic-Flink innovations merged into Apache Flink Agents

**Key Milestone:** When Flink Agents v1.0 stable releases, Agentic-Flink will shift to plugin-first architecture. Both projects are early-stage and evolving.

---

## Contributing to Apache Flink Agents

### Features Planned for Contribution

Agentic-Flink has developed several innovations that could benefit the broader Apache Flink Agents community:

#### 1. MoSCoW Context Management

**What It Is:**
Priority-based context window management (MUST/SHOULD/COULD/WONT) with automatic compaction.

**Why Contribute:**
- Solves token budget management universally
- Tested algorithm (24 passing tests)
- Language-agnostic approach

**Contribution Path:**
- Write FLIP proposal
- Share algorithm documentation
- Provide reference implementation
- Support community discussion

---

#### 2. Two-Tier Storage Architecture

**What It Is:**
Hot (Redis) + Warm (PostgreSQL) storage abstraction with automatic hydration.

**Why Contribute:**
- Production-proven (107 tests)
- Solves distributed agent state management
- Extensible to other backends

**Contribution Path:**
- Propose storage interface standard
- Share implementation patterns
- Contribute adapter examples
- Support multi-backend ecosystem

---

#### 3. Validation/Correction Patterns

**What It Is:**
Multi-attempt validation with LLM review, automatic correction, and supervisor escalation.

**Why Contribute:**
- Common pattern across agent systems
- Improves output quality universally
- Human-in-loop integration

**Contribution Path:**
- Document pattern library
- Share example implementations
- Propose as official Flink Agents pattern
- Create reusable components

---

### How to Contribute

**To Apache Flink Agents:**
1. Join the Flink dev mailing list
2. Review FLIP-531 and ongoing discussions
3. Propose features via JIRA
4. Submit patches via GitHub PRs
5. Participate in community calls

**To Agentic-Flink:**
1. Open GitHub issues for feature requests
2. Submit PRs for bug fixes and enhancements
3. Share your use cases and experiences
4. Improve documentation and examples
5. Help with integration testing

---

## FAQ

### Q: Should I use Agentic-Flink or Apache Flink Agents?

**A:** Use both - they're complementary:
- **Agentic-Flink** for storage, context management, and validation patterns
- **Apache Flink Agents** for official patterns, MCP protocol, and Python APIs
- **Hybrid** for combined capabilities

Both are early-stage software. Use appropriate caution for deployments.

If choosing one:
- **Agentic-Flink standalone** for Java-focused development (112 tests)
- **Flink Agents standalone** for Python/MCP support when v1.0 releases

---

### Q: Will Agentic-Flink become obsolete when Flink Agents matures?

**A:** No - Agentic-Flink will evolve into a complementary plugin/enhancement layer:
- Core patterns will converge with Flink Agents
- Unique innovations (MoSCoW, validation patterns) will remain as extensions
- May contribute features upstream to Apache
- Will continue to provide implementations for emerging patterns

---

### Q: Can I migrate from Agentic-Flink to Flink Agents later?

**A:** Yes, easily:
1. Plugin architecture already provides adapters
2. Event/tool formats are compatible
3. Migration guides will be provided
4. No vendor lock-in—both use Apache Flink underneath

---

### Q: Is the hybrid approach ready for use?

**A:** For development and testing, yes. For production, not yet:
- **Agentic-Flink components**: 112 tests, early-stage
- **Flink Agents**: Preview only (v0.1.0, not production-recommended)
- **Integration layer**: Adapters complete and tested
- **Recommendation**: Both are early-stage software - suitable for development/testing, not yet recommended for production

---

### Q: How do I choose between LangChain4J and native Flink Agents LLM integration?

**A:**
- **LangChain4J** (Agentic-Flink): Mature ecosystem, Ollama support, @Tool annotations, Java-focused
- **Native Flink Agents**: Official support, Python APIs, MCP protocol, emerging ecosystem

You can use both in hybrid architecture—they're not mutually exclusive.

---

### Q: What's the performance difference?

**A:** Not benchmarked yet, but expectations:
- **Standalone Agentic-Flink**: Proven performance, optimized two-tier storage
- **Standalone Flink Agents**: Unknown (preview release)
- **Hybrid**: Slight overhead from adapter layer (~1-5%)

Benchmarks will be published when Flink Agents v1.0 releases.

---

### Q: Can I use Flink Agents' Python API with Agentic-Flink features?

**A:** Not directly, but:
- Use Flink Agents Python API for agent definition
- Use adapters to integrate with Agentic-Flink storage (via Java)
- Context management and validation can run as separate Flink operators
- Full Python integration planned for v2.0

---

### Q: How do I get help?

**For Agentic-Flink:**
- GitHub issues: [Project repository]
- Documentation: This file + project docs
- Examples: 14 working examples in codebase

**For Apache Flink Agents:**
- Apache Flink mailing lists
- Official documentation: https://nightlies.apache.org/flink/flink-agents-docs-latest/
- JIRA for bug reports

**For Integration:**
- Start with integration examples in `plugins/flintagents/examples/`
- Review plugin README
- Open issues for integration-specific questions

---

## Conclusion

**Agentic-Flink** and **Apache Flink Agents** represent two complementary approaches to building scalable, event-driven AI agents on Apache Flink:

### Key Takeaways

1. **Different Origins, Shared Goals**: Both projects aim to make scalable, reliable AI agents a reality on Flink

2. **Complementary Strengths**:
   - Agentic-Flink: Storage, context management, validation patterns (112 tests)
   - Flink Agents: Official patterns, MCP protocol, Python APIs, Apache governance

3. **Integration is Key**: Use the plugin architecture to combine capabilities

4. **Maturity**: Both are early-stage - Agentic-Flink has 112 tests; Flink Agents is preview (0.1.0). Neither recommended for production yet.

5. **Evolution Path**: Projects will converge over time with features flowing both directions

### Recommendations

**For Development Today (Q4 2025):**
- Use Agentic-Flink standalone for development/testing
- Monitor Flink Agents maturity
- Both are early-stage - evaluate for your use case
- Prepare for integration when ready

**For New Projects (2026+):**
- Start with hybrid architecture
- Use Flink Agents for patterns
- Use Agentic-Flink for storage and context features
- Contribute learnings back to both communities

**For Long-Term:**
- Expect convergence as Flink Agents matures
- Agentic-Flink innovations will enhance Flink Agents
- Both projects benefit the Apache Flink ecosystem

---

### Resources

**Agentic-Flink:**
- Repository: [GitHub URL]
- Documentation: README.md, STATUS.md, ROADMAP.md
- Examples: `src/main/java/org/agentic/flink/example/`

**Apache Flink Agents:**
- Official Site: https://flink.apache.org/
- Documentation: https://nightlies.apache.org/flink/flink-agents-docs-latest/
- FLIP-531: https://cwiki.apache.org/confluence/display/FLINK/FLIP-531

**Integration:**
- Plugin Code: `src/main/java/org/agentic/flink/plugins/flintagents/`
- Integration Examples: `plugins/flintagents/examples/`
- This Document: FLINK_AGENTS_INTEGRATION.md

---

**Last Updated:** October 2025
**Maintained By:** Agentic-Flink Community
**License:** Apache 2.0

---

*"Building the future of scalable AI agents, together with the Apache community."*
