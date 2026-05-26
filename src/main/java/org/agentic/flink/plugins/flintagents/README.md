# Apache Flink Agents Plugin

**Status:** 📋 Future Integration - Optional Plugin
**Dependencies:** Requires Apache Flink Agents (v1.0+) when available

---

## What is This?

This package contains **optional integration** with Apache Flink Agents, the official AI agent framework from the Apache Flink community.

⚠️ **This is NOT part of the core framework** - The core Agentic Flink framework works without this plugin.

---

## Current Status

Apache Flink Agents is still in early development (currently 0.2-SNAPSHOT). This plugin provides:

- Event adapters for bidirectional conversion
- Tool adapters for wrapping our tools as Flink Agents actions
- Integration patterns and examples

**When Flink Agents reaches stable 1.0 release**, this plugin will be fully functional and tested.

---

## Why Optional?

1. **Apache Flink Agents is not production-ready yet**
   - Still in SNAPSHOT phase
   - APIs may change
   - No Maven Central artifacts

2. **Core framework doesn't need it**
   - Agentic Flink works with LangChain4J + Flink CEP
   - Adding Flink Agents is enhancement, not requirement

3. **Reduces dependencies**
   - Users don't need to build Flink Agents from source
   - Smaller dependency footprint
   - Faster builds

---

## Package Structure

```
plugins/flintagents/
├── adapter/
│   ├── FlinkAgentsEventAdapter.java    # Convert events between frameworks
│   └── FlinkAgentsToolAdapter.java     # Wrap tools as Flink Agents actions
├── action/
│   └── (Flink Agents-specific actions)
└── README.md (this file)
```

---

## Using This Plugin

### Prerequisites

1. Apache Flink Agents v1.0+ installed
2. Enable the Flink Agents Maven profile

### Enable the Plugin

In `pom.xml`:

```bash
# Build with Flink Agents plugin enabled
mvn clean package -P flink-agents
```

### Example Usage

```java
// Convert our event to Flink Agents event
AgentEvent ourEvent = new AgentEvent(...);
Event flinkAgentEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(ourEvent);

// Wrap our tool as Flink Agents action
ToolExecutor ourTool = new CalculatorTools();
Action flinkAgentAction = FlinkAgentsToolAdapter.wrapTool(ourTool);
```

---

## Building Flink Agents from Source

If you want to test this plugin before Flink Agents 1.0 is released:

```bash
# Clone Apache Flink Agents
git clone https://github.com/apache/flink-agents.git
cd flink-agents

# Build and install to local Maven repo
./tools/build.sh
mvn install -DskipTests

# Return to Agentic Flink
cd /path/to/Agentic-Flink

# Build with Flink Agents profile
mvn clean compile -P flink-agents
```

---

## What This Plugin Provides

### 1. Event Translation
Bidirectional, lossless conversion between:
- Our `AgentEvent` model
- Flink Agents `Event` model

### 2. Tool Wrapping
Adapts our tools to work as Flink Agents actions

### 3. Integration Patterns
Examples of how to use both frameworks together

---

## What This Plugin Does NOT Provide

- ❌ Core agent functionality (that's in the main framework)
- ❌ LLM integration (use LangChain4J directly)
- ❌ Storage backends (use core storage package)
- ❌ Context management (use core context package)

---

## When to Use This Plugin

**Use this plugin if:**
- You want to leverage Flink Agents' ReAct pattern
- You need MCP protocol support
- You want to contribute to Apache Flink Agents ecosystem
- You're experimenting with cutting-edge features

**Don't use this plugin if:**
- You want stable, production-ready code (use core only)
- You don't want to build dependencies from source
- You're just getting started (learn core first)

---

## Future Roadmap

See [ROADMAP.md](../../../../../ROADMAP.md) for Apache Flink Agents integration plans.

**Target:** v2.0 (Q2 2026) - When Flink Agents reaches maturity

**What's planned:**
- Full ReAct agent support
- MCP protocol integration
- Enhanced observability via meta-events
- Workflow and autonomous agent modes
- Community contributions back to Apache Flink Agents

---

## Documentation

### For Core Framework
See main project documentation:
- [README.md](../../../../../README.md)
- [STATUS.md](../../../../../STATUS.md)
- [ROADMAP.md](../../../../../ROADMAP.md)

### For Flink Agents
Official Apache Flink Agents documentation:
- https://github.com/apache/flink-agents
- https://nightlies.apache.org/flink/flink-agents-docs-release-0.1/

---

## Contributing

Improvements to this plugin are welcome! Please:

1. Test with actual Flink Agents installation
2. Add unit tests
3. Document compatibility versions
4. Submit PR with clear description

---

**Status:** Waiting for Apache Flink Agents v1.0 stable release
**Last Updated:** 2025-10-22
