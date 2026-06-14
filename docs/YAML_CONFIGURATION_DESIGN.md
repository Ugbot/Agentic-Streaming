# YAML Configuration Design Document

**Status:** Design / Future Implementation
**Target Version:** v1.1
**Author:** Agentic Flink Team
**Last Updated:** 2025-10-22

---

## 1. Overview

This document outlines the design for YAML-based agent configuration, inspired by [Apache Flink CDC's](https://github.com/apache/flink-cdc) approach to declarative pipeline definitions.

### Goals

1. **Declarative Agent Definition**: Define agents, tools, and supervisor chains in YAML
2. **YAML → Java Job**: Automatically generate Flink DataStream jobs from YAML
3. **No Code Required**: Non-Java users can define complex agent workflows
4. **Validation & Type Safety**: Schema validation with helpful error messages
5. **Composability**: Reference shared components, templates, and includes

### Non-Goals

- **v1.0**: Full YAML support (deferred to v1.1)
- Replace Java API (YAML is an alternative, not replacement)
- Runtime configuration changes (YAML is compile-time only)

---

## 2. Inspiration: Apache Flink CDC

### How Flink CDC Works

Flink CDC uses a **YAML-to-Pipeline** pattern:

1. **YAML Definition** (`flink-cdc.yaml`):
   ```yaml
   source:
     type: mysql
     hostname: localhost
     port: 3306
     username: root
     database-name: inventory

   sink:
     type: kafka
     properties:
       bootstrap.servers: localhost:9092

   pipeline:
     name: MySQL to Kafka Pipeline
     parallelism: 4

   route:
     - source-table: customers
       sink-table: kafka_customers
       description: Sync customer data
   ```

2. **Pipeline Factory** (`YamlPipelineDefinitionParser.java`):
   - Parses YAML into Java objects
   - Validates schema against JSON Schema
   - Creates `PipelineDefinition` object

3. **Job Composer** (`PipelineComposer.java`):
   - Takes `PipelineDefinition`
   - Generates Flink `DataStream` job
   - Wires sources, transformations, sinks
   - Returns `StreamExecutionEnvironment`

4. **Execution**:
   ```java
   PipelineDefinition def = YamlPipelineDefinitionParser.parse("flink-cdc.yaml");
   StreamExecutionEnvironment env = PipelineComposer.buildPipeline(def);
   env.execute("CDC Pipeline");
   ```

### Key Patterns to Adopt

| Pattern | Flink CDC Implementation | Our Adaptation |
|---------|--------------------------|----------------|
| **Schema Validation** | JSON Schema for YAML validation | JSON Schema for agent definitions |
| **Factory Pattern** | `PipelineDefinition` factory | `AgentJobDefinition` factory |
| **Builder Composition** | Sources/sinks/routes → Pipeline | Agents/chains/tools → Job |
| **Configuration Objects** | `Configuration` class with typed getters | `AgentConfig`, `ToolConfig`, etc. |
| **Error Handling** | `ValidationException` with line numbers | Same, with helpful messages |

---

## 3. Proposed YAML Structure

### 3.1 Simple Agent Definition

```yaml
# simple-agent.yaml
agent:
  id: research-agent
  name: Research Specialist
  type: RESEARCHER

  llm:
    model: qwen2.5:7b
    temperature: 0.3
    max-tokens: 8000
    system-prompt: |
      You are a research specialist. Gather and synthesize information
      from multiple sources. Be thorough and cite your sources.

  tools:
    - web-search
    - document-analysis
    - synthesis

  execution:
    max-iterations: 10
    timeout: 5m
    validation-enabled: true
    max-validation-attempts: 3

  supervisor: quality-supervisor
```

### 3.2 Supervisor Chain Definition

```yaml
# supervisor-chain.yaml
supervisor-chain:
  id: research-workflow
  name: Research Workflow Chain

  escalation-policy: NEXT_TIER
  auto-escalate-threshold: 0.7
  max-escalations: 3

  tiers:
    - name: validator
      agent-ref: validation-agent

    - name: executor
      agent-ref: research-agent
      quality-threshold: 0.8

    - name: qa-review
      agent-ref: qa-agent
      quality-threshold: 0.9

    - name: final-approval
      agent-ref: supervisor-agent
      requires-human-approval: true
```

### 3.3 Complete Job Definition

```yaml
# research-job.yaml
version: "1.0"
name: Research Agent Job
description: Multi-agent research workflow with tiered supervision

# ==================== Agent Definitions ====================
agents:
  - id: validation-agent
    name: Input Validator
    type: VALIDATOR
    llm:
      model: qwen2.5:3b
      temperature: 0.1
      system-prompt: "You validate research requests for clarity and feasibility."
    execution:
      max-iterations: 2
      timeout: 30s

  - id: research-agent
    name: Research Executor
    type: RESEARCHER
    llm:
      model: qwen2.5:7b
      temperature: 0.3
      system-prompt: "You are a research specialist."
    tools:
      - web-search
      - document-analysis
      - synthesis
    execution:
      max-iterations: 10
      timeout: 5m
      validation-enabled: true
    completion-tracking:
      tasks:
        - id: web-search
          description: Search for relevant sources
          required: true
        - id: document-analysis
          description: Analyze retrieved documents
          required: true
        - id: synthesis
          description: Synthesize findings
          required: true

  - id: qa-agent
    name: Quality Reviewer
    type: SUPERVISOR
    llm:
      model: qwen2.5:7b
      temperature: 0.2
      system-prompt: "You review research quality for accuracy and completeness."
    execution:
      timeout: 2m

  - id: supervisor-agent
    name: Final Approver
    type: SUPERVISOR
    llm:
      model: qwen2.5:7b
      temperature: 0.1
      system-prompt: "You provide final approval for research outputs."

# ==================== Tool Definitions ====================
tools:
  - id: web-search
    name: Web Search
    type: builtin
    class: org.agentic.flink.tools.builtin.WebSearchTool

  - id: document-analysis
    name: Document Analysis
    type: custom
    class: com.example.tools.DocumentAnalysisTool
    config:
      max-documents: 10
      analysis-depth: detailed

# ==================== Supervisor Chain ====================
supervisor-chain:
  id: research-workflow
  name: Research Workflow Chain
  escalation-policy: NEXT_TIER
  tiers:
    - name: validator
      agent-ref: validation-agent
    - name: executor
      agent-ref: research-agent
    - name: qa-review
      agent-ref: qa-agent
    - name: final-approval
      agent-ref: supervisor-agent

# ==================== Job Configuration ====================
job:
  parallelism: 1
  checkpoint-interval: 60s

  # Kafka integration (optional)
  source:
    type: kafka
    topic: research-requests
    bootstrap-servers: localhost:9092
    group-id: research-job

  sink:
    type: kafka
    topic: research-results
    bootstrap-servers: localhost:9092

  # Storage configuration
  storage:
    short-term:
      type: redis
      host: localhost
      port: 6379
      ttl: 1h

    long-term:
      type: postgresql
      host: localhost
      port: 5432
      database: agentic_flink
      username: flink_user
```

---

## 4. Implementation Architecture

### 4.1 Component Overview

```
YAML File
    ↓
YamlAgentJobParser
    ↓
AgentJobDefinition (Java object)
    ↓
AgentJobComposer
    ↓
StreamExecutionEnvironment (Flink job)
    ↓
Execute
```

### 4.2 Key Classes (To Be Implemented)

#### `YamlAgentJobParser.java`
```java
public class YamlAgentJobParser {

  /**
   * Parses a YAML file into an AgentJobDefinition.
   *
   * @param yamlPath Path to YAML file
   * @return parsed job definition
   * @throws ValidationException if YAML is invalid
   */
  public static AgentJobDefinition parse(String yamlPath)
      throws ValidationException {
    // 1. Load YAML using SnakeYAML
    // 2. Validate against JSON Schema
    // 3. Convert to AgentJobDefinition
    // 4. Validate cross-references (agent-refs, tool-refs)
    // 5. Return definition
  }

  /**
   * Validates YAML against JSON Schema.
   */
  private static void validateSchema(Map<String, Object> yaml) {
    // Use org.everit.json.schema for validation
  }
}
```

#### `AgentJobDefinition.java`
```java
public class AgentJobDefinition implements Serializable {
  private String version;
  private String name;
  private String description;

  private List<AgentDefinition> agents;
  private List<ToolDefinition> tools;
  private SupervisorChainDefinition supervisorChain;
  private JobConfiguration jobConfig;

  // Getters, validation methods

  /**
   * Converts this definition to runtime objects.
   */
  public AgentJobSpec toJobSpec() {
    // Build Agent objects from AgentDefinitions
    // Build SupervisorChain from SupervisorChainDefinition
    // Build ToolRegistry from ToolDefinitions
    // Return complete spec
  }
}
```

#### `AgentJobComposer.java`
```java
public class AgentJobComposer {

  /**
   * Composes a Flink DataStream job from an AgentJobDefinition.
   *
   * @param definition The job definition
   * @return Flink execution environment ready to execute
   */
  public static StreamExecutionEnvironment compose(AgentJobDefinition definition) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 1. Build agents
    Map<String, Agent> agents = buildAgents(definition.getAgents());

    // 2. Build supervisor chain
    SupervisorChain chain = buildSupervisorChain(
        definition.getSupervisorChain(), agents);

    // 3. Build tool registry
    ToolRegistry toolRegistry = buildToolRegistry(definition.getTools());

    // 4. Create source (Kafka or in-memory)
    DataStream<AgentEvent> sourceStream = createSource(env, definition.getJobConfig());

    // 5. Wire agent pipeline with CEP patterns
    AgentJobGenerator generator = new AgentJobGenerator(agents, chain, toolRegistry);
    DataStream<AgentEvent> resultStream = generator.generatePipeline(sourceStream);

    // 6. Create sink
    createSink(resultStream, definition.getJobConfig());

    return env;
  }

  private static Map<String, Agent> buildAgents(List<AgentDefinition> definitions) {
    // Convert YAML AgentDefinitions to Agent objects using AgentBuilder
  }

  private static SupervisorChain buildSupervisorChain(
      SupervisorChainDefinition def, Map<String, Agent> agents) {
    // Use SupervisorChainBuilder
  }
}
```

---

## 5. Schema Validation

### JSON Schema for Agent Definition

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Agentic Flink Agent Job Schema",
  "type": "object",
  "required": ["version", "agents"],
  "properties": {
    "version": {
      "type": "string",
      "pattern": "^[0-9]+\\.[0-9]+$",
      "description": "Schema version (e.g., '1.0')"
    },
    "name": {
      "type": "string",
      "minLength": 1,
      "description": "Job name"
    },
    "agents": {
      "type": "array",
      "minItems": 1,
      "items": { "$ref": "#/definitions/agent" }
    },
    "supervisor-chain": {
      "$ref": "#/definitions/supervisor-chain"
    },
    "tools": {
      "type": "array",
      "items": { "$ref": "#/definitions/tool" }
    },
    "job": {
      "$ref": "#/definitions/job-config"
    }
  },
  "definitions": {
    "agent": {
      "type": "object",
      "required": ["id", "llm"],
      "properties": {
        "id": {
          "type": "string",
          "pattern": "^[a-z][a-z0-9-]*$",
          "description": "Agent ID (lowercase, dash-separated)"
        },
        "name": {
          "type": "string"
        },
        "type": {
          "enum": ["EXECUTOR", "VALIDATOR", "CORRECTOR", "SUPERVISOR", "COORDINATOR", "RESEARCHER", "CUSTOM"]
        },
        "llm": {
          "$ref": "#/definitions/llm-config"
        },
        "tools": {
          "type": "array",
          "items": { "type": "string" }
        },
        "execution": {
          "$ref": "#/definitions/execution-config"
        },
        "supervisor": {
          "type": "string",
          "description": "Reference to supervisor agent ID"
        }
      }
    },
    "llm-config": {
      "type": "object",
      "required": ["model", "system-prompt"],
      "properties": {
        "model": {
          "type": "string",
          "examples": ["qwen2.5:3b", "qwen2.5:7b", "gpt-5.4-mini"]
        },
        "temperature": {
          "type": "number",
          "minimum": 0.0,
          "maximum": 2.0
        },
        "max-tokens": {
          "type": "integer",
          "minimum": 100,
          "maximum": 128000
        },
        "system-prompt": {
          "type": "string",
          "minLength": 10
        }
      }
    },
    "execution-config": {
      "type": "object",
      "properties": {
        "max-iterations": {
          "type": "integer",
          "minimum": 1,
          "maximum": 100
        },
        "timeout": {
          "type": "string",
          "pattern": "^[0-9]+(s|m|h)$",
          "examples": ["30s", "5m", "1h"]
        },
        "validation-enabled": {
          "type": "boolean"
        },
        "max-validation-attempts": {
          "type": "integer",
          "minimum": 1
        }
      }
    },
    "supervisor-chain": {
      "type": "object",
      "required": ["id", "tiers"],
      "properties": {
        "id": { "type": "string" },
        "name": { "type": "string" },
        "escalation-policy": {
          "enum": ["NEXT_TIER", "SKIP_TO_TOP", "RETRY_CURRENT", "FAIL_FAST"]
        },
        "auto-escalate-threshold": {
          "type": "number",
          "minimum": 0.0,
          "maximum": 1.0
        },
        "tiers": {
          "type": "array",
          "minItems": 1,
          "items": { "$ref": "#/definitions/tier" }
        }
      }
    },
    "tier": {
      "type": "object",
      "required": ["name", "agent-ref"],
      "properties": {
        "name": { "type": "string" },
        "agent-ref": {
          "type": "string",
          "description": "Reference to agent ID"
        },
        "quality-threshold": {
          "type": "number",
          "minimum": 0.0,
          "maximum": 1.0
        },
        "requires-human-approval": {
          "type": "boolean"
        }
      }
    },
    "tool": {
      "type": "object",
      "required": ["id", "type"],
      "properties": {
        "id": { "type": "string" },
        "name": { "type": "string" },
        "type": {
          "enum": ["builtin", "custom"]
        },
        "class": {
          "type": "string",
          "description": "Fully qualified Java class name"
        },
        "config": {
          "type": "object",
          "description": "Tool-specific configuration"
        }
      }
    },
    "job-config": {
      "type": "object",
      "properties": {
        "parallelism": {
          "type": "integer",
          "minimum": 1
        },
        "checkpoint-interval": {
          "type": "string",
          "pattern": "^[0-9]+(s|m)$"
        },
        "source": { "$ref": "#/definitions/source-config" },
        "sink": { "$ref": "#/definitions/sink-config" },
        "storage": { "$ref": "#/definitions/storage-config" }
      }
    },
    "source-config": {
      "type": "object",
      "required": ["type"],
      "properties": {
        "type": {
          "enum": ["kafka", "file", "memory"]
        }
      }
    },
    "sink-config": {
      "type": "object",
      "required": ["type"],
      "properties": {
        "type": {
          "enum": ["kafka", "file", "print"]
        }
      }
    },
    "storage-config": {
      "type": "object",
      "properties": {
        "short-term": {
          "type": "object",
          "required": ["type"],
          "properties": {
            "type": { "enum": ["redis", "memory"] }
          }
        },
        "long-term": {
          "type": "object",
          "required": ["type"],
          "properties": {
            "type": { "enum": ["postgresql", "file"] }
          }
        }
      }
    }
  }
}
```

---

## 6. Error Handling & Validation

### Validation Levels

1. **Schema Validation**: YAML structure matches JSON Schema
2. **Reference Validation**: All `agent-ref`, `tool-ref` references exist
3. **Semantic Validation**: Configurations make sense (e.g., timeout > 0)
4. **Runtime Validation**: Tools/classes actually exist and are loadable

### Error Message Examples

**Good Error Message (Like Flink CDC):**
```
ValidationException: Invalid agent definition at line 15
  agents[0].llm.temperature: 2.5 exceeds maximum value of 2.0

  Suggested fix:
    llm:
      temperature: 0.7  # Must be between 0.0 and 2.0
```

**Reference Error:**
```
ValidationException: Broken reference in supervisor-chain at line 42
  tiers[1].agent-ref: 'qa-agent' does not exist

  Available agents: validation-agent, research-agent, supervisor-agent

  Did you mean 'supervisor-agent'?
```

---

## 7. Implementation Plan

### Phase 1: Core Infrastructure (v1.1-alpha)
- [ ] Add SnakeYAML dependency
- [ ] Add JSON Schema validation library
- [ ] Create `YamlAgentJobParser` skeleton
- [ ] Create `AgentJobDefinition` data classes
- [ ] Implement schema validation with helpful errors

### Phase 2: Agent Definition (v1.1-beta)
- [ ] Parse agent definitions to `AgentBuilder` calls
- [ ] Parse tool definitions to tool registry
- [ ] Validate agent-ref and tool-ref references
- [ ] Support includes/templates

### Phase 3: Supervisor Chain (v1.1-beta)
- [ ] Parse supervisor-chain to `SupervisorChainBuilder` calls
- [ ] Validate tier ordering
- [ ] Support escalation policies

### Phase 4: Job Composition (v1.1-rc)
- [ ] Implement `AgentJobComposer`
- [ ] Wire sources (Kafka, file, memory)
- [ ] Wire sinks
- [ ] Wire storage backends
- [ ] Generate complete Flink job

### Phase 5: Testing & Documentation (v1.1)
- [ ] Comprehensive YAML examples
- [ ] Integration tests
- [ ] User guide
- [ ] Migration guide from Java API

---

## 8. Dependencies

```xml
<!-- YAML Parsing -->
<dependency>
    <groupId>org.yaml</groupId>
    <artifactId>snakeyaml</artifactId>
    <version>2.0</version>
</dependency>

<!-- JSON Schema Validation -->
<dependency>
    <groupId>org.everit.json</groupId>
    <artifactId>org.everit.json.schema</artifactId>
    <version>1.14.1</version>
</dependency>

<!-- Duration Parsing -->
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-lang3</artifactId>
    <version>3.12.0</version>
</dependency>
```

---

## 9. Future Enhancements

### v1.2: Advanced Features
- **YAML Templating**: Jinja2-style templates
- **Variable Substitution**: Environment variables, placeholders
- **Includes**: Split large YAMLs into modules
- **Conditional Logic**: Different configs based on environment

### v2.0: Dynamic Configuration
- **Hot Reload**: Update agents without restarting job
- **A/B Testing**: Route traffic to different agent versions
- **Feature Flags**: Enable/disable agents dynamically

---

## 10. References

### Apache Flink CDC Implementation
- **GitHub**: https://github.com/apache/flink-cdc
- **YAML Parser**: `flink-cdc-pipeline-connector-core/src/main/java/org/apache/flink/cdc/composer/definition/YamlPipelineDefinitionParser.java`
- **Pipeline Composer**: `flink-cdc-composer/src/main/java/org/apache/flink/cdc/composer/PipelineComposer.java`
- **Schema**: `flink-cdc-pipeline-connector-core/src/main/resources/pipeline-schema.json`

### Key Learnings from Flink CDC
1. **Schema-First Design**: JSON Schema catches 90% of errors before runtime
2. **Helpful Error Messages**: Include line numbers, suggestions, examples
3. **Factory Pattern**: Clean separation between YAML parsing and job building
4. **Validation Layers**: Schema → References → Semantics → Runtime
5. **Composability**: Small, reusable YAML modules better than monoliths

---

## 11. Example Usage

### Command-Line Tool
```bash
# Validate YAML without executing
agentic-flink validate research-job.yaml

# Execute job from YAML
agentic-flink run research-job.yaml

# Generate Java code from YAML (for debugging)
agentic-flink codegen research-job.yaml > ResearchJob.java
```

### Programmatic API
```java
// Load and execute YAML job
AgentJobDefinition definition = YamlAgentJobParser.parse("research-job.yaml");
StreamExecutionEnvironment env = AgentJobComposer.compose(definition);
env.execute("Research Agent Job");
```

---

## Conclusion

This design provides a clear path to YAML-based agent configuration, following proven patterns from Apache Flink CDC. Implementation is deferred to v1.1 to focus on core functionality first, but the groundwork (Agent/SupervisorChain builders) is already in place.

**Key Benefits:**
- Non-Java users can define complex agent workflows
- Declarative, readable configuration
- Validated against schema with helpful errors
- Generates optimized Flink jobs automatically

**Next Steps:**
1. Complete Phase 2.4 (Job Generator) to have full programmatic API
2. Implement Phase 1 of YAML support in v1.1
3. Reference this design when building YAML parser
