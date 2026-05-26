package org.agentic.flink.dsl;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.completion.TaskList;
import org.agentic.flink.context.manager.ContextWindowManager;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.OutputSchema;
import org.agentic.flink.memory.FlinkStateShortTermMemory;
import org.agentic.flink.memory.ShortTermMemorySpec;
import org.agentic.flink.channel.Channel;
import org.agentic.flink.channel.KeyedContextItem;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.skill.SkillRegistry;
import org.agentic.flink.tools.mcp.McpServerSpec;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.storage.LongTermMemoryStore;
import java.time.Duration;
import java.util.*;

/**
 * Fluent builder for creating immutable Agent instances.
 *
 * <p>This builder provides a declarative API for defining agents, inspired by the builder pattern
 * used throughout the codebase. It allows you to specify all agent configuration in a readable,
 * type-safe manner.
 *
 * <p><b>Basic Usage:</b>
 * <pre>{@code
 * Agent agent = Agent.builder()
 *     .withId("my-agent")
 *     .withSystemPrompt("You are a helpful assistant")
 *     .withTools("calculator", "weather")
 *     .build();
 * }</pre>
 *
 * <p><b>Advanced Usage:</b>
 * <pre>{@code
 * Agent agent = Agent.builder()
 *     .withId("research-agent")
 *     .withName("Research Specialist")
 *     .withType(AgentType.RESEARCHER)
 *     .withSystemPrompt("Gather and synthesize research")
 *     .withLlmModel("qwen2.5:7b")
 *     .withTemperature(0.3)
 *     .withTools("web-search", "document-analysis", "synthesis")
 *     .withRequiredTools("web-search")  // Must be available
 *     .withMaxIterations(10)
 *     .withTimeout(Duration.ofMinutes(5))
 *     .withValidationEnabled(true)
 *     .withMaxValidationAttempts(3)
 *     .withCorrectionEnabled(true)
 *     .withSupervisor("quality-supervisor")
 *     .withSupervisorThreshold(0.8)  // Review if score < 0.8
 *     .withTaskList(researchTasks)
 *     .withCompensationEnabled(true)
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see Agent
 */
public class AgentBuilder {

  // Core identity
  String agentId;
  String agentName;
  String description;
  AgentType agentType = AgentType.EXECUTOR;

  // LLM configuration
  String systemPrompt;
  String llmModel = ConfigKeys.DEFAULT_OLLAMA_MODEL;  // Default model
  double temperature = 0.7;
  int maxTokens = 4000;
  int maxResponseTokens = 1000;

  // Tool configuration
  Set<String> allowedTools = new HashSet<>();
  Set<String> requiredTools = new HashSet<>();
  Map<String, Object> toolDefaults = new HashMap<>();

  // Execution configuration
  int maxIterations = 5;
  Duration timeout = Duration.ofSeconds(30);
  Duration toolTimeout = Duration.ofSeconds(10);
  int maxRetries = 3;

  // Validation & correction
  boolean validationEnabled = false;
  int maxValidationAttempts = 2;
  boolean correctionEnabled = false;
  int maxCorrectionAttempts = 2;
  String validationPrompt;
  String correctionPrompt;

  // Supervision
  String supervisorId;
  boolean supervisorReviewRequired = false;
  double supervisorThreshold = 0.7;

  // Context management
  ContextWindowManager.ContextWindowConfig contextConfig;
  boolean contextCompressionEnabled = true;

  // State machine
  AgentStateMachine stateMachine;

  // Completion tracking
  TaskList taskList;
  boolean autoDetectTaskCompletion = true;

  // Saga integration
  boolean compensationEnabled = false;
  Map<String, Object> compensationConfig = new HashMap<>();

  // Memory (Flink-state-first)
  Duration shortTermTtl = Duration.ZERO;
  ShortTermMemorySpec shortTermMemorySpec;
  LongTermMemoryStore longTermStore;
  List<Channel<KeyedContextItem>> memoryChannels = new ArrayList<>();
  VectorMemorySpec vectorMemorySpec;

  // Chat model (LangChain4J wrapped behind the ChatConnection SPI)
  ChatConnection chatConnection;
  ChatSetup chatSetup;
  OutputSchema<?> outputSchema;

  // Embeddings
  EmbeddingConnection embeddingConnection;
  EmbeddingSetup embeddingSetup;

  // Listeners
  List<AgentEventListener> listeners = new ArrayList<>();

  // Skills
  List<Skill> skills = new ArrayList<>();

  // MCP servers
  List<McpServerSpec> mcpServers = new ArrayList<>();

  // Inference (non-LLM deep learning models)
  Map<String, InferenceConnection> inferenceConnections = new LinkedHashMap<>();
  List<InferenceToolAdapter> inferenceTools = new ArrayList<>();
  List<Guardrail> guardrails = new ArrayList<>();

  // Package-private constructor
  AgentBuilder() {}

  // ==================== Core Identity ====================

  /**
   * Sets the unique identifier for this agent (required).
   *
   * @param agentId The agent ID
   * @return this builder
   */
  public AgentBuilder withId(String agentId) {
    this.agentId = agentId;
    return this;
  }

  /**
   * Sets the display name for this agent.
   *
   * @param agentName The agent name
   * @return this builder
   */
  public AgentBuilder withName(String agentName) {
    this.agentName = agentName;
    return this;
  }

  /**
   * Sets the description for this agent.
   *
   * @param description The agent description
   * @return this builder
   */
  public AgentBuilder withDescription(String description) {
    this.description = description;
    return this;
  }

  /**
   * Sets the agent type (pre-configured behavior template).
   *
   * @param agentType The agent type
   * @return this builder
   */
  public AgentBuilder withType(AgentType agentType) {
    this.agentType = agentType;
    applyTypeDefaults(agentType);
    return this;
  }

  // ==================== LLM Configuration ====================

  /**
   * Sets the system prompt for this agent (required).
   *
   * @param systemPrompt The system prompt
   * @return this builder
   */
  public AgentBuilder withSystemPrompt(String systemPrompt) {
    this.systemPrompt = systemPrompt;
    return this;
  }

  /**
   * Sets the chat-model transport (provider, base URL, credentials).
   *
   * <p>This is the preferred LLM configuration entry-point. One {@link ChatConnection} can serve
   * many agents at different {@link ChatSetup}s. If unset, a default {@code
   * LangChain4jChatConnection} is discovered via {@link java.util.ServiceLoader} pointing at
   * local Ollama.
   *
   * @return this builder
   */
  public AgentBuilder withChatConnection(ChatConnection chatConnection) {
    this.chatConnection = chatConnection;
    return this;
  }

  /**
   * Sets the per-agent chat configuration (model name, temperature, max response tokens,
   * structured output, etc.).
   *
   * <p>If unset, an implicit {@link ChatSetup} is built from {@link #withLlmModel(String)},
   * {@link #withTemperature(double)} and {@link #withMaxResponseTokens(int)} for backwards
   * compatibility.
   *
   * @return this builder
   */
  public AgentBuilder withChatSetup(ChatSetup chatSetup) {
    this.chatSetup = chatSetup;
    if (chatSetup != null) {
      this.llmModel = chatSetup.getModelName();
      this.temperature = chatSetup.getTemperature();
      this.maxResponseTokens = chatSetup.getMaxResponseTokens();
    }
    return this;
  }

  /**
   * Configure the structured output contract for this agent.
   *
   * <p>Equivalent to {@code withChatSetup(chatSetup.toBuilder().withOutputSchema(schema).build())}
   * for the common case where everything else stays on the default.
   *
   * @return this builder
   */
  public AgentBuilder withOutputSchema(OutputSchema<?> outputSchema) {
    this.outputSchema = outputSchema;
    return this;
  }

  /**
   * Sets the LLM model to use (default: qwen2.5:3b).
   *
   * @param llmModel The model name (e.g., "qwen2.5:7b", "gpt-4o-mini")
   * @return this builder
   * @deprecated Prefer {@link #withChatSetup(ChatSetup)}; this method only sets the model name on
   *     an implicit setup constructed at {@link #build()} time.
   */
  @Deprecated
  public AgentBuilder withLlmModel(String llmModel) {
    this.llmModel = llmModel;
    return this;
  }

  /**
   * Sets the temperature for LLM generation (0.0 = deterministic, 1.0 = creative).
   *
   * @param temperature The temperature (default: 0.7)
   * @return this builder
   * @deprecated Prefer {@link #withChatSetup(ChatSetup)}.
   */
  @Deprecated
  public AgentBuilder withTemperature(double temperature) {
    this.temperature = temperature;
    return this;
  }

  /**
   * Sets the maximum total tokens (context + response).
   *
   * @param maxTokens Maximum tokens (default: 4000)
   * @return this builder
   */
  public AgentBuilder withMaxTokens(int maxTokens) {
    this.maxTokens = maxTokens;
    return this;
  }

  /**
   * Sets the maximum response tokens (LLM output).
   *
   * @param maxResponseTokens Maximum response tokens (default: 1000)
   * @return this builder
   * @deprecated Prefer {@link #withChatSetup(ChatSetup)}.
   */
  @Deprecated
  public AgentBuilder withMaxResponseTokens(int maxResponseTokens) {
    this.maxResponseTokens = maxResponseTokens;
    return this;
  }

  // ==================== Tool Configuration ====================

  /**
   * Adds allowed tools for this agent.
   *
   * @param toolNames Tool names to allow
   * @return this builder
   */
  public AgentBuilder withTools(String... toolNames) {
    this.allowedTools.addAll(Arrays.asList(toolNames));
    return this;
  }

  /**
   * Adds required tools (must be available for agent to function).
   *
   * @param toolNames Required tool names
   * @return this builder
   */
  public AgentBuilder withRequiredTools(String... toolNames) {
    this.requiredTools.addAll(Arrays.asList(toolNames));
    this.allowedTools.addAll(Arrays.asList(toolNames));  // Required tools are also allowed
    return this;
  }

  /**
   * Sets default parameters for a tool.
   *
   * @param toolName The tool name
   * @param defaults Map of default parameter values
   * @return this builder
   */
  public AgentBuilder withToolDefaults(String toolName, Map<String, Object> defaults) {
    this.toolDefaults.put(toolName, defaults);
    return this;
  }

  // ==================== Execution Configuration ====================

  /**
   * Sets the maximum number of iterations/loops (default: 5).
   *
   * @param maxIterations Maximum iterations
   * @return this builder
   */
  public AgentBuilder withMaxIterations(int maxIterations) {
    this.maxIterations = maxIterations;
    return this;
  }

  /**
   * Sets the total timeout for agent execution (default: 30 seconds).
   *
   * @param timeout Timeout duration
   * @return this builder
   */
  public AgentBuilder withTimeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Sets the timeout for individual tool calls (default: 10 seconds).
   *
   * @param toolTimeout Tool timeout duration
   * @return this builder
   */
  public AgentBuilder withToolTimeout(Duration toolTimeout) {
    this.toolTimeout = toolTimeout;
    return this;
  }

  /**
   * Sets the maximum number of retries for failed operations (default: 3).
   *
   * @param maxRetries Maximum retries
   * @return this builder
   */
  public AgentBuilder withMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  // ==================== Validation & Correction ====================

  /**
   * Enables validation of agent outputs (default: false).
   *
   * @param enabled true to enable validation
   * @return this builder
   */
  public AgentBuilder withValidationEnabled(boolean enabled) {
    this.validationEnabled = enabled;
    return this;
  }

  /**
   * Sets the maximum validation attempts (default: 2).
   *
   * @param maxAttempts Maximum validation attempts
   * @return this builder
   */
  public AgentBuilder withMaxValidationAttempts(int maxAttempts) {
    this.maxValidationAttempts = maxAttempts;
    return this;
  }

  /**
   * Enables correction of failed validations (default: false).
   *
   * @param enabled true to enable correction
   * @return this builder
   */
  public AgentBuilder withCorrectionEnabled(boolean enabled) {
    this.correctionEnabled = enabled;
    return this;
  }

  /**
   * Sets the maximum correction attempts (default: 2).
   *
   * @param maxAttempts Maximum correction attempts
   * @return this builder
   */
  public AgentBuilder withMaxCorrectionAttempts(int maxAttempts) {
    this.maxCorrectionAttempts = maxAttempts;
    return this;
  }

  /**
   * Sets a custom validation prompt.
   *
   * @param validationPrompt The validation prompt template
   * @return this builder
   */
  public AgentBuilder withValidationPrompt(String validationPrompt) {
    this.validationPrompt = validationPrompt;
    return this;
  }

  /**
   * Sets a custom correction prompt.
   *
   * @param correctionPrompt The correction prompt template
   * @return this builder
   */
  public AgentBuilder withCorrectionPrompt(String correctionPrompt) {
    this.correctionPrompt = correctionPrompt;
    return this;
  }

  // ==================== Supervision ====================

  /**
   * Sets the supervisor agent ID (enables supervision).
   *
   * @param supervisorId The supervisor agent ID
   * @return this builder
   */
  public AgentBuilder withSupervisor(String supervisorId) {
    this.supervisorId = supervisorId;
    this.supervisorReviewRequired = true;
    return this;
  }

  /**
   * Sets whether supervisor review is required (default: false, true if supervisor set).
   *
   * @param required true if supervisor review required
   * @return this builder
   */
  public AgentBuilder withSupervisorReviewRequired(boolean required) {
    this.supervisorReviewRequired = required;
    return this;
  }

  /**
   * Sets the quality threshold for supervisor escalation (default: 0.7).
   *
   * <p>If agent output quality score < threshold, escalate to supervisor.
   *
   * @param threshold Quality threshold (0.0 to 1.0)
   * @return this builder
   */
  public AgentBuilder withSupervisorThreshold(double threshold) {
    this.supervisorThreshold = threshold;
    return this;
  }

  // ==================== Context Management ====================

  /**
   * Sets the context window configuration.
   *
   * @param contextConfig Context window config
   * @return this builder
   */
  public AgentBuilder withContextConfig(ContextWindowManager.ContextWindowConfig contextConfig) {
    this.contextConfig = contextConfig;
    return this;
  }

  /**
   * Enables context compression (default: true).
   *
   * @param enabled true to enable compression
   * @return this builder
   */
  public AgentBuilder withContextCompressionEnabled(boolean enabled) {
    this.contextCompressionEnabled = enabled;
    return this;
  }

  // ==================== State Machine ====================

  /**
   * Sets a custom state machine for this agent.
   *
   * @param stateMachine The state machine
   * @return this builder
   */
  public AgentBuilder withStateMachine(AgentStateMachine stateMachine) {
    this.stateMachine = stateMachine;
    return this;
  }

  // ==================== Completion Tracking ====================

  /**
   * Sets the task list for completion tracking.
   *
   * @param taskList The task list
   * @return this builder
   */
  public AgentBuilder withTaskList(TaskList taskList) {
    this.taskList = taskList;
    return this;
  }

  /**
   * Enables auto-detection of task completion from events (default: true).
   *
   * @param enabled true to enable auto-detection
   * @return this builder
   */
  public AgentBuilder withAutoDetectTaskCompletion(boolean enabled) {
    this.autoDetectTaskCompletion = enabled;
    return this;
  }

  // ==================== Saga Integration ====================

  /**
   * Enables compensation/rollback (default: false).
   *
   * @param enabled true to enable compensation
   * @return this builder
   */
  public AgentBuilder withCompensationEnabled(boolean enabled) {
    this.compensationEnabled = enabled;
    return this;
  }

  /**
   * Sets compensation configuration.
   *
   * @param key Config key
   * @param value Config value
   * @return this builder
   */
  public AgentBuilder withCompensationConfig(String key, Object value) {
    this.compensationConfig.put(key, value);
    return this;
  }

  // ==================== Memory ====================

  /**
   * Sets the TTL applied to short-term Flink keyed state. {@link Duration#ZERO} (default)
   * disables TTL — entries live until the key is cleared or the checkpoint is dropped.
   */
  public AgentBuilder withShortTermTtl(Duration ttl) {
    this.shortTermTtl = ttl == null ? Duration.ZERO : ttl;
    return this;
  }

  /**
   * Override the default short-term memory implementation. By default, {@link
   * FlinkStateShortTermMemory} is used with the configured TTL.
   */
  public AgentBuilder withShortTermMemory(ShortTermMemorySpec spec) {
    this.shortTermMemorySpec = spec;
    return this;
  }

  /**
   * Configure the long-term memory store. Optional — when not set, conversation resumption is
   * disabled and short-term state lives only inside Flink checkpoints.
   */
  public AgentBuilder withLongTermStore(LongTermMemoryStore store) {
    this.longTermStore = store;
    return this;
  }

  /**
   * Register one or more memory channels that emit {@link KeyedContextItem}s into the agent's
   * memory layer. Replaces the older {@code withFeed(MemoryFeed...)}; pass
   * {@code KafkaContextChannel}, {@code PostgresChangeChannel}, {@code RedisPubSubChannel}, or
   * any custom {@code Channel<KeyedContextItem>}.
   */
  @SafeVarargs
  public final AgentBuilder withMemoryChannel(Channel<KeyedContextItem>... channels) {
    if (channels != null) {
      this.memoryChannels.addAll(Arrays.asList(channels));
    }
    return this;
  }

  /** Enable in-JVM vector memory backed by Flink state. */
  public AgentBuilder withVectorMemory(VectorMemorySpec spec) {
    this.vectorMemorySpec = spec;
    return this;
  }

  /** Sets the embedding transport. ServiceLoader-discovered Ollama is used by default. */
  public AgentBuilder withEmbeddingConnection(EmbeddingConnection connection) {
    this.embeddingConnection = connection;
    return this;
  }

  /** Sets the per-agent embedding config (model name, dimension, normalize). */
  public AgentBuilder withEmbeddingSetup(EmbeddingSetup setup) {
    this.embeddingSetup = setup;
    return this;
  }

  /** Register one or more lifecycle listeners. Fanned out via {@code CompositeListener}. */
  public AgentBuilder withListener(AgentEventListener... ls) {
    if (ls != null) {
      this.listeners.addAll(Arrays.asList(ls));
    }
    return this;
  }

  /**
   * Add one or more {@link Skill}s. Tools declared by the skill are added to the agent's
   * allowed tool list; the skill's prompt fragment is concatenated onto the system prompt at
   * {@link #build()} time.
   */
  public AgentBuilder withSkill(Skill... newSkills) {
    if (newSkills != null) {
      for (Skill s : newSkills) {
        this.skills.add(s);
        this.allowedTools.addAll(s.getTools());
      }
    }
    return this;
  }

  /**
   * Register one or more MCP servers. The framework discovers tools from each server at job
   * startup and registers them in the agent's tool registry.
   */
  public AgentBuilder withMcpServer(McpServerSpec... servers) {
    if (servers != null) {
      this.mcpServers.addAll(Arrays.asList(servers));
    }
    return this;
  }

  // ==================== Inference ====================

  /**
   * Register an {@link InferenceConnection} (classifier, scorer, embedder, generic) under a
   * logical name. Multiple may be registered. Retrieve at runtime via
   * {@link Agent#getInferenceConnection(String)}.
   */
  public AgentBuilder withInferenceConnection(String name, InferenceConnection connection) {
    if (name == null || connection == null) {
      throw new IllegalArgumentException("name and connection must be non-null");
    }
    this.inferenceConnections.put(name, connection);
    return this;
  }

  /**
   * Register an inference model as a tool, callable via the LLM tool-call path. The adapter is
   * also added to {@link #allowedTools} so the LLM sees it in the available-tools list.
   */
  public AgentBuilder withInferenceTool(InferenceToolAdapter adapter) {
    if (adapter == null) {
      throw new IllegalArgumentException("adapter must be non-null");
    }
    this.inferenceTools.add(adapter);
    this.allowedTools.add(adapter.getToolId());
    return this;
  }

  /**
   * Add one or more guardrails. They run before and after every LLM call inside
   * {@link org.agentic.flink.execution.LLMClient#chat}.
   */
  public AgentBuilder withGuardrail(Guardrail... gs) {
    if (gs != null) {
      this.guardrails.addAll(Arrays.asList(gs));
    }
    return this;
  }

  // ==================== Build ====================

  /**
   * Builds the immutable Agent instance.
   *
   * @return new Agent
   * @throws IllegalStateException if required fields are missing
   */
  public Agent build() {
    validate();
    applyDefaults();
    return new Agent(this);
  }

  // ==================== Private Methods ====================

  private void validate() {
    if (agentId == null || agentId.isEmpty()) {
      throw new IllegalStateException("Agent ID is required");
    }
    if (systemPrompt == null || systemPrompt.isEmpty()) {
      throw new IllegalStateException("System prompt is required");
    }
  }

  private void applyDefaults() {
    // Apply name default
    if (agentName == null || agentName.isEmpty()) {
      agentName = agentType.getDisplayName() + " Agent";
    }

    // Apply description default
    if (description == null || description.isEmpty()) {
      description = agentType.getDescription();
    }

    // Create default context config if not provided
    if (contextConfig == null) {
      contextConfig = new ContextWindowManager.ContextWindowConfig(
          maxTokens,
          50,  // maxItems
          0.8   // compactionThreshold
      );
    }

    // Default short-term memory: Flink keyed state with the configured TTL.
    if (shortTermMemorySpec == null) {
      shortTermMemorySpec = FlinkStateShortTermMemory.spec(shortTermTtl);
    }

    // Concatenate skill prompt fragments onto the system prompt.
    if (!skills.isEmpty()) {
      StringBuilder sb = new StringBuilder(systemPrompt == null ? "" : systemPrompt);
      for (Skill s : skills) {
        String frag = s.getSystemPromptFragment();
        if (frag != null && !frag.isEmpty()) {
          if (sb.length() > 0) sb.append("\n\n");
          sb.append("# Skill: ").append(s.getName()).append('\n').append(frag);
        }
      }
      systemPrompt = sb.toString();
    }

    // Materialize implicit ChatSetup from the (deprecated) string-based fields if needed.
    if (chatSetup == null) {
      ChatSetup.Builder b =
          ChatSetup.builder()
              .withModel(llmModel)
              .withTemperature(temperature)
              .withMaxResponseTokens(maxResponseTokens);
      if (outputSchema != null) {
        b.withOutputSchema(outputSchema);
      }
      chatSetup = b.build();
    } else if (outputSchema != null && !chatSetup.hasOutputSchema()) {
      chatSetup = chatSetup.toBuilder().withOutputSchema(outputSchema).build();
    }

    // Create default state machine if not provided
    if (stateMachine == null) {
      stateMachine = AgentStateMachine.builder()
          .withId(agentId + "-state-machine")
          .withStandardTransitions()
          .withMaxValidationAttempts(maxValidationAttempts)
          .withMaxCorrectionAttempts(maxCorrectionAttempts)
          .withCompensationEnabled(compensationEnabled)
          .withGlobalTimeout((int) timeout.getSeconds())
          .build();
    }
  }

  /**
   * Applies type-specific defaults based on AgentType.
   */
  private void applyTypeDefaults(AgentType type) {
    switch (type) {
      case VALIDATOR:
        this.validationEnabled = true;
        this.maxValidationAttempts = 3;
        this.temperature = 0.1;  // More deterministic for validation
        break;

      case CORRECTOR:
        this.correctionEnabled = true;
        this.maxCorrectionAttempts = 3;
        this.temperature = 0.5;  // Moderate creativity for corrections
        break;

      case SUPERVISOR:
        this.supervisorReviewRequired = false;  // Supervisors don't have supervisors
        this.validationEnabled = true;
        this.temperature = 0.3;  // Careful review
        break;

      case COORDINATOR:
        this.maxIterations = 20;  // Coordinators may need more iterations
        this.timeout = Duration.ofMinutes(10);  // Longer timeout
        break;

      case RESEARCHER:
        this.maxIterations = 15;  // Research may need multiple passes
        this.timeout = Duration.ofMinutes(5);
        this.temperature = 0.4;  // Balanced for research
        break;

      case EXECUTOR:
      case CUSTOM:
      default:
        // Use defaults
        break;
    }
  }
}
