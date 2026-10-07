package org.agentic.flink.dsl;

import java.time.Duration;
import java.util.*;
import org.agentic.flink.a2a.A2AClientFactory;
import org.agentic.flink.a2a.A2ASkillMapper;
import org.agentic.flink.a2a.RemoteAgentSpec;
import org.agentic.flink.annotation.Public;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.OutputSchema;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.memory.conversation.ConversationStores;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.tools.mcp.McpServerSpec;

/**
 * Fluent builder for creating immutable Agent instances.
 *
 * <p>Every {@code withX} method on this builder is read by the legacy pure-Flink execution graph
 * assembled by {@link org.agentic.flink.job.AgentJobGenerator}:
 *
 * <ul>
 *   <li>{@link org.agentic.flink.job.AgentTurnDispatcher} (CEP dispatcher) reads the state machine
 *       and {@link #withShortTermTtl}.
 *   <li>{@link org.agentic.flink.stream.AgentExecutionFunction} (async execution) reads the chat
 *       connection and setup, listeners, guardrails, required tools, tool defaults, tool timeout,
 *       MCP servers, inference tools, remote agents, compensating tools, the long-term store and
 *       the conversation store.
 *   <li>{@link org.agentic.flink.execution.AgentExecutor} reads the system prompt, allowed tools
 *       and iteration budget.
 * </ul>
 *
 * <p><b>Basic Usage:</b>
 *
 * <pre>{@code
 * Agent agent = Agent.builder()
 *     .withId("my-agent")
 *     .withSystemPrompt("You are a helpful assistant")
 *     .withTools("calculator", "weather")
 *     .build();
 * }</pre>
 *
 * <p><b>Advanced Usage:</b>
 *
 * <pre>{@code
 * Agent agent = Agent.builder()
 *     .withId("research-agent")
 *     .withName("Research Specialist")
 *     .withType(AgentType.RESEARCHER)
 *     .withSystemPrompt("Gather and synthesize research")
 *     .withChatSetup(ChatSetup.builder().withModel("qwen2.5:7b").withTemperature(0.3).build())
 *     .withTools("web-search", "document-analysis", "synthesis")
 *     .withRequiredTools("web-search")  // Job fails at open() if missing
 *     .withToolDefaults("web-search", Map.of("region", "eu"))
 *     .withMaxIterations(10)
 *     .withTimeout(Duration.ofMinutes(5))
 *     .withToolTimeout(Duration.ofSeconds(20))
 *     .withMaxValidationAttempts(3)
 *     .withCompensatingTool("reserve", "release")
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see Agent
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated(since = "1.0.0")
@Public
public class AgentBuilder {

  // Core identity
  String agentId;
  String agentName;
  String description;
  AgentType agentType = AgentType.EXECUTOR;

  // LLM configuration
  String systemPrompt;
  // Type-default temperature, folded into the implicit ChatSetup when none is set explicitly.
  double temperature = 0.7;

  // Tool configuration
  Set<String> allowedTools = new HashSet<>();
  Set<String> requiredTools = new HashSet<>();
  Map<String, Map<String, Object>> toolDefaults = new HashMap<>();

  // Execution configuration
  int maxIterations = 5;
  Duration timeout = Duration.ofSeconds(30);
  Duration toolTimeout = Duration.ofSeconds(10);

  // Validation & correction retry budgets (consumed by the default state machine)
  int maxValidationAttempts = 2;
  int maxCorrectionAttempts = 2;

  // State machine
  AgentStateMachine stateMachine;

  // Saga integration
  boolean compensationEnabled = false;
  Map<String, String> compensatingTools = new LinkedHashMap<>();

  // Memory
  Duration shortTermTtl = Duration.ZERO;
  LongTermMemoryStore longTermStore;
  ConversationStore conversationStore;

  // Chat model (LangChain4J wrapped behind the ChatConnection SPI)
  ChatConnection chatConnection;
  ChatSetup chatSetup;
  OutputSchema<?> outputSchema;

  // Listeners
  List<AgentEventListener> listeners = new ArrayList<>();

  // Skills
  List<Skill> skills = new ArrayList<>();

  // MCP servers
  List<McpServerSpec> mcpServers = new ArrayList<>();

  // A2A remote agents (peers callable as workflow steps / tools)
  List<RemoteAgentSpec> remoteAgents = new ArrayList<>();
  A2AClientFactory a2aClientFactory = A2AClientFactory.discovering();

  // Inference (non-LLM deep learning models exposed as tools) and guardrails
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
   * <p>Type defaults are applied when this method is called, so call it before the options it
   * touches (temperature, retry budgets, iteration budget, timeout) if you want to override them.
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
   * many agents at different {@link ChatSetup}s. If unset, the execution operator falls back to a
   * {@code LangChain4jChatConnection} pointing at local Ollama.
   *
   * @return this builder
   */
  public AgentBuilder withChatConnection(ChatConnection chatConnection) {
    this.chatConnection = chatConnection;
    return this;
  }

  /**
   * Sets the per-agent chat configuration (model name, temperature, max response tokens, structured
   * output, etc.).
   *
   * <p>If unset, an implicit {@link ChatSetup} is built at {@link #build()} time using the default
   * model and the agent-type temperature default.
   *
   * @return this builder
   */
  public AgentBuilder withChatSetup(ChatSetup chatSetup) {
    this.chatSetup = chatSetup;
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
   * Adds required tools. The execution operator validates them against the job's tool registry in
   * {@code open()} and fails the job if any is missing.
   *
   * @param toolNames Required tool names
   * @return this builder
   */
  public AgentBuilder withRequiredTools(String... toolNames) {
    this.requiredTools.addAll(Arrays.asList(toolNames));
    this.allowedTools.addAll(Arrays.asList(toolNames)); // Required tools are also allowed
    return this;
  }

  /**
   * Sets default parameters for a tool. Defaults are merged under the arguments the model supplies
   * on every call of that tool, so the model's values win on conflict.
   *
   * @param toolName The tool name
   * @param defaults Map of default parameter values
   * @return this builder
   */
  public AgentBuilder withToolDefaults(String toolName, Map<String, Object> defaults) {
    if (toolName == null || defaults == null) {
      throw new IllegalArgumentException("toolName and defaults must be non-null");
    }
    this.toolDefaults.put(toolName, new HashMap<>(defaults));
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
   * Sets the total timeout for agent execution (default: 30 seconds). Applied as the async operator
   * timeout and as the CEP pattern window of the default state machine.
   *
   * @param timeout Timeout duration
   * @return this builder
   */
  public AgentBuilder withTimeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Sets the timeout for individual tool calls (default: 10 seconds). A tool call that exceeds it
   * completes exceptionally with a {@link java.util.concurrent.TimeoutException}.
   *
   * @param toolTimeout Tool timeout duration
   * @return this builder
   */
  public AgentBuilder withToolTimeout(Duration toolTimeout) {
    this.toolTimeout = toolTimeout;
    return this;
  }

  // ==================== Validation & Correction ====================

  /**
   * Sets the maximum validation attempts before the default state machine moves VALIDATING to
   * FAILED instead of CORRECTING (default: 2).
   *
   * @param maxAttempts Maximum validation attempts
   * @return this builder
   */
  public AgentBuilder withMaxValidationAttempts(int maxAttempts) {
    this.maxValidationAttempts = maxAttempts;
    return this;
  }

  /**
   * Sets the maximum correction attempts before the default state machine moves SUPERVISOR_REVIEW
   * to FAILED instead of CORRECTING (default: 2).
   *
   * @param maxAttempts Maximum correction attempts
   * @return this builder
   */
  public AgentBuilder withMaxCorrectionAttempts(int maxAttempts) {
    this.maxCorrectionAttempts = maxAttempts;
    return this;
  }

  // ==================== State Machine ====================

  /**
   * Sets a custom state machine for this agent. When set, the retry budgets, timeout and
   * compensation flag are not applied to it; they only shape the default state machine.
   *
   * @param stateMachine The state machine
   * @return this builder
   */
  public AgentBuilder withStateMachine(AgentStateMachine stateMachine) {
    this.stateMachine = stateMachine;
    return this;
  }

  // ==================== Saga Integration ====================

  /**
   * Enables compensation/rollback (default: false). When enabled, a failed turn emits a
   * compensation request for every completed tool call that has a compensating tool registered via
   * {@link #withCompensatingTool}, and the CEP dispatcher emits one on pattern timeout.
   *
   * @param enabled true to enable compensation
   * @return this builder
   */
  public AgentBuilder withCompensationEnabled(boolean enabled) {
    this.compensationEnabled = enabled;
    return this;
  }

  /**
   * Registers the tool that undoes a completed call of {@code toolName}. Implies {@link
   * #withCompensationEnabled}. The compensating tool receives the original tool name, arguments and
   * result as parameters and is executed by {@link org.agentic.flink.stream.CompensationFunction}
   * in reverse call order.
   *
   * @param toolName Tool whose effects need rollback
   * @param compensatingToolName Tool that performs the rollback
   * @return this builder
   */
  public AgentBuilder withCompensatingTool(String toolName, String compensatingToolName) {
    if (toolName == null || compensatingToolName == null) {
      throw new IllegalArgumentException("toolName and compensatingToolName must be non-null");
    }
    this.compensatingTools.put(toolName, compensatingToolName);
    this.compensationEnabled = true;
    return this;
  }

  // ==================== Memory ====================

  /**
   * Sets the TTL applied to the per-turn keyed state of the CEP dispatcher. {@link Duration#ZERO}
   * (default) keeps the dispatcher's built-in TTL.
   */
  public AgentBuilder withShortTermTtl(Duration ttl) {
    this.shortTermTtl = ttl == null ? Duration.ZERO : ttl;
    return this;
  }

  /**
   * Configure the long-term memory store. When set, the execution operator archives every terminal
   * turn outcome (completed or failed) as a fact keyed by flow id. The store must be initialized
   * before the job is submitted ({@code StorageFactory} does this).
   */
  public AgentBuilder withLongTermStore(LongTermMemoryStore store) {
    this.longTermStore = store;
    return this;
  }

  /**
   * Configure the per-conversation memory store: the multi-turn transcript shared across operators,
   * keyed by conversation id. The execution operator appends the user message and the assistant
   * answer of every completed turn under the flow id.
   *
   * <p>When not set, the framework discovers one via {@link ConversationStores#discover()}: the
   * process-wide in-JVM store for the embedded deployment, or a ServiceLoader-registered
   * Redis/Postgres-backed store for a distributed cluster.
   */
  public AgentBuilder withConversationStore(ConversationStore store) {
    this.conversationStore = store;
    return this;
  }

  /**
   * Register one or more lifecycle listeners. Fanned out via {@code CompositeListener} to the LLM
   * client (chat and guardrail hooks) and the execution operator (start, error, long-term sync).
   */
  public AgentBuilder withListener(AgentEventListener... ls) {
    if (ls != null) {
      this.listeners.addAll(Arrays.asList(ls));
    }
    return this;
  }

  /**
   * Add one or more {@link Skill}s. Tools declared by the skill are added to the agent's allowed
   * tool list; the skill's prompt fragment is concatenated onto the system prompt at {@link
   * #build()} time.
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
   * Register one or more MCP servers. The execution operator discovers tools from each server in
   * {@code open()} and registers them in the agent's tool registry.
   */
  public AgentBuilder withMcpServer(McpServerSpec... servers) {
    if (servers != null) {
      this.mcpServers.addAll(Arrays.asList(servers));
    }
    return this;
  }

  /**
   * Register one or more remote A2A agents ("peers") this agent can call as workflow steps.
   *
   * <p>Each peer is exposed to the LLM as a synthetic tool named {@code a2a:<name>} (added to the
   * allowed-tools list) and described via a generated {@link Skill} so the model knows when to
   * delegate. The concrete {@link org.agentic.flink.a2a.A2AToolExecutor} is registered into the
   * execution operator's tool registry in {@code open()} via {@code
   * A2AToolRegistry.registerInto(...)}. For deterministic (non-LLM) sequencing, use {@code
   * AgentJobBuilder.withA2AStep(...)} instead.
   */
  public AgentBuilder withRemoteAgent(RemoteAgentSpec... specs) {
    if (specs != null) {
      for (RemoteAgentSpec spec : specs) {
        this.remoteAgents.add(spec);
        this.allowedTools.add(spec.toolId());
        Skill skill = A2ASkillMapper.fromSpec(spec);
        this.skills.add(skill);
      }
    }
    return this;
  }

  /**
   * Override the {@link A2AClientFactory} used to build clients for this agent's remote peers.
   * Defaults to {@link A2AClientFactory#discovering()} (resolves the SDK adapter via
   * ServiceLoader). Tests inject an in-memory factory here.
   */
  public AgentBuilder withA2AClientFactory(A2AClientFactory factory) {
    if (factory != null) {
      this.a2aClientFactory = factory;
    }
    return this;
  }

  // ==================== Inference ====================

  /**
   * Register an inference model as a tool, callable via the LLM tool-call path. The adapter is
   * added to the allowed tools and registered in the execution operator's tool registry.
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
   * Add one or more guardrails. They run before and after every LLM call inside {@link
   * org.agentic.flink.execution.LLMClient#chat}.
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

    // Default per-conversation memory: ServiceLoader-discovered, else the shared in-JVM store.
    if (conversationStore == null) {
      conversationStore = ConversationStores.discover();
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

    // Materialize an implicit ChatSetup (default model + agent-type temperature) if none was set.
    if (chatSetup == null) {
      ChatSetup.Builder b =
          ChatSetup.builder()
              .withModel(ConfigKeys.DEFAULT_OLLAMA_MODEL)
              .withTemperature(temperature);
      if (outputSchema != null) {
        b.withOutputSchema(outputSchema);
      }
      chatSetup = b.build();
    } else if (outputSchema != null && !chatSetup.hasOutputSchema()) {
      chatSetup = chatSetup.toBuilder().withOutputSchema(outputSchema).build();
    }

    // Create default state machine if not provided. The budgets must be set before
    // withStandardTransitions() because that call snapshots them into the transitions.
    if (stateMachine == null) {
      stateMachine =
          AgentStateMachine.builder()
              .withId(agentId + "-state-machine")
              .withMaxValidationAttempts(maxValidationAttempts)
              .withMaxCorrectionAttempts(maxCorrectionAttempts)
              .withCompensationEnabled(compensationEnabled)
              .withGlobalTimeout((int) timeout.getSeconds())
              .withStandardTransitions()
              .build();
    }
  }

  /** Applies type-specific defaults based on AgentType. */
  private void applyTypeDefaults(AgentType type) {
    switch (type) {
      case VALIDATOR:
        this.maxValidationAttempts = 3;
        this.temperature = 0.1; // More deterministic for validation
        break;

      case CORRECTOR:
        this.maxCorrectionAttempts = 3;
        this.temperature = 0.5; // Moderate creativity for corrections
        break;

      case SUPERVISOR:
        this.temperature = 0.3; // Careful review
        break;

      case COORDINATOR:
        this.maxIterations = 20; // Coordinators may need more iterations
        this.timeout = Duration.ofMinutes(10); // Longer timeout
        break;

      case RESEARCHER:
        this.maxIterations = 15; // Research may need multiple passes
        this.timeout = Duration.ofMinutes(5);
        this.temperature = 0.4; // Balanced for research
        break;

      case EXECUTOR:
      case CUSTOM:
      default:
        // Use defaults
        break;
    }
  }
}
