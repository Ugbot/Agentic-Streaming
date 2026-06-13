package org.agentic.flink.dsl;

import org.agentic.flink.completion.TaskList;
import org.agentic.flink.context.manager.ContextWindowManager;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.channel.Channel;
import org.agentic.flink.channel.KeyedContextItem;
import org.agentic.flink.memory.ShortTermMemorySpec;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.skill.SkillRegistry;
import org.agentic.flink.tools.mcp.McpServerSpec;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.storage.LongTermMemoryStore;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;

/**
 * Immutable agent definition created via the declarative builder API.
 *
 * <p>An Agent represents a complete specification of an AI agent's behavior, including:
 * <ul>
 *   <li>LLM configuration (model, prompt, temperature)</li>
 *   <li>Tool access (which tools this agent can call)</li>
 *   <li>Validation & correction settings</li>
 *   <li>Supervisor chain integration</li>
 *   <li>Context management configuration</li>
 *   <li>State machine behavior</li>
 *   <li>Completion tracking (task lists)</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * Agent researchAgent = Agent.builder()
 *     .withId("research-agent")
 *     .withName("Research Specialist")
 *     .withSystemPrompt("You are a research specialist. Gather and synthesize information.")
 *     .withTools("web-search", "document-analysis", "synthesis")
 *     .withLlmModel("qwen2.5:7b")
 *     .withTemperature(0.3)
 *     .withMaxIterations(10)
 *     .withTimeout(Duration.ofMinutes(5))
 *     .withValidationEnabled(true)
 *     .withSupervisor("quality-supervisor")
 *     .withMaxTokens(8000)
 *     .build();
 * }</pre>
 *
 * <p>The Agent is immutable after creation and can be safely shared across Flink task managers.
 *
 * @author Agentic Flink Team
 * @see AgentBuilder
 */
public class Agent implements Serializable {

  private static final long serialVersionUID = 1L;

  // ==================== Core Identity ====================

  private final String agentId;
  private final String agentName;
  private final String description;
  private final AgentType agentType;

  // ==================== LLM Configuration ====================

  private final String systemPrompt;
  private final int maxTokens;

  // ==================== Tool Configuration ====================

  private final Set<String> allowedTools;
  private final Set<String> requiredTools;
  private final Map<String, Object> toolDefaults;

  // ==================== Execution Configuration ====================

  private final int maxIterations;
  private final Duration timeout;
  private final Duration toolTimeout;
  private final int maxRetries;

  // ==================== Validation & Correction ====================

  private final boolean validationEnabled;
  private final int maxValidationAttempts;
  private final boolean correctionEnabled;
  private final int maxCorrectionAttempts;
  private final String validationPrompt;
  private final String correctionPrompt;

  // ==================== Supervision ====================

  private final String supervisorId;
  private final boolean supervisorReviewRequired;
  private final double supervisorThreshold;

  // ==================== Context Management ====================

  private final ContextWindowManager.ContextWindowConfig contextConfig;
  private final boolean contextCompressionEnabled;

  // ==================== State Machine ====================

  private final AgentStateMachine stateMachine;

  // ==================== Completion Tracking ====================

  private final TaskList taskList;
  private final boolean autoDetectTaskCompletion;

  // ==================== Saga Integration ====================

  private final boolean compensationEnabled;
  private final Map<String, Object> compensationConfig;

  // ==================== Memory (Flink-state-first) ====================

  private final Duration shortTermTtl;
  private final ShortTermMemorySpec shortTermMemorySpec;
  private final LongTermMemoryStore longTermStore;
  private final List<Channel<KeyedContextItem>> memoryChannels;
  private final VectorMemorySpec vectorMemorySpec;

  // Chat
  private final ChatConnection chatConnection;
  private final ChatSetup chatSetup;

  // Embeddings
  private final EmbeddingConnection embeddingConnection;
  private final EmbeddingSetup embeddingSetup;

  // Listeners
  private final List<AgentEventListener> listeners;

  // Skills + MCP
  private final SkillRegistry skillRegistry;
  private final List<McpServerSpec> mcpServers;

  // A2A remote agents (peers)
  private final List<org.agentic.flink.a2a.RemoteAgentSpec> remoteAgents;
  private final org.agentic.flink.a2a.A2AClientFactory a2aClientFactory;

  // Inference
  private final Map<String, InferenceConnection> inferenceConnections;
  private final List<InferenceToolAdapter> inferenceTools;
  private final List<Guardrail> guardrails;

  // Package-private constructor - use builder
  Agent(AgentBuilder builder) {
    // Core identity
    this.agentId = builder.agentId;
    this.agentName = builder.agentName;
    this.description = builder.description;
    this.agentType = builder.agentType;

    // LLM configuration
    this.systemPrompt = builder.systemPrompt;
    this.maxTokens = builder.maxTokens;

    // Tool configuration
    this.allowedTools = Collections.unmodifiableSet(new HashSet<>(builder.allowedTools));
    this.requiredTools = Collections.unmodifiableSet(new HashSet<>(builder.requiredTools));
    this.toolDefaults = Collections.unmodifiableMap(new HashMap<>(builder.toolDefaults));

    // Execution configuration
    this.maxIterations = builder.maxIterations;
    this.timeout = builder.timeout;
    this.toolTimeout = builder.toolTimeout;
    this.maxRetries = builder.maxRetries;

    // Validation & correction
    this.validationEnabled = builder.validationEnabled;
    this.maxValidationAttempts = builder.maxValidationAttempts;
    this.correctionEnabled = builder.correctionEnabled;
    this.maxCorrectionAttempts = builder.maxCorrectionAttempts;
    this.validationPrompt = builder.validationPrompt;
    this.correctionPrompt = builder.correctionPrompt;

    // Supervision
    this.supervisorId = builder.supervisorId;
    this.supervisorReviewRequired = builder.supervisorReviewRequired;
    this.supervisorThreshold = builder.supervisorThreshold;

    // Context management
    this.contextConfig = builder.contextConfig;
    this.contextCompressionEnabled = builder.contextCompressionEnabled;

    // State machine
    this.stateMachine = builder.stateMachine;

    // Completion tracking
    this.taskList = builder.taskList;
    this.autoDetectTaskCompletion = builder.autoDetectTaskCompletion;

    // Saga integration
    this.compensationEnabled = builder.compensationEnabled;
    this.compensationConfig = Collections.unmodifiableMap(new HashMap<>(builder.compensationConfig));

    // Memory
    this.shortTermTtl = builder.shortTermTtl;
    this.shortTermMemorySpec = builder.shortTermMemorySpec;
    this.longTermStore = builder.longTermStore;
    this.memoryChannels =
        Collections.unmodifiableList(new ArrayList<>(builder.memoryChannels));
    this.vectorMemorySpec = builder.vectorMemorySpec;

    // Chat
    this.chatConnection = builder.chatConnection;
    this.chatSetup = builder.chatSetup;

    // Embeddings
    this.embeddingConnection = builder.embeddingConnection;
    this.embeddingSetup = builder.embeddingSetup;

    // Listeners
    this.listeners =
        Collections.unmodifiableList(new ArrayList<>(builder.listeners));

    // Skills + MCP
    SkillRegistry.Builder rb = SkillRegistry.builder();
    for (Skill s : builder.skills) {
      rb.register(s);
    }
    this.skillRegistry = rb.build();
    this.mcpServers =
        Collections.unmodifiableList(new ArrayList<>(builder.mcpServers));
    this.remoteAgents =
        Collections.unmodifiableList(new ArrayList<>(builder.remoteAgents));
    this.a2aClientFactory =
        builder.a2aClientFactory == null
            ? org.agentic.flink.a2a.A2AClientFactory.discovering()
            : builder.a2aClientFactory;

    // Inference
    this.inferenceConnections =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.inferenceConnections));
    this.inferenceTools =
        Collections.unmodifiableList(new ArrayList<>(builder.inferenceTools));
    this.guardrails =
        Collections.unmodifiableList(new ArrayList<>(builder.guardrails));
  }

  // ==================== Getters ====================

  public String getAgentId() { return agentId; }
  public String getAgentName() { return agentName; }
  public String getDescription() { return description; }
  public AgentType getAgentType() { return agentType; }

  public String getSystemPrompt() { return systemPrompt; }
  public String getLlmModel() { return chatSetup.getModelName(); }
  public double getTemperature() { return chatSetup.getTemperature(); }
  public int getMaxTokens() { return maxTokens; }
  public int getMaxResponseTokens() { return chatSetup.getMaxResponseTokens(); }

  public Set<String> getAllowedTools() { return allowedTools; }
  public Set<String> getRequiredTools() { return requiredTools; }
  public Map<String, Object> getToolDefaults() { return toolDefaults; }

  public int getMaxIterations() { return maxIterations; }
  public Duration getTimeout() { return timeout; }
  public Duration getToolTimeout() { return toolTimeout; }
  public int getMaxRetries() { return maxRetries; }

  public boolean isValidationEnabled() { return validationEnabled; }
  public int getMaxValidationAttempts() { return maxValidationAttempts; }
  public boolean isCorrectionEnabled() { return correctionEnabled; }
  public int getMaxCorrectionAttempts() { return maxCorrectionAttempts; }
  public String getValidationPrompt() { return validationPrompt; }
  public String getCorrectionPrompt() { return correctionPrompt; }

  public String getSupervisorId() { return supervisorId; }
  public boolean isSupervisorReviewRequired() { return supervisorReviewRequired; }
  public double getSupervisorThreshold() { return supervisorThreshold; }

  public ContextWindowManager.ContextWindowConfig getContextConfig() { return contextConfig; }
  public boolean isContextCompressionEnabled() { return contextCompressionEnabled; }

  public AgentStateMachine getStateMachine() { return stateMachine; }

  public TaskList getTaskList() { return taskList; }
  public boolean isAutoDetectTaskCompletion() { return autoDetectTaskCompletion; }

  public boolean isCompensationEnabled() { return compensationEnabled; }
  public Map<String, Object> getCompensationConfig() { return compensationConfig; }

  public Duration getShortTermTtl() { return shortTermTtl; }
  public ShortTermMemorySpec getShortTermMemorySpec() { return shortTermMemorySpec; }
  public LongTermMemoryStore getLongTermStore() { return longTermStore; }
  public List<Channel<KeyedContextItem>> getMemoryChannels() { return memoryChannels; }
  public VectorMemorySpec getVectorMemorySpec() { return vectorMemorySpec; }
  public boolean hasVectorMemory() { return vectorMemorySpec != null; }
  public boolean hasLongTermStore() { return longTermStore != null; }

  public ChatConnection getChatConnection() { return chatConnection; }
  public ChatSetup getChatSetup() { return chatSetup; }
  public EmbeddingConnection getEmbeddingConnection() { return embeddingConnection; }
  public EmbeddingSetup getEmbeddingSetup() { return embeddingSetup; }
  public List<AgentEventListener> getListeners() { return listeners; }
  public SkillRegistry getSkillRegistry() { return skillRegistry; }
  public List<McpServerSpec> getMcpServers() { return mcpServers; }
  public List<org.agentic.flink.a2a.RemoteAgentSpec> getRemoteAgents() { return remoteAgents; }
  public org.agentic.flink.a2a.A2AClientFactory getA2AClientFactory() { return a2aClientFactory; }
  public boolean hasSkills() { return skillRegistry != null && skillRegistry.size() > 0; }
  public boolean hasMcpServers() { return !mcpServers.isEmpty(); }
  public boolean hasRemoteAgents() { return !remoteAgents.isEmpty(); }

  public Map<String, InferenceConnection> getInferenceConnections() { return inferenceConnections; }
  public InferenceConnection getInferenceConnection(String name) { return inferenceConnections.get(name); }
  public List<InferenceToolAdapter> getInferenceTools() { return inferenceTools; }
  public List<Guardrail> getGuardrails() { return guardrails; }
  public boolean hasGuardrails() { return !guardrails.isEmpty(); }

  // ==================== Helper Methods ====================

  /**
   * Checks if this agent is allowed to use a specific tool.
   *
   * @param toolName The tool name to check
   * @return true if the tool is in the allowed list
   */
  public boolean canUseTool(String toolName) {
    return allowedTools.contains(toolName);
  }

  /**
   * Checks if this agent has a supervisor configured.
   *
   * @return true if supervisorId is set
   */
  public boolean hasSupervisor() {
    return supervisorId != null && !supervisorId.isEmpty();
  }

  /**
   * Checks if this agent requires completion tracking.
   *
   * @return true if taskList is configured
   */
  public boolean requiresCompletionTracking() {
    return taskList != null;
  }

  /**
   * Creates a builder initialized with this agent's configuration.
   *
   * <p>Useful for creating modified copies of agents.
   *
   * @return builder pre-populated with this agent's settings
   */
  public AgentBuilder toBuilder() {
    return new AgentBuilder()
        .withId(this.agentId)
        .withName(this.agentName)
        .withDescription(this.description)
        .withType(this.agentType)
        .withSystemPrompt(this.systemPrompt)
        .withChatSetup(this.chatSetup)
        .withMaxTokens(this.maxTokens)
        .withTools(this.allowedTools.toArray(new String[0]))
        .withMaxIterations(this.maxIterations)
        .withTimeout(this.timeout)
        .withValidationEnabled(this.validationEnabled)
        .withCorrectionEnabled(this.correctionEnabled)
        .withSupervisor(this.supervisorId)
        .withCompensationEnabled(this.compensationEnabled);
  }

  /**
   * Creates a new builder for defining an agent.
   *
   * @return new agent builder
   */
  public static AgentBuilder builder() {
    return new AgentBuilder();
  }

  @Override
  public String toString() {
    return String.format(
        "Agent[id=%s, name=%s, type=%s, model=%s, tools=%d, supervisor=%s]",
        agentId, agentName, agentType, chatSetup.getModelName(), allowedTools.size(),
        hasSupervisor() ? supervisorId : "none");
  }

  // ==================== Agent Types ====================

  /**
   * Enum defining different types of agents.
   *
   * <p>This allows pre-configured agent templates and routing logic.
   */
  public enum AgentType {
    /**
     * Simple executor agent - performs tasks without validation or supervision.
     */
    EXECUTOR("Executor", "Performs tasks with tool calling"),

    /**
     * Validator agent - validates inputs or outputs.
     */
    VALIDATOR("Validator", "Validates data against rules or schemas"),

    /**
     * Corrector agent - attempts to fix validation failures.
     */
    CORRECTOR("Corrector", "Fixes validation failures using LLM feedback"),

    /**
     * Supervisor agent - reviews and approves work from other agents.
     */
    SUPERVISOR("Supervisor", "Reviews and approves agent outputs"),

    /**
     * Coordinator agent - orchestrates multiple sub-agents.
     */
    COORDINATOR("Coordinator", "Orchestrates multiple sub-agent workflows"),

    /**
     * Research agent - specializes in information gathering and synthesis.
     */
    RESEARCHER("Researcher", "Gathers and synthesizes information"),

    /**
     * ReAct agent — reason / act / observe loop. Pair with {@link
     * org.agentic.flink.function.ReActProcessFunction} for the canonical scaffolding.
     */
    REACT("ReAct", "Reason/Act/Observe loop with bounded iteration budget"),

    /**
     * Custom agent - user-defined behavior.
     */
    CUSTOM("Custom", "User-defined agent behavior");

    private final String displayName;
    private final String description;

    AgentType(String displayName, String description) {
      this.displayName = displayName;
      this.description = description;
    }

    public String getDisplayName() { return displayName; }
    public String getDescription() { return description; }
  }
}
