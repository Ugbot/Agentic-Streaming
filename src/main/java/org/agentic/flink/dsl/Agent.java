package org.agentic.flink.dsl;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import org.agentic.flink.annotation.Public;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.skill.SkillRegistry;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.storage.LongTermMemoryStore;
import org.agentic.flink.tools.mcp.McpServerSpec;

/**
 * Immutable agent definition created via the declarative builder API.
 *
 * <p>An Agent is the configuration record consumed by the legacy pure-Flink execution graph ({@link
 * org.agentic.flink.job.AgentJobGenerator}): the CEP dispatcher reads the state machine and the
 * short-term TTL, the async execution operator ({@link
 * org.agentic.flink.stream.AgentExecutionFunction}) reads everything else. Every value stored here
 * is read by one of those operators or by {@link org.agentic.flink.execution.AgentExecutor}.
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * Agent researchAgent = Agent.builder()
 *     .withId("research-agent")
 *     .withName("Research Specialist")
 *     .withSystemPrompt("You are a research specialist. Gather and synthesize information.")
 *     .withTools("web-search", "document-analysis", "synthesis")
 *     .withRequiredTools("web-search")
 *     .withMaxIterations(10)
 *     .withTimeout(Duration.ofMinutes(5))
 *     .withToolTimeout(Duration.ofSeconds(20))
 *     .withCompensatingTool("book-flight", "cancel-flight")
 *     .build();
 * }</pre>
 *
 * <p>The Agent is immutable after creation and can be safely shared across Flink task managers.
 *
 * @author Agentic Flink Team
 * @see AgentBuilder
 * @deprecated the agent model (turn/event/state/router/brain/tool/guardrail/verifier/saga) now
 *     lives in the canonical core {@code org.jagentic.core} and runs on Flink through {@link
 *     org.agentic.flink.runtime.WorkflowTurnFunction}; this class is kept as the pre-spec
 *     Flink-only DSL and receives no new features.
 */
@Deprecated(since = "1.0.0")
@Public
public class Agent implements Serializable {

  private static final long serialVersionUID = 1L;

  // ==================== Core Identity ====================

  private final String agentId;
  private final String agentName;
  private final String description;
  private final AgentType agentType;

  // ==================== LLM Configuration ====================

  private final String systemPrompt;

  // ==================== Tool Configuration ====================

  private final Set<String> allowedTools;
  private final Set<String> requiredTools;
  private final Map<String, Map<String, Object>> toolDefaults;

  // ==================== Execution Configuration ====================

  private final int maxIterations;
  private final Duration timeout;
  private final Duration toolTimeout;

  // ==================== Validation & Correction (state machine retry budgets) ====================

  private final int maxValidationAttempts;
  private final int maxCorrectionAttempts;

  // ==================== State Machine ====================

  private final AgentStateMachine stateMachine;

  // ==================== Saga Integration ====================

  private final boolean compensationEnabled;
  private final Map<String, String> compensatingTools;

  // ==================== Memory ====================

  private final Duration shortTermTtl;
  private final LongTermMemoryStore longTermStore;
  private final ConversationStore conversationStore;

  // Chat
  private final ChatConnection chatConnection;
  private final ChatSetup chatSetup;

  // Listeners
  private final List<AgentEventListener> listeners;

  // Skills + MCP
  private final SkillRegistry skillRegistry;
  private final List<McpServerSpec> mcpServers;

  // A2A remote agents (peers)
  private final List<org.agentic.flink.a2a.RemoteAgentSpec> remoteAgents;
  private final org.agentic.flink.a2a.A2AClientFactory a2aClientFactory;

  // Inference
  private final List<InferenceToolAdapter> inferenceTools;
  private final List<Guardrail> guardrails;

  // Package-private constructor - use builder
  Agent(AgentBuilder builder) {
    this.agentId = builder.agentId;
    this.agentName = builder.agentName;
    this.description = builder.description;
    this.agentType = builder.agentType;

    this.systemPrompt = builder.systemPrompt;

    this.allowedTools = Collections.unmodifiableSet(new HashSet<>(builder.allowedTools));
    this.requiredTools = Collections.unmodifiableSet(new HashSet<>(builder.requiredTools));
    Map<String, Map<String, Object>> defaults = new HashMap<>();
    builder.toolDefaults.forEach(
        (tool, values) -> defaults.put(tool, Collections.unmodifiableMap(new HashMap<>(values))));
    this.toolDefaults = Collections.unmodifiableMap(defaults);

    this.maxIterations = builder.maxIterations;
    this.timeout = builder.timeout;
    this.toolTimeout = builder.toolTimeout;

    this.maxValidationAttempts = builder.maxValidationAttempts;
    this.maxCorrectionAttempts = builder.maxCorrectionAttempts;

    this.stateMachine = builder.stateMachine;

    this.compensationEnabled = builder.compensationEnabled;
    this.compensatingTools =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.compensatingTools));

    this.shortTermTtl = builder.shortTermTtl;
    this.longTermStore = builder.longTermStore;
    this.conversationStore = builder.conversationStore;

    this.chatConnection = builder.chatConnection;
    this.chatSetup = builder.chatSetup;

    this.listeners = Collections.unmodifiableList(new ArrayList<>(builder.listeners));

    SkillRegistry.Builder rb = SkillRegistry.builder();
    for (Skill s : builder.skills) {
      rb.register(s);
    }
    this.skillRegistry = rb.build();
    this.mcpServers = Collections.unmodifiableList(new ArrayList<>(builder.mcpServers));
    this.remoteAgents = Collections.unmodifiableList(new ArrayList<>(builder.remoteAgents));
    this.a2aClientFactory =
        builder.a2aClientFactory == null
            ? org.agentic.flink.a2a.A2AClientFactory.discovering()
            : builder.a2aClientFactory;

    this.inferenceTools = Collections.unmodifiableList(new ArrayList<>(builder.inferenceTools));
    this.guardrails = Collections.unmodifiableList(new ArrayList<>(builder.guardrails));
  }

  // ==================== Getters ====================

  public String getAgentId() {
    return agentId;
  }

  public String getAgentName() {
    return agentName;
  }

  public String getDescription() {
    return description;
  }

  public AgentType getAgentType() {
    return agentType;
  }

  public String getSystemPrompt() {
    return systemPrompt;
  }

  public String getLlmModel() {
    return chatSetup.getModelName();
  }

  public double getTemperature() {
    return chatSetup.getTemperature();
  }

  public int getMaxResponseTokens() {
    return chatSetup.getMaxResponseTokens();
  }

  public Set<String> getAllowedTools() {
    return allowedTools;
  }

  public Set<String> getRequiredTools() {
    return requiredTools;
  }

  /** Default parameters per tool name, merged under the arguments the model supplies. */
  public Map<String, Map<String, Object>> getToolDefaults() {
    return toolDefaults;
  }

  public int getMaxIterations() {
    return maxIterations;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public Duration getToolTimeout() {
    return toolTimeout;
  }

  public int getMaxValidationAttempts() {
    return maxValidationAttempts;
  }

  public int getMaxCorrectionAttempts() {
    return maxCorrectionAttempts;
  }

  public AgentStateMachine getStateMachine() {
    return stateMachine;
  }

  public boolean isCompensationEnabled() {
    return compensationEnabled;
  }

  /** Tool name to compensating tool name, in registration order. */
  public Map<String, String> getCompensatingTools() {
    return compensatingTools;
  }

  public Duration getShortTermTtl() {
    return shortTermTtl;
  }

  public LongTermMemoryStore getLongTermStore() {
    return longTermStore;
  }

  public boolean hasLongTermStore() {
    return longTermStore != null;
  }

  public ConversationStore getConversationStore() {
    return conversationStore;
  }

  public ChatConnection getChatConnection() {
    return chatConnection;
  }

  public ChatSetup getChatSetup() {
    return chatSetup;
  }

  public List<AgentEventListener> getListeners() {
    return listeners;
  }

  public SkillRegistry getSkillRegistry() {
    return skillRegistry;
  }

  public List<McpServerSpec> getMcpServers() {
    return mcpServers;
  }

  public List<org.agentic.flink.a2a.RemoteAgentSpec> getRemoteAgents() {
    return remoteAgents;
  }

  public org.agentic.flink.a2a.A2AClientFactory getA2AClientFactory() {
    return a2aClientFactory;
  }

  public boolean hasSkills() {
    return skillRegistry != null && skillRegistry.size() > 0;
  }

  public boolean hasMcpServers() {
    return !mcpServers.isEmpty();
  }

  public boolean hasRemoteAgents() {
    return !remoteAgents.isEmpty();
  }

  public List<InferenceToolAdapter> getInferenceTools() {
    return inferenceTools;
  }

  public List<Guardrail> getGuardrails() {
    return guardrails;
  }

  public boolean hasGuardrails() {
    return !guardrails.isEmpty();
  }

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
   * Creates a builder initialized with this agent's configuration.
   *
   * <p>Useful for creating modified copies of agents. Listeners, MCP servers, inference tools and
   * guardrails are carried over. Skills and remote agents are not re-registered because their
   * prompt fragments and tool ids are already folded into the copied system prompt and allowed
   * tools. The state machine is rebuilt from the copied retry budgets unless {@link
   * AgentBuilder#withStateMachine} is called again.
   *
   * @return builder pre-populated with this agent's settings
   */
  public AgentBuilder toBuilder() {
    AgentBuilder b =
        new AgentBuilder()
            .withId(this.agentId)
            .withName(this.agentName)
            .withDescription(this.description)
            .withType(this.agentType)
            .withSystemPrompt(this.systemPrompt)
            .withChatConnection(this.chatConnection)
            .withChatSetup(this.chatSetup)
            .withTools(this.allowedTools.toArray(new String[0]))
            .withRequiredTools(this.requiredTools.toArray(new String[0]))
            .withMaxIterations(this.maxIterations)
            .withTimeout(this.timeout)
            .withToolTimeout(this.toolTimeout)
            .withMaxValidationAttempts(this.maxValidationAttempts)
            .withMaxCorrectionAttempts(this.maxCorrectionAttempts)
            .withCompensationEnabled(this.compensationEnabled)
            .withShortTermTtl(this.shortTermTtl)
            .withLongTermStore(this.longTermStore)
            .withConversationStore(this.conversationStore)
            .withA2AClientFactory(this.a2aClientFactory)
            .withListener(this.listeners.toArray(new AgentEventListener[0]))
            .withMcpServer(this.mcpServers.toArray(new McpServerSpec[0]))
            .withGuardrail(this.guardrails.toArray(new Guardrail[0]));
    this.toolDefaults.forEach(b::withToolDefaults);
    this.compensatingTools.forEach(b::withCompensatingTool);
    for (InferenceToolAdapter adapter : this.inferenceTools) {
      b.withInferenceTool(adapter);
    }
    return b;
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
        "Agent[id=%s, name=%s, type=%s, model=%s, tools=%d]",
        agentId, agentName, agentType, chatSetup.getModelName(), allowedTools.size());
  }

  // ==================== Agent Types ====================

  /**
   * Enum defining different types of agents.
   *
   * <p>This allows pre-configured agent templates and routing logic.
   */
  public enum AgentType {
    /** Simple executor agent - performs tasks without validation or supervision. */
    EXECUTOR("Executor", "Performs tasks with tool calling"),

    /** Validator agent - validates inputs or outputs. */
    VALIDATOR("Validator", "Validates data against rules or schemas"),

    /** Corrector agent - attempts to fix validation failures. */
    CORRECTOR("Corrector", "Fixes validation failures using LLM feedback"),

    /** Supervisor agent - reviews and approves work from other agents. */
    SUPERVISOR("Supervisor", "Reviews and approves agent outputs"),

    /** Coordinator agent - orchestrates multiple sub-agents. */
    COORDINATOR("Coordinator", "Orchestrates multiple sub-agent workflows"),

    /** Research agent - specializes in information gathering and synthesis. */
    RESEARCHER("Researcher", "Gathers and synthesizes information"),

    /**
     * ReAct agent: reason / act / observe loop. Pair with {@link
     * org.agentic.flink.function.ReActProcessFunction} for the canonical scaffolding.
     */
    REACT("ReAct", "Reason/Act/Observe loop with bounded iteration budget"),

    /** Custom agent - user-defined behavior. */
    CUSTOM("Custom", "User-defined agent behavior");

    private final String displayName;
    private final String description;

    AgentType(String displayName, String description) {
      this.displayName = displayName;
      this.description = description;
    }

    public String getDisplayName() {
      return displayName;
    }

    public String getDescription() {
      return description;
    }
  }
}
