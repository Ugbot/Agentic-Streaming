package org.agentic.flink.storage;

import org.agentic.flink.context.core.ContextItem;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Storage interface for steering context and system-level configuration.
 *
 * <p>Steering state includes system prompts, agent configuration, guardrails, business rules, and
 * other context that guides agent behavior rather than conversational content.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Tier: WARM or HOT (depending on update frequency)
 *   <li>Latency: 1-5ms
 *   <li>Scope: System prompts, agent configuration, business rules, guardrails
 *   <li>TTL: Long-lived or indefinite (updated infrequently)
 *   <li>Backends: Redis, DynamoDB, Cassandra (for distributed), in-memory cache (for local)
 * </ul>
 *
 * <p>Use cases:
 *
 * <ul>
 *   <li>System prompts and instructions
 *   <li>Agent personality and behavior configuration
 *   <li>Business rules and policies
 *   <li>Guardrails and safety constraints
 *   <li>Tool availability and permissions
 *   <li>Routing rules for multi-agent systems
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * SteeringStateStore store = new RedisSteeringStore();
 * store.initialize(config);
 *
 * // Save system prompt
 * store.saveSystemPrompt("customer-support-agent",
 *     "You are a helpful customer support agent. Always be polite and professional.");
 *
 * // Save agent configuration
 * Map<String, Object> agentConfig = new HashMap<>();
 * agentConfig.put("temperature", 0.7);
 * agentConfig.put("max_tokens", 500);
 * agentConfig.put("allowed_tools", Arrays.asList("order_lookup", "refund"));
 * store.saveAgentConfig("customer-support-agent", agentConfig);
 *
 * // Save business rules
 * List<ContextItem> rules = Arrays.asList(
 *     new ContextItem("Refunds must be approved for amounts > $100", ContextPriority.MUST),
 *     new ContextItem("Premium users get priority support", ContextPriority.SHOULD)
 * );
 * store.saveBusinessRules("customer-support", rules);
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public interface SteeringStateStore extends StorageProvider<String, ContextItem> {

  /**
   * Save system prompt for an agent.
   *
   * <p>System prompts define the agent's role, personality, and core instructions. These are
   * typically prepended to every conversation.
   *
   * @param agentId Agent identifier
   * @param systemPrompt System prompt text
   * @throws Exception if save operation fails
   */
  void saveSystemPrompt(String agentId, String systemPrompt) throws Exception;

  /**
   * Load system prompt for an agent.
   *
   * <p>Returns Optional.empty() if no system prompt is configured.
   *
   * @param agentId Agent identifier
   * @return Optional containing system prompt text
   * @throws Exception if load operation fails
   */
  Optional<String> loadSystemPrompt(String agentId) throws Exception;

  /**
   * Save agent configuration.
   *
   * <p>Configuration includes parameters like:
   *
   * <ul>
   *   <li>temperature: LLM sampling temperature
   *   <li>max_tokens: Maximum response length
   *   <li>top_p: Nucleus sampling parameter
   *   <li>allowed_tools: List of tools this agent can use
   *   <li>timeout_seconds: Operation timeout
   * </ul>
   *
   * @param agentId Agent identifier
   * @param config Configuration map
   * @throws Exception if save operation fails
   */
  void saveAgentConfig(String agentId, Map<String, Object> config) throws Exception;

  /**
   * Load agent configuration.
   *
   * <p>Returns empty map if no configuration exists.
   *
   * @param agentId Agent identifier
   * @return Configuration map
   * @throws Exception if load operation fails
   */
  Map<String, Object> loadAgentConfig(String agentId) throws Exception;

  /**
   * Save business rules for a domain.
   *
   * <p>Business rules are domain-specific policies that guide agent decisions. Examples:
   *
   * <ul>
   *   <li>Refund approval thresholds
   *   <li>Escalation criteria
   *   <li>Compliance requirements
   *   <li>SLA definitions
   * </ul>
   *
   * @param domain Domain identifier (e.g., "customer-support", "sales")
   * @param rules List of business rules as context items
   * @throws Exception if save operation fails
   */
  void saveBusinessRules(String domain, List<ContextItem> rules) throws Exception;

  /**
   * Load business rules for a domain.
   *
   * <p>Returns empty list if no rules exist.
   *
   * @param domain Domain identifier
   * @return List of business rules
   * @throws Exception if load operation fails
   */
  List<ContextItem> loadBusinessRules(String domain) throws Exception;

  /**
   * Save guardrails for an agent.
   *
   * <p>Guardrails are safety constraints and validation rules. Examples:
   *
   * <ul>
   *   <li>Content filtering rules
   *   <li>PII detection and masking
   *   <li>Output validation patterns
   *   <li>Action approval requirements
   * </ul>
   *
   * @param agentId Agent identifier
   * @param guardrails List of guardrails as context items
   * @throws Exception if save operation fails
   */
  void saveGuardrails(String agentId, List<ContextItem> guardrails) throws Exception;

  /**
   * Load guardrails for an agent.
   *
   * <p>Returns empty list if no guardrails exist.
   *
   * @param agentId Agent identifier
   * @return List of guardrails
   * @throws Exception if load operation fails
   */
  List<ContextItem> loadGuardrails(String agentId) throws Exception;

  /**
   * Save tool permissions for an agent.
   *
   * <p>Defines which tools an agent is allowed to use and under what conditions.
   *
   * @param agentId Agent identifier
   * @param toolPermissions Map of tool names to permission configuration
   * @throws Exception if save operation fails
   */
  void saveToolPermissions(String agentId, Map<String, Map<String, Object>> toolPermissions)
      throws Exception;

  /**
   * Load tool permissions for an agent.
   *
   * <p>Returns empty map if no permissions are configured (default deny).
   *
   * @param agentId Agent identifier
   * @return Map of tool names to permission configuration
   * @throws Exception if load operation fails
   */
  Map<String, Map<String, Object>> loadToolPermissions(String agentId) throws Exception;

  /**
   * Check if an agent has permission to use a specific tool.
   *
   * <p>More efficient than loading all permissions when checking a single tool.
   *
   * @param agentId Agent identifier
   * @param toolName Tool name to check
   * @return true if agent has permission to use this tool
   * @throws Exception if permission check fails
   */
  boolean hasToolPermission(String agentId, String toolName) throws Exception;

  /**
   * List all configured agents.
   *
   * <p>Returns agent IDs that have steering configuration. Used for monitoring and management.
   *
   * @return List of agent IDs
   * @throws Exception if listing operation fails
   */
  List<String> listConfiguredAgents() throws Exception;

  /**
   * Delete all steering configuration for an agent.
   *
   * <p>Removes system prompt, configuration, guardrails, and tool permissions.
   *
   * @param agentId Agent identifier
   * @throws Exception if deletion fails
   */
  void deleteAgentConfig(String agentId) throws Exception;

  /**
   * Version steering configuration for rollback capability.
   *
   * <p>Creates a versioned snapshot of all steering state for an agent. This enables rollback if a
   * configuration change causes issues.
   *
   * @param agentId Agent identifier
   * @param version Version identifier (e.g., "v1.2.3", timestamp)
   * @throws Exception if versioning fails
   */
  void versionSteeringState(String agentId, String version) throws Exception;

  /**
   * Rollback steering configuration to a previous version.
   *
   * @param agentId Agent identifier
   * @param version Version identifier to rollback to
   * @throws Exception if rollback fails
   */
  void rollbackSteeringState(String agentId, String version) throws Exception;

  @Override
  default StorageTier getTier() {
    return StorageTier.WARM; // Can be HOT if cached locally
  }

  @Override
  default long getExpectedLatencyMs() {
    return 3; // 1-5ms for steering state
  }
}
