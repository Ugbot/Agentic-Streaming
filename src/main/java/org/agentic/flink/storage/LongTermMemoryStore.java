package org.agentic.flink.storage;

import org.agentic.flink.context.core.AgentContext;
import org.agentic.flink.context.core.ContextItem;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Storage interface for long-term memory and conversation persistence (warm tier).
 *
 * <p>Long-term memory stores conversation context and facts that need to persist beyond the active
 * processing window. This enables conversation resumption after job restarts and provides access
 * to historical context.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Tier: WARM
 *   <li>Latency: 1-10ms
 *   <li>Scope: Recent conversations for resumption, long-term facts
 *   <li>TTL: Hours to days
 *   <li>Backends: Redis, DynamoDB, Cassandra, MongoDB
 * </ul>
 *
 * <p>Use cases:
 *
 * <ul>
 *   <li>Conversation resumption after job restart
 *   <li>Long-term facts (user preferences, business rules)
 *   <li>Cross-job context sharing
 *   <li>Recent conversation history
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * LongTermMemoryStore store = new RedisConversationStore();
 * store.initialize(config);
 *
 * // Save full conversation context
 * store.saveContext("flow-001", agentContext);
 *
 * // Save long-term facts
 * Map<String, ContextItem> facts = new HashMap<>();
 * facts.put("user_tier", new ContextItem("premium", ContextPriority.MUST));
 * facts.put("preferred_language", new ContextItem("en", ContextPriority.SHOULD));
 * store.saveFacts("flow-001", facts);
 *
 * // Resume conversation
 * Optional<AgentContext> resumed = store.loadContext("flow-001");
 * Map<String, ContextItem> loadedFacts = store.loadFacts("flow-001");
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public interface LongTermMemoryStore extends StorageProvider<String, AgentContext> {

  /**
   * Save complete agent context for a conversation.
   *
   * <p>This stores the full AgentContext including all metadata, state, and conversation history.
   * Used for conversation resumption after restarts.
   *
   * @param flowId Conversation flow identifier
   * @param context Complete agent context to persist
   * @throws Exception if save operation fails
   */
  void saveContext(String flowId, AgentContext context) throws Exception;

  /**
   * Load complete agent context for a conversation.
   *
   * <p>Returns Optional.empty() if no context exists or has expired.
   *
   * @param flowId Conversation flow identifier
   * @return Optional containing the agent context, or empty if not found
   * @throws Exception if load operation fails
   */
  Optional<AgentContext> loadContext(String flowId) throws Exception;

  /**
   * Check if a conversation exists in long-term storage.
   *
   * <p>More efficient than loading full context when only existence needs to be checked.
   *
   * @param flowId Conversation flow identifier
   * @return true if conversation exists and has not expired
   * @throws Exception if existence check fails
   */
  boolean conversationExists(String flowId) throws Exception;

  /**
   * Delete a conversation and all associated data.
   *
   * <p>This removes the context, facts, and any conversation metadata. This operation is
   * irreversible.
   *
   * @param flowId Conversation flow identifier
   * @throws Exception if deletion fails
   */
  void deleteConversation(String flowId) throws Exception;

  /**
   * Save long-term facts for a conversation.
   *
   * <p>Facts are key-value pairs that persist across the conversation lifecycle. Examples: user
   * preferences, account tier, business rules, validated information.
   *
   * <p>This operation replaces existing facts. For incremental updates, use addFact().
   *
   * @param flowId Conversation flow identifier
   * @param facts Map of fact IDs to context items
   * @throws Exception if save operation fails
   */
  void saveFacts(String flowId, Map<String, ContextItem> facts) throws Exception;

  /**
   * Load long-term facts for a conversation.
   *
   * <p>Returns empty map if no facts exist.
   *
   * @param flowId Conversation flow identifier
   * @return Map of fact IDs to context items
   * @throws Exception if load operation fails
   */
  Map<String, ContextItem> loadFacts(String flowId) throws Exception;

  /**
   * Add or update a single long-term fact.
   *
   * <p>This updates an existing fact or creates a new one without affecting other facts.
   *
   * @param flowId Conversation flow identifier
   * @param factId Fact identifier (e.g., "user_tier", "preferred_language")
   * @param fact Context item containing the fact data
   * @throws Exception if update operation fails
   */
  void addFact(String flowId, String factId, ContextItem fact) throws Exception;

  /**
   * Remove a specific long-term fact.
   *
   * @param flowId Conversation flow identifier
   * @param factId Fact identifier to remove
   * @throws Exception if removal operation fails
   */
  void removeFact(String flowId, String factId) throws Exception;

  /**
   * List all active conversation IDs.
   *
   * <p>Returns conversations that exist and have not expired. Used for monitoring and batch
   * operations.
   *
   * @return List of conversation flow IDs
   * @throws Exception if listing operation fails
   */
  List<String> listActiveConversations() throws Exception;

  /**
   * List conversations for a specific user.
   *
   * <p>Returns all conversations associated with a userId. Requires that AgentContext includes
   * userId metadata.
   *
   * @param userId User identifier
   * @return List of conversation flow IDs for this user
   * @throws Exception if listing operation fails
   */
  List<String> listConversationsForUser(String userId) throws Exception;

  /**
   * Get metadata for a conversation without loading full context.
   *
   * <p>Returns lightweight metadata like:
   *
   * <ul>
   *   <li>created_at: Conversation start timestamp
   *   <li>last_updated_at: Last activity timestamp
   *   <li>user_id: Associated user
   *   <li>agent_id: Agent handling this conversation
   *   <li>turn_count: Number of turns in conversation
   * </ul>
   *
   * @param flowId Conversation flow identifier
   * @return Map of metadata keys to values
   * @throws Exception if metadata retrieval fails
   */
  Map<String, Object> getConversationMetadata(String flowId) throws Exception;

  /**
   * Set time-to-live for a conversation.
   *
   * <p>After TTL expires, the conversation and all associated data will be deleted. Used to
   * implement retention policies.
   *
   * @param flowId Conversation flow identifier
   * @param ttlSeconds Time to live in seconds
   * @throws Exception if TTL setting fails
   */
  void setConversationTTL(String flowId, long ttlSeconds) throws Exception;

  /**
   * Archive a conversation to cold storage.
   *
   * <p>Moves conversation from warm tier to cold tier for long-term retention. The conversation is
   * removed from warm storage after successful archival.
   *
   * @param flowId Conversation flow identifier
   * @param coldStore Cold tier storage to archive to
   * @throws Exception if archival fails
   */
  void archiveConversation(String flowId, StorageProvider<String, AgentContext> coldStore)
      throws Exception;

  @Override
  default StorageTier getTier() {
    return StorageTier.WARM;
  }

  @Override
  default long getExpectedLatencyMs() {
    return 5; // 1-10ms for warm tier
  }
}
