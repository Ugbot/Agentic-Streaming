package org.agentic.flink.dsl;

import java.io.Serializable;
import java.util.*;

/**
 * Defines a flexible N-tier supervisor chain for agent workflows.
 *
 * <p>A SupervisorChain represents a hierarchical structure of supervisor agents where work can be
 * escalated from one tier to the next. This enables sophisticated review and quality control
 * workflows.
 *
 * <p><b>Example Structures:</b>
 *
 * <p><b>3-Tier (Like TieredAgentExample):</b>
 * <pre>
 * Tier 1: Validation Agent  → validates inputs
 * Tier 2: Execution Agent   → performs work
 * Tier 3: Supervisor Agent  → reviews quality
 * </pre>
 *
 * <p><b>4-Tier Security Review:</b>
 * <pre>
 * Tier 1: Executor      → performs task
 * Tier 2: QA Reviewer   → checks quality
 * Tier 3: Security Check → validates security
 * Tier 4: Final Approval → human-in-the-loop
 * </pre>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * SupervisorChain chain = SupervisorChain.builder()
 *     .withId("research-chain")
 *     .addTier("executor", executorAgent)
 *     .addTier("quality-check", qaAgent)
 *     .addTier("final-review", supervisorAgent)
 *     .withEscalationPolicy(EscalationPolicy.NEXT_TIER)
 *     .withAutoEscalateOnScore(0.7)
 *     .build();
 *
 * // Use in job
 * AgentJob job = AgentJob.builder()
 *     .withSupervisorChain(chain)
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see SupervisorChainBuilder
 */
public class SupervisorChain implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String chainId;
  private final String chainName;
  private final List<SupervisorTier> tiers;
  private final EscalationPolicy escalationPolicy;
  private final double autoEscalateThreshold;
  private final int maxEscalations;
  private final boolean failOnMaxEscalations;

  // Package-private constructor - use builder
  SupervisorChain(SupervisorChainBuilder builder) {
    this.chainId = builder.chainId;
    this.chainName = builder.chainName;
    this.tiers = Collections.unmodifiableList(new ArrayList<>(builder.tiers));
    this.escalationPolicy = builder.escalationPolicy;
    this.autoEscalateThreshold = builder.autoEscalateThreshold;
    this.maxEscalations = builder.maxEscalations;
    this.failOnMaxEscalations = builder.failOnMaxEscalations;
  }

  public String getChainId() { return chainId; }
  public String getChainName() { return chainName; }
  public List<SupervisorTier> getTiers() { return tiers; }
  public EscalationPolicy getEscalationPolicy() { return escalationPolicy; }
  public double getAutoEscalateThreshold() { return autoEscalateThreshold; }
  public int getMaxEscalations() { return maxEscalations; }
  public boolean isFailOnMaxEscalations() { return failOnMaxEscalations; }

  /**
   * Gets the number of tiers in this chain.
   *
   * @return tier count
   */
  public int getTierCount() {
    return tiers.size();
  }

  /**
   * Gets a tier by index (0-based).
   *
   * @param tierIndex The tier index
   * @return the supervisor tier
   * @throws IndexOutOfBoundsException if index invalid
   */
  public SupervisorTier getTier(int tierIndex) {
    return tiers.get(tierIndex);
  }

  /**
   * Gets a tier by name.
   *
   * @param tierName The tier name
   * @return Optional containing the tier, or empty if not found
   */
  public Optional<SupervisorTier> getTierByName(String tierName) {
    return tiers.stream()
        .filter(t -> t.getTierName().equals(tierName))
        .findFirst();
  }

  /**
   * Gets the first tier (entry point).
   *
   * @return the first tier
   */
  public SupervisorTier getFirstTier() {
    if (tiers.isEmpty()) {
      throw new IllegalStateException("Chain has no tiers");
    }
    return tiers.get(0);
  }

  /**
   * Gets the last tier (final supervisor).
   *
   * @return the last tier
   */
  public SupervisorTier getLastTier() {
    if (tiers.isEmpty()) {
      throw new IllegalStateException("Chain has no tiers");
    }
    return tiers.get(tiers.size() - 1);
  }

  /**
   * Gets the next tier after the specified tier.
   *
   * @param currentTierIndex Current tier index
   * @return Optional containing next tier, or empty if at end
   */
  public Optional<SupervisorTier> getNextTier(int currentTierIndex) {
    if (currentTierIndex < 0 || currentTierIndex >= tiers.size() - 1) {
      return Optional.empty();
    }
    return Optional.of(tiers.get(currentTierIndex + 1));
  }

  /**
   * Checks if escalation should occur based on a quality score.
   *
   * @param qualityScore The quality score (0.0 to 1.0)
   * @return true if score < threshold
   */
  public boolean shouldEscalate(double qualityScore) {
    return qualityScore < autoEscalateThreshold;
  }

  public static SupervisorChainBuilder builder() {
    return new SupervisorChainBuilder();
  }

  @Override
  public String toString() {
    return String.format(
        "SupervisorChain[id=%s, tiers=%d, policy=%s]",
        chainId, tiers.size(), escalationPolicy);
  }

  // ==================== Supervisor Tier ====================

  /**
   * Represents one tier (level) in the supervisor chain.
   */
  public static class SupervisorTier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int tierIndex;
    private final String tierName;
    private final Agent agent;
    private final boolean requiresHumanApproval;
    private final double qualityThreshold;

    public SupervisorTier(
        int tierIndex,
        String tierName,
        Agent agent,
        boolean requiresHumanApproval,
        double qualityThreshold) {
      this.tierIndex = tierIndex;
      this.tierName = tierName;
      this.agent = agent;
      this.requiresHumanApproval = requiresHumanApproval;
      this.qualityThreshold = qualityThreshold;
    }

    public int getTierIndex() { return tierIndex; }
    public String getTierName() { return tierName; }
    public Agent getAgent() { return agent; }
    public boolean isRequiresHumanApproval() { return requiresHumanApproval; }
    public double getQualityThreshold() { return qualityThreshold; }

    /**
     * Checks if this is the first tier (entry point).
     *
     * @return true if tierIndex == 0
     */
    public boolean isFirstTier() {
      return tierIndex == 0;
    }

    @Override
    public String toString() {
      return String.format(
          "Tier[%d: %s, agent=%s, humanApproval=%s]",
          tierIndex, tierName, agent.getAgentId(), requiresHumanApproval);
    }
  }

  // ==================== Escalation Policy ====================

  /**
   * Defines how escalation works when a tier rejects/fails.
   */
  public enum EscalationPolicy {
    /**
     * Escalate to the next tier in the chain.
     *
     * <p>Tier 1 → Tier 2 → Tier 3 → ...
     */
    NEXT_TIER("Next Tier", "Escalate to next tier in sequence"),

    /**
     * Skip intermediate tiers and escalate directly to the top.
     *
     * <p>Tier 1 → Tier N (final)
     */
    SKIP_TO_TOP("Skip to Top", "Escalate directly to final tier"),

    /**
     * Retry the current tier (with corrections if configured).
     *
     * <p>Useful for correction loops before escalating.
     */
    RETRY_CURRENT("Retry Current", "Retry current tier with corrections"),

    /**
     * Fail immediately without escalation.
     *
     * <p>For strict workflows where rejection means failure.
     */
    FAIL_FAST("Fail Fast", "Fail immediately on rejection"),

    /**
     * Custom escalation logic (user-defined).
     */
    CUSTOM("Custom", "Custom user-defined escalation logic");

    private final String displayName;
    private final String description;

    EscalationPolicy(String displayName, String description) {
      this.displayName = displayName;
      this.description = description;
    }

    public String getDisplayName() { return displayName; }
    public String getDescription() { return description; }
  }
}
