package org.agentic.flink.dsl;

import org.agentic.flink.dsl.SupervisorChain.EscalationPolicy;
import org.agentic.flink.dsl.SupervisorChain.SupervisorTier;
import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating SupervisorChain instances.
 *
 * <p>Provides a declarative API for defining flexible N-tier supervisor hierarchies.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * SupervisorChain chain = SupervisorChain.builder()
 *     .withId("quality-chain")
 *     .withName("Quality Review Chain")
 *     .addTier("executor", executorAgent)
 *     .addTier("qa-review", qaAgent)
 *         .withQualityThreshold(0.8)
 *     .addTier("final-approval", supervisorAgent)
 *         .withHumanApprovalRequired()
 *     .withEscalationPolicy(EscalationPolicy.NEXT_TIER)
 *     .withAutoEscalateOnScore(0.7)
 *     .withMaxEscalations(2)
 *     .build();
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see SupervisorChain
 */
public class SupervisorChainBuilder {

  String chainId;
  String chainName;
  List<SupervisorTier> tiers = new ArrayList<>();
  EscalationPolicy escalationPolicy = EscalationPolicy.NEXT_TIER;
  double autoEscalateThreshold = 0.7;
  int maxEscalations = 3;
  boolean failOnMaxEscalations = true;

  // Current tier being built
  private TierBuilder currentTierBuilder;

  // Package-private constructor
  SupervisorChainBuilder() {}

  // ==================== Chain Configuration ====================

  /**
   * Sets the unique identifier for this chain (required).
   *
   * @param chainId The chain ID
   * @return this builder
   */
  public SupervisorChainBuilder withId(String chainId) {
    this.chainId = chainId;
    return this;
  }

  /**
   * Sets the display name for this chain.
   *
   * @param chainName The chain name
   * @return this builder
   */
  public SupervisorChainBuilder withName(String chainName) {
    this.chainName = chainName;
    return this;
  }

  /**
   * Sets the escalation policy (how to handle tier rejections).
   *
   * @param policy The escalation policy (default: NEXT_TIER)
   * @return this builder
   */
  public SupervisorChainBuilder withEscalationPolicy(EscalationPolicy policy) {
    this.escalationPolicy = policy;
    return this;
  }

  /**
   * Sets the quality score threshold for automatic escalation.
   *
   * <p>If a tier's output quality score falls below this threshold, automatically escalate to the
   * next tier.
   *
   * @param threshold Quality threshold (0.0 to 1.0, default: 0.7)
   * @return this builder
   */
  public SupervisorChainBuilder withAutoEscalateOnScore(double threshold) {
    this.autoEscalateThreshold = threshold;
    return this;
  }

  /**
   * Sets the maximum number of escalations allowed before failing.
   *
   * @param maxEscalations Maximum escalations (default: 3)
   * @return this builder
   */
  public SupervisorChainBuilder withMaxEscalations(int maxEscalations) {
    this.maxEscalations = maxEscalations;
    return this;
  }

  /**
   * Sets whether to fail if max escalations is reached.
   *
   * @param failOnMax true to fail (default), false to allow final tier approval
   * @return this builder
   */
  public SupervisorChainBuilder withFailOnMaxEscalations(boolean failOnMax) {
    this.failOnMaxEscalations = failOnMax;
    return this;
  }

  // ==================== Tier Building ====================

  /**
   * Adds a new tier to the chain.
   *
   * <p>Tiers are added in sequence: first addTier() call = Tier 0, second = Tier 1, etc.
   *
   * @param tierName The name of this tier
   * @param agent The agent for this tier
   * @return tier builder for configuring this tier
   */
  public TierBuilder addTier(String tierName, Agent agent) {
    // Finalize previous tier if any
    if (currentTierBuilder != null) {
      tiers.add(currentTierBuilder.buildTier());
    }

    // Create new tier builder
    currentTierBuilder = new TierBuilder(this, tierName, agent, tiers.size());
    return currentTierBuilder;
  }

  /**
   * Adds a simple tier with default settings.
   *
   * @param tierName The tier name
   * @param agent The agent
   * @return this builder
   */
  public SupervisorChainBuilder addSimpleTier(String tierName, Agent agent) {
    if (currentTierBuilder != null) {
      tiers.add(currentTierBuilder.buildTier());
    }
    tiers.add(new SupervisorTier(tiers.size(), tierName, agent, false, 0.0));
    currentTierBuilder = null;
    return this;
  }

  // ==================== Build ====================

  /**
   * Builds the immutable SupervisorChain.
   *
   * @return new SupervisorChain
   * @throws IllegalStateException if validation fails
   */
  public SupervisorChain build() {
    // Finalize last tier if any
    if (currentTierBuilder != null) {
      tiers.add(currentTierBuilder.buildTier());
      currentTierBuilder = null;
    }

    validate();
    applyDefaults();
    return new SupervisorChain(this);
  }

  // ==================== Private Methods ====================

  private void validate() {
    if (chainId == null || chainId.isEmpty()) {
      throw new IllegalStateException("Chain ID is required");
    }
    if (tiers.isEmpty()) {
      throw new IllegalStateException("Chain must have at least one tier");
    }
  }

  private void applyDefaults() {
    if (chainName == null || chainName.isEmpty()) {
      chainName = chainId + " Supervisor Chain";
    }
  }

  // ==================== Tier Builder ====================

  /**
   * Builder for configuring individual supervisor tiers.
   *
   * <p>Returned from addTier() to allow method chaining for tier-specific configuration.
   */
  public static class TierBuilder {
    private final SupervisorChainBuilder parent;
    private final String tierName;
    private final Agent agent;
    private final int tierIndex;
    private boolean requiresHumanApproval = false;
    private double qualityThreshold = 0.0;

    private TierBuilder(
        SupervisorChainBuilder parent,
        String tierName,
        Agent agent,
        int tierIndex) {
      this.parent = parent;
      this.tierName = tierName;
      this.agent = agent;
      this.tierIndex = tierIndex;
    }

    /**
     * Marks this tier as requiring human approval.
     *
     * <p>Work will pause at this tier until a human approves/rejects.
     *
     * @return this tier builder
     */
    public TierBuilder withHumanApprovalRequired() {
      this.requiresHumanApproval = true;
      return this;
    }

    /**
     * Sets the quality threshold for this tier.
     *
     * <p>If the agent's output quality score < threshold, escalate to next tier.
     *
     * @param threshold Quality threshold (0.0 to 1.0)
     * @return this tier builder
     */
    public TierBuilder withQualityThreshold(double threshold) {
      this.qualityThreshold = threshold;
      return this;
    }

    /**
     * Adds another tier (continues chain building).
     *
     * @param tierName Next tier name
     * @param agent Next tier agent
     * @return new tier builder for the next tier
     */
    public TierBuilder addTier(String tierName, Agent agent) {
      return parent.addTier(tierName, agent);
    }

    /**
     * Returns to parent builder to set chain-level properties.
     *
     * @return parent chain builder
     */
    public SupervisorChainBuilder withEscalationPolicy(EscalationPolicy policy) {
      parent.tiers.add(buildTier());  // Finalize this tier
      parent.currentTierBuilder = null;
      return parent.withEscalationPolicy(policy);
    }

    /**
     * Returns to parent builder to set chain-level properties.
     *
     * @return parent chain builder
     */
    public SupervisorChainBuilder withAutoEscalateOnScore(double threshold) {
      parent.tiers.add(buildTier());  // Finalize this tier
      parent.currentTierBuilder = null;
      return parent.withAutoEscalateOnScore(threshold);
    }

    /**
     * Builds the final chain (finalizes this tier).
     *
     * @return completed SupervisorChain
     */
    public SupervisorChain build() {
      return parent.build();
    }

    /**
     * Builds this tier.
     *
     * @return new SupervisorTier
     */
    private SupervisorTier buildTier() {
      return new SupervisorTier(
          tierIndex,
          tierName,
          agent,
          requiresHumanApproval,
          qualityThreshold
      );
    }
  }

  // ==================== Pre-configured Chain Templates ====================

  /**
   * Creates a simple 2-tier chain: Executor → Supervisor.
   *
   * @param executorAgent The executor agent
   * @param supervisorAgent The supervisor agent
   * @return 2-tier chain
   */
  public static SupervisorChain simpleTwoTier(Agent executorAgent, Agent supervisorAgent) {
    return SupervisorChain.builder()
        .withId("simple-two-tier")
        .addSimpleTier("executor", executorAgent)
        .addSimpleTier("supervisor", supervisorAgent)
        .withEscalationPolicy(EscalationPolicy.NEXT_TIER)
        .build();
  }

  /**
   * Creates a 3-tier chain like TieredAgentExample: Validator → Executor → Supervisor.
   *
   * @param validatorAgent The validator agent
   * @param executorAgent The executor agent
   * @param supervisorAgent The supervisor agent
   * @return 3-tier chain
   */
  public static SupervisorChain threeTierValidated(
      Agent validatorAgent,
      Agent executorAgent,
      Agent supervisorAgent) {
    return SupervisorChain.builder()
        .withId("three-tier-validated")
        .addSimpleTier("validator", validatorAgent)
        .addSimpleTier("executor", executorAgent)
        .addSimpleTier("supervisor", supervisorAgent)
        .withEscalationPolicy(EscalationPolicy.NEXT_TIER)
        .withAutoEscalateOnScore(0.8)
        .build();
  }

  /**
   * Creates a 4-tier chain with human approval: Executor → QA → Security → Human Approval.
   *
   * @param executorAgent The executor agent
   * @param qaAgent The QA agent
   * @param securityAgent The security agent
   * @param approvalAgent The final approval agent
   * @return 4-tier chain with human approval
   */
  public static SupervisorChain fourTierWithApproval(
      Agent executorAgent,
      Agent qaAgent,
      Agent securityAgent,
      Agent approvalAgent) {
    return SupervisorChain.builder()
        .withId("four-tier-approval")
        .addSimpleTier("executor", executorAgent)
        .addSimpleTier("qa-review", qaAgent)
        .addSimpleTier("security-check", securityAgent)
        .addTier("final-approval", approvalAgent)
            .withHumanApprovalRequired()
            .build();
  }
}
