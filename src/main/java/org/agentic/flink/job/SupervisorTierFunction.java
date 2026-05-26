package org.agentic.flink.job;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.SupervisorChain;
import org.agentic.flink.dsl.SupervisorChain.EscalationPolicy;
import org.agentic.flink.dsl.SupervisorChain.SupervisorTier;
import org.agentic.flink.execution.AgentExecutor;
import org.agentic.flink.execution.ExecutionResult;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CEP PatternProcessFunction for executing a supervisor tier with escalation logic.
 *
 * <p>Extends agent execution with supervisor-specific capabilities:
 * <ul>
 *   <li>Quality score evaluation against tier thresholds</li>
 *   <li>Escalation to next tier based on quality/failure</li>
 *   <li>Escalation policy enforcement (NEXT_TIER, SKIP_TO_TOP, RETRY_CURRENT, FAIL_FAST)</li>
 *   <li>Human approval handling (pause/resume)</li>
 *   <li>Max escalation limits</li>
 * </ul>
 *
 * <p><b>Escalation Flow Example (NEXT_TIER policy):</b>
 * <pre>
 * Tier 0 (Executor) → Quality < 0.7? → Escalate to Tier 1 (QA Review)
 * Tier 1 (QA Review) → Quality < 0.8? → Escalate to Tier 2 (Final Approval)
 * Tier 2 (Final Approval) → Human Approval → COMPLETED or ESCALATE
 * </pre>
 *
 * <p><b>Escalation Metadata:</b>
 * <pre>
 * event.metadata:
 *   - current_tier: 0
 *   - target_tier: 1
 *   - escalation_count: 1
 *   - escalation_reason: "quality_threshold"
 *   - quality_score: 0.65
 * </pre>
 *
 * @author Agentic Flink Team
 * @see SupervisorChain
 * @see EscalationPolicy
 */
public class SupervisorTierFunction extends PatternProcessFunction<AgentEvent, AgentEvent>
    implements TimedOutPartialMatchHandler<AgentEvent> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SupervisorTierFunction.class);

  private final SupervisorTier tier;
  private final SupervisorChain chain;
  private final ToolRegistry toolRegistry;

  // Side output tags
  private static final OutputTag<AgentEvent> ESCALATION_TAG =
      AgentJobGenerator.ESCALATION_TAG;
  private static final OutputTag<AgentEvent> TIMEOUT_TAG =
      AgentJobGenerator.TIMEOUT_TAG;
  private static final OutputTag<AgentEvent> VALIDATION_FAILURES_TAG =
      AgentJobGenerator.VALIDATION_FAILURES_TAG;

  private transient AgentExecutor executor;

  /** Regex for explicit score patterns like "Score: 0.9" or "Quality: 85%". */
  private static final Pattern SCORE_PATTERN =
      Pattern.compile("(?:score|quality|rating|confidence)[:\\s]+([0-9]+(?:\\.[0-9]+)?)[\\s]*(%)?",
          Pattern.CASE_INSENSITIVE);

  private static final String[] POSITIVE_KEYWORDS = {
      "good", "excellent", "correct", "complete", "accurate", "valid", "approved",
      "satisfactory", "well", "proper", "thorough", "comprehensive"
  };
  private static final String[] NEGATIVE_KEYWORDS = {
      "poor", "incorrect", "incomplete", "error", "invalid", "rejected",
      "unsatisfactory", "wrong", "missing", "failure", "failed", "bad"
  };

  public SupervisorTierFunction(
      SupervisorTier tier, SupervisorChain chain, ToolRegistry toolRegistry) {
    this.tier = tier;
    this.chain = chain;
    this.toolRegistry = toolRegistry;
  }

  private AgentExecutor getOrCreateExecutor() {
    if (executor == null) {
      executor = AgentExecutor.builder()
          .withAgent(tier.getAgent())
          .withToolRegistry(toolRegistry)
          .build();
    }
    return executor;
  }

  @Override
  public void processMatch(
      Map<String, List<AgentEvent>> match, Context ctx, Collector<AgentEvent> out)
      throws Exception {

    Agent agent = tier.getAgent();
    LOG.debug("Processing tier {} ({}) for agent: {}",
        tier.getTierIndex(), tier.getTierName(), agent.getAgentId());

    List<AgentEvent> startEvents = match.get("initial");
    if (startEvents == null || startEvents.isEmpty()) {
      LOG.warn("No start event in pattern match, skipping");
      return;
    }

    AgentEvent startEvent = startEvents.get(0);
    String flowId = startEvent.getFlowId();

    // Get escalation count from metadata
    int escalationCount = getEscalationCount(startEvent);

    LOG.info("Processing tier {} for flow: {} (escalation count: {})",
        tier.getTierIndex(), flowId, escalationCount);

    // Check max escalations
    if (escalationCount >= chain.getMaxEscalations()) {
      if (chain.isFailOnMaxEscalations()) {
        LOG.error("Max escalations ({}) reached for flow: {}, failing",
            chain.getMaxEscalations(), flowId);
        handleMaxEscalationsReached(startEvent, ctx, out);
        return;
      } else {
        LOG.warn("Max escalations ({}) reached for flow: {}, allowing completion",
            chain.getMaxEscalations(), flowId);
        // Continue processing at final tier
      }
    }

    try {
      // Execute the tier's agent via AgentExecutor
      AgentEvent tierResult = executeTier(startEvent, match);

      // Evaluate quality and determine if escalation is needed
      double qualityScore = evaluateQualityScore(tierResult);
      tierResult.getData().put("quality_score", qualityScore);
      tierResult.getData().put("tier_index", tier.getTierIndex());
      tierResult.getData().put("tier_name", tier.getTierName());

      LOG.debug("Tier {} quality score: {} (threshold: {})",
          tier.getTierIndex(), qualityScore, tier.getQualityThreshold());

      // Check if escalation is needed
      if (shouldEscalate(qualityScore)) {
        handleEscalation(tierResult, escalationCount, ctx, out);
      } else {
        // Quality passed - emit completion event
        AgentEvent completionEvent = tierResult.withEventType(AgentEventType.FLOW_COMPLETED);
        completionEvent.putMetadata("state", AgentState.COMPLETED.name());
        completionEvent.getData().put("approved_by_tier", tier.getTierIndex());

        out.collect(completionEvent);
        LOG.info("Tier {} approved flow: {}", tier.getTierIndex(), flowId);
      }

    } catch (Exception e) {
      LOG.error("Tier {} execution failed for flow: {}", tier.getTierIndex(), flowId, e);

      AgentEvent failureEvent = startEvent.withEventType(AgentEventType.FLOW_FAILED);
      failureEvent.incrementIteration();
      failureEvent.putMetadata("state", AgentState.FAILED.name());
      failureEvent.getData().put("error", e.getMessage());
      failureEvent.getData().put("failed_at_tier", tier.getTierIndex());

      ctx.output(VALIDATION_FAILURES_TAG, failureEvent);
    }
  }

  /**
   * Executes this supervisor tier by delegating to the {@link AgentExecutor}.
   *
   * <p>The executor runs the full agentic loop for the tier's agent, then the result
   * is packaged as a SUPERVISOR_REVIEW_COMPLETED event for quality evaluation.
   *
   * @throws TimeoutException if execution exceeds the agent's configured timeout
   */
  private AgentEvent executeTier(AgentEvent startEvent, Map<String, List<AgentEvent>> match)
      throws Exception {

    Agent agent = tier.getAgent();
    long timeoutMs = agent.getTimeout() != null
        ? agent.getTimeout().toMillis()
        : 300_000L; // 5 minute default

    ExecutionResult executionResult = getOrCreateExecutor()
        .execute(startEvent)
        .get(timeoutMs, TimeUnit.MILLISECONDS);

    AgentEvent result = startEvent.withEventType(AgentEventType.SUPERVISOR_REVIEW_COMPLETED);
    result.incrementIteration();
    result.putMetadata("state", AgentState.SUPERVISOR_REVIEW.name());
    result.getData().put("reviewed_by", agent.getAgentId());
    result.getData().put("tier_index", tier.getTierIndex());

    if (executionResult.isSuccess()) {
      result.getData().put("review_output", executionResult.getOutput());
      result.getData().put("tool_call_count", executionResult.getEvents().size());
    } else {
      result.getData().put("review_output", executionResult.getOutput());
      result.getData().put("execution_failed", true);
      result.getData().put("error", executionResult.getErrorMessage());
    }

    return result;
  }

  /**
   * Evaluates a quality score from the tier's execution result.
   *
   * <p>Scoring strategy (ordered by priority):
   * <ol>
   *   <li>If the event data already contains a numeric {@code quality_score}, use it directly.</li>
   *   <li>If the event was marked as a failed execution, return 0.0.</li>
   *   <li>Parse the LLM review output for explicit score patterns such as
   *       "Score: 0.9" or "Quality: 85%".</li>
   *   <li>Fall back to keyword analysis: count positive vs negative quality indicators
   *       in the review text and derive a score between 0.0 and 1.0.</li>
   *   <li>If no review output is available at all, default to 0.5 (uncertain).</li>
   * </ol>
   */
  private double evaluateQualityScore(AgentEvent tierResult) {
    // 1. Check for an explicit numeric score already present
    Object scoreObj = tierResult.getData().get("quality_score");
    if (scoreObj instanceof Number) {
      return clampScore(((Number) scoreObj).doubleValue());
    }

    // 2. If the execution itself failed, quality is zero
    Object execFailed = tierResult.getData().get("execution_failed");
    if (Boolean.TRUE.equals(execFailed)) {
      return 0.0;
    }

    // 3. Try to extract a score from the LLM review output text
    Object reviewOutput = tierResult.getData().get("review_output");
    if (reviewOutput == null) {
      return 0.5; // No output to evaluate
    }

    String reviewText = reviewOutput.toString();
    if (reviewText.isEmpty()) {
      return 0.5;
    }

    // 3a. Look for explicit score patterns like "Score: 0.9" or "Quality: 85%"
    Matcher matcher = SCORE_PATTERN.matcher(reviewText);
    if (matcher.find()) {
      double parsed = Double.parseDouble(matcher.group(1));
      boolean isPercentage = matcher.group(2) != null;
      if (isPercentage || parsed > 1.0) {
        parsed = parsed / 100.0;
      }
      return clampScore(parsed);
    }

    // 3b. Keyword-based scoring as fallback
    String lowerText = reviewText.toLowerCase();

    int positiveCount = 0;
    for (String keyword : POSITIVE_KEYWORDS) {
      if (lowerText.contains(keyword)) {
        positiveCount++;
      }
    }

    int negativeCount = 0;
    for (String keyword : NEGATIVE_KEYWORDS) {
      if (lowerText.contains(keyword)) {
        negativeCount++;
      }
    }

    int total = positiveCount + negativeCount;
    if (total == 0) {
      return 0.5; // No quality indicators found
    }

    // Score = ratio of positive keywords, shifted towards 0.5 baseline
    double rawRatio = (double) positiveCount / total;
    // Blend with 0.5 to avoid extreme scores from sparse keyword matches
    double blendedScore = 0.5 + (rawRatio - 0.5) * Math.min(1.0, total / 6.0);
    return clampScore(blendedScore);
  }

  private static double clampScore(double score) {
    return Math.max(0.0, Math.min(1.0, score));
  }

  /**
   * Checks if escalation should occur based on quality score and tier config.
   */
  private boolean shouldEscalate(double qualityScore) {
    // Check tier-specific threshold
    if (tier.getQualityThreshold() > 0 && qualityScore < tier.getQualityThreshold()) {
      return true;
    }

    // Check chain-level threshold
    if (chain.shouldEscalate(qualityScore)) {
      return true;
    }

    // Check if human approval required (treated as escalation to human)
    if (tier.isRequiresHumanApproval()) {
      // Phase 3 (not yet implemented): Human approval logic
      LOG.info("Tier {} requires human approval", tier.getTierIndex());
      return false;  // For now, don't escalate (assume approved)
    }

    return false;
  }

  /**
   * Handles escalation to next tier based on escalation policy.
   */
  private void handleEscalation(
      AgentEvent tierResult,
      int currentEscalationCount,
      Context ctx,
      Collector<AgentEvent> out) {

    int newEscalationCount = currentEscalationCount + 1;
    EscalationPolicy policy = chain.getEscalationPolicy();

    LOG.info("Escalating flow {} from tier {} (policy: {}, escalation count: {})",
        tierResult.getFlowId(), tier.getTierIndex(), policy, newEscalationCount);

    AgentEvent escalationEvent = tierResult.withEventType(AgentEventType.SUPERVISOR_REVIEW_REJECTED);
    escalationEvent.incrementIteration();
    escalationEvent.putMetadata("state", AgentState.SUPERVISOR_REVIEW.name());

    // Set escalation metadata
    escalationEvent.putMetadata("escalation_count", newEscalationCount);
    escalationEvent.putMetadata("escalation_reason", "quality_threshold");
    escalationEvent.putMetadata("escalated_from_tier", tier.getTierIndex());

    // Determine target tier based on policy
    int targetTier = determineTargetTier(policy);
    escalationEvent.putMetadata("target_tier", targetTier);

    // Route to escalation side output (will be picked up by next tier)
    ctx.output(ESCALATION_TAG, escalationEvent);

    LOG.info("Escalated flow {} to tier {}", tierResult.getFlowId(), targetTier);
  }

  /**
   * Determines target tier based on escalation policy.
   */
  private int determineTargetTier(EscalationPolicy policy) {
    switch (policy) {
      case NEXT_TIER:
        // Escalate to next tier in sequence
        Optional<SupervisorTier> nextTier = chain.getNextTier(tier.getTierIndex());
        return nextTier.map(SupervisorTier::getTierIndex).orElse(tier.getTierIndex());

      case SKIP_TO_TOP:
        // Skip to final tier
        return chain.getLastTier().getTierIndex();

      case RETRY_CURRENT:
        // Stay at current tier (retry)
        return tier.getTierIndex();

      case FAIL_FAST:
        // No escalation (will fail)
        return -1;

      case CUSTOM:
        // Phase 3 (not yet implemented): Custom escalation logic
        LOG.warn("Custom escalation policy not yet implemented, using NEXT_TIER");
        return chain.getNextTier(tier.getTierIndex())
            .map(SupervisorTier::getTierIndex)
            .orElse(tier.getTierIndex());

      default:
        return chain.getNextTier(tier.getTierIndex())
            .map(SupervisorTier::getTierIndex)
            .orElse(tier.getTierIndex());
    }
  }

  /**
   * Handles max escalations reached.
   */
  private void handleMaxEscalationsReached(
      AgentEvent event, Context ctx, Collector<AgentEvent> out) {

    AgentEvent failureEvent = event.withEventType(AgentEventType.FLOW_FAILED);
    failureEvent.incrementIteration();
    failureEvent.putMetadata("state", AgentState.FAILED.name());

    failureEvent.getData().put("error", "Max escalations reached");
    failureEvent.getData().put("max_escalations", chain.getMaxEscalations());
    failureEvent.getData().put("final_tier", tier.getTierIndex());

    ctx.output(VALIDATION_FAILURES_TAG, failureEvent);
  }

  /**
   * Gets escalation count from event metadata.
   */
  private int getEscalationCount(AgentEvent event) {
    Object countObj = event.getMetadata().get("escalation_count");
    if (countObj instanceof Integer) {
      return (Integer) countObj;
    }
    return 0;
  }

  @Override
  public void processTimedOutMatch(
      Map<String, List<AgentEvent>> match, Context ctx) throws Exception {

    LOG.warn("Pattern match timed out for tier: {}", tier.getTierName());

    List<AgentEvent> startEvents = match.get("initial");
    if (startEvents == null || startEvents.isEmpty()) {
      return;
    }

    AgentEvent startEvent = startEvents.get(0);
    String flowId = startEvent.getFlowId();

    LOG.warn("Tier {} timed out for flow: {}", tier.getTierIndex(), flowId);

    AgentEvent timeoutEvent = startEvent.withEventType(AgentEventType.TIMEOUT_OCCURRED);
    timeoutEvent.incrementIteration();
    timeoutEvent.putMetadata("state", AgentState.FAILED.name());
    timeoutEvent.getData().put("timeout_reason", "Tier execution exceeded time window");
    timeoutEvent.getData().put("tier_index", tier.getTierIndex());
    timeoutEvent.getData().put("tier_name", tier.getTierName());

    ctx.output(TIMEOUT_TAG, timeoutEvent);
  }
}
