package org.agentic.flink.cep;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.statemachine.AgentState;
import java.time.Duration;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * Fluent builder for creating Apache Flink CEP patterns for agent workflows.
 *
 * <p>This class provides a declarative DSL for building CEP patterns, inspired by the Saga kit's
 * pattern builder but tailored for agent workflows. It simplifies the creation of complex
 * event-driven state machines.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * Pattern<AgentEvent, ?> pattern = CepPatternBuilder.create()
 *     .start("init")
 *         .matching(AgentEventType.FLOW_STARTED)
 *     .followedBy("validate")
 *         .matching(AgentEventType.VALIDATION_PASSED, AgentEventType.VALIDATION_FAILED)
 *         .where(event -> event.getData().containsKey("validation_result"))
 *     .next("execute")
 *         .matching(AgentEventType.TOOL_CALL_COMPLETED)
 *         .oneOrMore()
 *     .end("complete")
 *         .matching(AgentEventType.FLOW_COMPLETED)
 *     .within(Duration.ofSeconds(30))
 *     .build();
 * }</pre>
 *
 * <p><b>Pattern Semantics Borrowed from Saga Kit:</b>
 * <ul>
 *   <li><b>.next()</b> - Strict contiguity: no events can occur between</li>
 *   <li><b>.followedBy()</b> - Relaxed contiguity: other events allowed between</li>
 *   <li><b>.followedByAny()</b> - Non-deterministic relaxed contiguity</li>
 *   <li><b>.oneOrMore()</b> - Match pattern 1 or more times</li>
 *   <li><b>.optional()</b> - Pattern may or may not occur</li>
 *   <li><b>.greedy()</b> - Match as many events as possible</li>
 *   <li><b>.within()</b> - Time window constraint (like saga timeout)</li>
 * </ul>
 *
 * @author Agentic Flink Team
 * @see Pattern
 * @see PatternConditions
 */
public class CepPatternBuilder {

  private Pattern<AgentEvent, ?> pattern;
  private PatternStepBuilder currentStep;
  private Duration globalTimeout;

  private CepPatternBuilder() {
    // Private constructor, use create() factory method
  }

  /**
   * Creates a new CEP pattern builder.
   *
   * @return new builder instance
   */
  public static CepPatternBuilder create() {
    return new CepPatternBuilder();
  }

  /**
   * Starts the pattern with an initial step.
   *
   * @param stepName Name of the first pattern step
   * @return step builder for configuring this step
   */
  public PatternStepBuilder start(String stepName) {
    this.currentStep = new PatternStepBuilder(this, stepName, StepType.START);
    return currentStep;
  }

  /**
   * Adds a step with strict contiguity (next).
   *
   * <p>No events can occur between the previous step and this one.
   *
   * @param stepName Name of this pattern step
   * @return step builder for configuring this step
   */
  public PatternStepBuilder next(String stepName) {
    if (pattern == null) {
      throw new IllegalStateException("Must call start() before next()");
    }
    this.currentStep = new PatternStepBuilder(this, stepName, StepType.NEXT);
    return currentStep;
  }

  /**
   * Adds a step with relaxed contiguity (followedBy).
   *
   * <p>Other events are allowed to occur between the previous step and this one.
   *
   * @param stepName Name of this pattern step
   * @return step builder for configuring this step
   */
  public PatternStepBuilder followedBy(String stepName) {
    if (pattern == null) {
      throw new IllegalStateException("Must call start() before followedBy()");
    }
    this.currentStep = new PatternStepBuilder(this, stepName, StepType.FOLLOWED_BY);
    return currentStep;
  }

  /**
   * Adds a step with non-deterministic relaxed contiguity (followedByAny).
   *
   * <p>Like followedBy, but explores all possible matches (not just the first).
   *
   * @param stepName Name of this pattern step
   * @return step builder for configuring this step
   */
  public PatternStepBuilder followedByAny(String stepName) {
    if (pattern == null) {
      throw new IllegalStateException("Must call start() before followedByAny()");
    }
    this.currentStep = new PatternStepBuilder(this, stepName, StepType.FOLLOWED_BY_ANY);
    return currentStep;
  }

  /**
   * Sets a global timeout for the entire pattern (like saga's .within()).
   *
   * <p>If the pattern doesn't complete within this duration, a timeout event is triggered.
   *
   * @param timeout Maximum duration for pattern completion
   * @return this builder
   */
  public CepPatternBuilder within(Duration timeout) {
    this.globalTimeout = timeout;
    return this;
  }

  /**
   * Builds the final CEP pattern.
   *
   * @return Compiled Flink CEP pattern
   */
  public Pattern<AgentEvent, ?> build() {
    if (pattern == null) {
      throw new IllegalStateException("Pattern has no steps. Call start() first.");
    }

    // Apply global timeout if set
    if (globalTimeout != null) {
      pattern = pattern.within(globalTimeout);
    }

    return pattern;
  }

  /**
   * Builder for configuring individual pattern steps.
   */
  public class PatternStepBuilder {
    private final CepPatternBuilder parentBuilder;
    private final String stepName;
    private final StepType stepType;
    private SimpleCondition<AgentEvent> eventTypeCondition;
    private IterativeCondition<AgentEvent> additionalCondition;
    private boolean isOptional = false;
    private boolean isOneOrMore = false;
    private boolean isGreedy = false;
    private boolean isConsecutive = false;

    private PatternStepBuilder(CepPatternBuilder parent, String stepName, StepType stepType) {
      this.parentBuilder = parent;
      this.stepName = stepName;
      this.stepType = stepType;
    }

    /**
     * Specifies which event types this step should match.
     *
     * @param eventTypes One or more event types
     * @return this step builder
     */
    public PatternStepBuilder matching(AgentEventType... eventTypes) {
      this.eventTypeCondition = PatternConditions.eventTypeCondition(eventTypes);
      return this;
    }

    /**
     * Specifies which agent states this step should match.
     *
     * @param states One or more agent states
     * @return this step builder
     */
    public PatternStepBuilder inState(AgentState... states) {
      this.eventTypeCondition = PatternConditions.stateCondition(states);
      return this;
    }

    /**
     * Adds an additional condition (AND with event type condition).
     *
     * @param condition Additional condition to check
     * @return this step builder
     */
    public PatternStepBuilder where(IterativeCondition<AgentEvent> condition) {
      this.additionalCondition = condition;
      return this;
    }

    /**
     * Makes this step optional (may or may not occur).
     *
     * @return this step builder
     */
    public PatternStepBuilder optional() {
      this.isOptional = true;
      return this;
    }

    /**
     * Allows this step to match one or more times.
     *
     * @return this step builder
     */
    public PatternStepBuilder oneOrMore() {
      this.isOneOrMore = true;
      return this;
    }

    /**
     * Makes the pattern greedy (match as many events as possible).
     *
     * @return this step builder
     */
    public PatternStepBuilder greedy() {
      this.isGreedy = true;
      return this;
    }

    /**
     * Requires consecutive matching (no gaps for oneOrMore patterns).
     *
     * @return this step builder
     */
    public PatternStepBuilder consecutive() {
      this.isConsecutive = true;
      return this;
    }

    /**
     * Completes this step and returns to parent builder.
     *
     * @return parent CEP pattern builder
     */
    public CepPatternBuilder end(String stepName) {
      // First, finalize the current step
      finalizeStep();

      // Now add the terminal step
      Pattern<AgentEvent, ?> terminalPattern;
      SimpleCondition<AgentEvent> terminalCondition = new SimpleCondition<AgentEvent>() {
        @Override
        public boolean filter(AgentEvent event) throws Exception {
          return event.getEventType() == AgentEventType.FLOW_COMPLETED
              || event.getEventType() == AgentEventType.FLOW_FAILED
              || event.getEventType() == AgentEventType.LOOP_MAX_ITERATIONS_REACHED;
        }
      };

      if (parentBuilder.pattern == null) {
        terminalPattern = Pattern.<AgentEvent>begin(stepName).where(terminalCondition);
      } else {
        terminalPattern = parentBuilder.pattern.followedBy(stepName).where(terminalCondition);
      }

      parentBuilder.pattern = terminalPattern;
      return parentBuilder;
    }

    /**
     * Adds the next pattern step with strict contiguity.
     *
     * @param stepName Name of the next step
     * @return step builder for the next step
     */
    public PatternStepBuilder next(String stepName) {
      finalizeStep();
      return parentBuilder.next(stepName);
    }

    /**
     * Adds the next pattern step with relaxed contiguity.
     *
     * @param stepName Name of the next step
     * @return step builder for the next step
     */
    public PatternStepBuilder followedBy(String stepName) {
      finalizeStep();
      return parentBuilder.followedBy(stepName);
    }

    /**
     * Adds the next pattern step with non-deterministic relaxed contiguity.
     *
     * @param stepName Name of the next step
     * @return step builder for the next step
     */
    public PatternStepBuilder followedByAny(String stepName) {
      finalizeStep();
      return parentBuilder.followedByAny(stepName);
    }

    /**
     * Sets global timeout and returns to parent builder.
     *
     * @param timeout Maximum duration for pattern
     * @return parent CEP pattern builder
     */
    public CepPatternBuilder within(Duration timeout) {
      finalizeStep();
      return parentBuilder.within(timeout);
    }

    /**
     * Builds the final pattern.
     *
     * @return Compiled Flink CEP pattern
     */
    public Pattern<AgentEvent, ?> build() {
      finalizeStep();
      return parentBuilder.build();
    }

    /**
     * Finalizes the current step and adds it to the parent pattern.
     */
    @SuppressWarnings("unchecked")
    private void finalizeStep() {
      Pattern<AgentEvent, ?> stepPattern;

      // Create the step based on type
      switch (stepType) {
        case START:
          stepPattern = Pattern.<AgentEvent>begin(stepName);
          break;
        case NEXT:
          stepPattern = parentBuilder.pattern.next(stepName);
          break;
        case FOLLOWED_BY:
          stepPattern = parentBuilder.pattern.followedBy(stepName);
          break;
        case FOLLOWED_BY_ANY:
          stepPattern = parentBuilder.pattern.followedByAny(stepName);
          break;
        default:
          throw new IllegalStateException("Unknown step type: " + stepType);
      }

      // Add event type condition (required)
      if (eventTypeCondition != null) {
        stepPattern = ((Pattern<AgentEvent, AgentEvent>) stepPattern).where(eventTypeCondition);
      }

      // Add additional condition if provided
      if (additionalCondition != null) {
        stepPattern = ((Pattern<AgentEvent, AgentEvent>) stepPattern).where(additionalCondition);
      }

      // Apply quantifiers
      if (isOneOrMore) {
        stepPattern = stepPattern.oneOrMore();
        if (isConsecutive) {
          stepPattern = stepPattern.consecutive();
        }
      }

      if (isGreedy) {
        stepPattern = stepPattern.greedy();
      }

      if (isOptional) {
        stepPattern = stepPattern.optional();
      }

      parentBuilder.pattern = stepPattern;
    }
  }

  /**
   * Enum for pattern step types (how they connect to previous step).
   */
  private enum StepType {
    START,
    NEXT,
    FOLLOWED_BY,
    FOLLOWED_BY_ANY
  }

  // ==================== Pre-configured Pattern Templates ====================

  /**
   * Creates a simple agent pattern: start → execute → complete.
   *
   * @param timeoutSeconds Timeout for the entire pattern
   * @return pre-configured simple agent pattern
   */
  public static Pattern<AgentEvent, ?> simpleAgentPattern(int timeoutSeconds) {
    return CepPatternBuilder.create()
        .start("start")
            .matching(AgentEventType.FLOW_STARTED)
        .followedBy("execute")
            .matching(AgentEventType.TOOL_CALL_COMPLETED)
        .end("complete")
        .within(Duration.ofSeconds(timeoutSeconds))
        .build();
  }

  /**
   * Creates a validated agent pattern: start → validate → execute → complete.
   *
   * @param timeoutSeconds Timeout for the entire pattern
   * @return pre-configured validated agent pattern
   */
  public static Pattern<AgentEvent, ?> validatedAgentPattern(int timeoutSeconds) {
    return CepPatternBuilder.create()
        .start("start")
            .matching(AgentEventType.FLOW_STARTED)
        .followedBy("validate")
            .matching(AgentEventType.VALIDATION_PASSED)
        .followedBy("execute")
            .matching(AgentEventType.TOOL_CALL_COMPLETED)
        .end("complete")
        .within(Duration.ofSeconds(timeoutSeconds))
        .build();
  }

  /**
   * Creates a supervised agent pattern: start → execute → supervisor → complete.
   *
   * @param timeoutSeconds Timeout for the entire pattern
   * @return pre-configured supervised agent pattern
   */
  public static Pattern<AgentEvent, ?> supervisedAgentPattern(int timeoutSeconds) {
    return CepPatternBuilder.create()
        .start("start")
            .matching(AgentEventType.FLOW_STARTED)
        .followedBy("execute")
            .matching(AgentEventType.TOOL_CALL_COMPLETED)
        .followedBy("supervisor")
            .matching(AgentEventType.SUPERVISOR_APPROVED)
        .end("complete")
        .within(Duration.ofSeconds(timeoutSeconds))
        .build();
  }

  /**
   * Creates a full agent pattern with validation, execution, correction, and supervision.
   *
   * @param timeoutSeconds Timeout for the entire pattern
   * @return pre-configured full-featured agent pattern
   */
  public static Pattern<AgentEvent, ?> fullAgentPattern(int timeoutSeconds) {
    return CepPatternBuilder.create()
        .start("start")
            .matching(AgentEventType.FLOW_STARTED)
        .followedBy("validate")
            .matching(AgentEventType.VALIDATION_PASSED, AgentEventType.VALIDATION_FAILED)
            .optional()
        .followedBy("execute")
            .matching(AgentEventType.TOOL_CALL_COMPLETED, AgentEventType.TOOL_CALL_FAILED)
            .oneOrMore()
        .followedBy("correct")
            .matching(AgentEventType.CORRECTION_COMPLETED)
            .optional()
            .oneOrMore()
        .followedBy("supervisor")
            .matching(AgentEventType.SUPERVISOR_APPROVED, AgentEventType.SUPERVISOR_REJECTED)
            .optional()
        .end("terminal")
        .within(Duration.ofSeconds(timeoutSeconds))
        .build();
  }
}
