package org.agentic.flink.example.banking.safety;

import java.util.Objects;
import org.agentic.flink.screening.RepeatDetector;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.ScreeningPipeline;
import org.agentic.flink.screening.ScreeningResult;
import org.agentic.flink.screening.VelocityDetector;

/**
 * Inbound threat screening for the banking agents, built on the framework's {@link
 * ScreeningPipeline}. Layers three cheap, deterministic rule detectors keyed by A2A {@code
 * contextId}:
 *
 * <ul>
 *   <li>{@link InjectionDetector} — prompt-injection / identity-bypass / impersonation /
 *       data-exfiltration lexicon;
 *   <li>{@link RepeatDetector} — the same message text repeated (a stuck/echo loop between agents);
 *   <li>{@link VelocityDetector} — too many messages too fast (flooding / abuse).
 * </ul>
 *
 * <p>Combined risk maps to ALLOW / REVIEW / BLOCK via the pipeline thresholds. {@code BLOCK} → the
 * message never reaches the LLM (safe refusal); {@code REVIEW} → the escalation path. History is
 * per-{@code contextId}, so concurrent sessions stay isolated.
 */
public final class BankingScreening {

  private final ScreeningPipeline pipeline;

  private BankingScreening(ScreeningPipeline pipeline) {
    this.pipeline = pipeline;
  }

  /** Default banking screening: one injection category → REVIEW, two (or injection+repeat) → BLOCK. */
  public static BankingScreening defaults() {
    ScreeningPipeline pipeline =
        ScreeningPipeline.builder()
            .addDetector(new InjectionDetector(0.45))
            .addDetector(
                new RepeatDetector(
                    3, 0.5, (a, b) -> Objects.equals(a.label(), b.label())))
            .addDetector(new VelocityDetector(6, java.time.Duration.ofSeconds(20), 0.4))
            .withReviewThreshold(0.45)
            .withBlockThreshold(0.85)
            .withHistory(10_000, 16)
            .build();
    return new BankingScreening(pipeline);
  }

  /** Wrap an already-configured pipeline (e.g. one with an ML classifier or LLM tier). */
  public static BankingScreening of(ScreeningPipeline pipeline) {
    return new BankingScreening(pipeline);
  }

  /**
   * Screen one inbound message for a session. {@code nowEpochMs} drives the velocity window (pass
   * the event time, never wall-clock, for determinism).
   */
  public ScreeningResult screen(String contextId, String messageText, long nowEpochMs) {
    return pipeline.screen(ScreenItem.of(contextId, 0.0, messageText, nowEpochMs));
  }
}
