package org.agentic.flink.example.banking.safety;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.agentic.flink.screening.Detector;
import org.agentic.flink.screening.Phase;
import org.agentic.flink.screening.ScreenContext;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.Signal;

/**
 * Classic keyword/lexicon screening for the threats that target a customer-service agent:
 * prompt-injection, identity-verification bypass, impersonation/social-engineering, and bulk
 * data-exfiltration. A {@link Detector} for use in a {@link org.agentic.flink.screening.ScreeningPipeline}.
 *
 * <p>Each category that matches the message text contributes {@code categoryWeight} to the combined
 * risk; the pipeline's review/block thresholds then turn one-vs-many matches into ALLOW / REVIEW /
 * BLOCK. Cheap, deterministic, and runs before the message ever reaches the LLM.
 */
public final class InjectionDetector implements Detector {
  private static final long serialVersionUID = 1L;

  private static final Map<String, List<String>> CATEGORIES = buildCategories();

  private final double categoryWeight;

  public InjectionDetector() {
    this(0.45);
  }

  public InjectionDetector(double categoryWeight) {
    this.categoryWeight = categoryWeight;
  }

  @Override
  public Signal inspect(ScreenItem item, ScreenContext ctx) {
    String text = item.label() == null ? "" : item.label().toLowerCase(Locale.ROOT);
    if (text.isBlank()) {
      return null;
    }
    StringBuilder hits = new StringBuilder();
    int matched = 0;
    for (Map.Entry<String, List<String>> e : CATEGORIES.entrySet()) {
      for (String phrase : e.getValue()) {
        if (text.contains(phrase)) {
          matched++;
          if (hits.length() > 0) {
            hits.append(", ");
          }
          hits.append(e.getKey());
          break; // one hit per category
        }
      }
    }
    if (matched == 0) {
      return null;
    }
    return new Signal(
        name(),
        Phase.LEXICON,
        categoryWeight * matched,
        "matched threat categories: " + hits);
  }

  @Override
  public String name() {
    return "injection";
  }

  private static Map<String, List<String>> buildCategories() {
    Map<String, List<String>> m = new LinkedHashMap<>();
    m.put(
        "prompt-injection",
        List.of(
            "ignore previous", "ignore all previous", "disregard the instructions",
            "disregard your instructions", "you are now", "new instructions:",
            "system prompt", "reveal your instructions", "print your instructions",
            "repeat the words above"));
    m.put(
        "identity-bypass",
        List.of(
            "skip verification", "bypass verification", "no need to verify",
            "don't verify", "do not verify", "without verifying", "skip the security"));
    m.put(
        "impersonation",
        List.of(
            "i am the bank", "i am an administrator", "i am your developer",
            "as your developer", "this is an emergency, skip", "i work for rho-bank",
            "override authorization"));
    m.put(
        "data-exfiltration",
        List.of(
            "list all customers", "all account numbers", "every account",
            "dump the database", "everyone's balance", "all customer data"));
    return m;
  }
}
