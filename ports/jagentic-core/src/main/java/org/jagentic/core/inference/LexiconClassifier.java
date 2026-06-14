package org.jagentic.core.inference;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Keyword-weighted bag-of-words classifier: the score for a label is the fraction of the
 * text's tokens that hit that label's keyword set, normalised so per-label scores sum to 1.
 * Deterministic, offline, no model. */
public final class LexiconClassifier implements Classifier {

  private static final Pattern WORD = Pattern.compile("[a-z0-9']+");

  private final Map<String, Set<String>> lexicon;
  private final String defaultLabel;

  public LexiconClassifier(Map<String, List<String>> lexicon, String defaultLabel) {
    if (lexicon == null || lexicon.isEmpty()) {
      throw new IllegalArgumentException("lexicon must have at least one label");
    }
    this.lexicon = new LinkedHashMap<>();
    for (var e : lexicon.entrySet()) {
      this.lexicon.put(e.getKey(), e.getValue().stream().map(String::toLowerCase).collect(Collectors.toSet()));
    }
    this.defaultLabel = defaultLabel == null || defaultLabel.isBlank() ? "other" : defaultLabel;
  }

  private static List<String> tokens(String text) {
    List<String> out = new java.util.ArrayList<>();
    if (text != null) {
      Matcher m = WORD.matcher(text.toLowerCase());
      while (m.find()) {
        out.add(m.group());
      }
    }
    return out;
  }

  @Override
  public Classification classify(String text) {
    List<String> toks = tokens(text);
    Map<String, Double> scores = new HashMap<>();
    for (String label : lexicon.keySet()) {
      scores.put(label, 0.0);
    }
    if (toks.isEmpty()) {
      return new Classification(defaultLabel, 0.0, scores);
    }
    double n = toks.size();
    double total = 0.0;
    for (var e : lexicon.entrySet()) {
      long hits = toks.stream().filter(e.getValue()::contains).count();
      double s = hits / n;
      scores.put(e.getKey(), s);
      total += s;
    }
    if (total <= 0.0) {
      return new Classification(defaultLabel, 0.0, scores);
    }
    String best = defaultLabel;
    double bestScore = -1.0;
    for (var e : scores.entrySet()) {
      double norm = e.getValue() / total;
      scores.put(e.getKey(), norm);
      if (norm > bestScore) {
        bestScore = norm;
        best = e.getKey();
      }
    }
    return new Classification(best, bestScore, scores);
  }
}
