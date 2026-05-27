package org.agentic.flink.inference;

import org.agentic.flink.embedding.EmbeddingClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Deterministic, zero-infrastructure {@link InferenceConnection} that classifies text by weighted
 * lexicon matching — a lightweight stand-in for a trained classifier.
 *
 * <p>It is NOT a neural model: it scores "suspiciousness" by summing the weights of configured
 * terms found in the input and squashing to {@code [0,1]}. It exists so a cascade (cheap filter →
 * ML classifier → LLM) can run end-to-end with no model server. Swap in
 * {@code DjlInferenceConnection} for a real ONNX/PyTorch classifier — the {@link Classifier} SPI
 * is identical.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; provider name {@code "lexicon"}. The default
 * lexicon targets phishing/fraud signals.
 */
public final class LexiconInferenceConnection implements InferenceConnection {
  private static final long serialVersionUID = 1L;

  /** Default phishing/fraud lexicon: term (lower-case) -> weight. */
  private static final Map<String, Double> DEFAULT_LEXICON = defaultLexicon();

  private final Map<String, Double> lexicon;
  private final String positiveLabel;
  private final String negativeLabel;

  public LexiconInferenceConnection() {
    this(DEFAULT_LEXICON, "SUSPICIOUS", "CLEAN");
  }

  public LexiconInferenceConnection(
      Map<String, Double> lexicon, String positiveLabel, String negativeLabel) {
    this.lexicon = lexicon == null ? DEFAULT_LEXICON : new LinkedHashMap<>(lexicon);
    this.positiveLabel = positiveLabel;
    this.negativeLabel = negativeLabel;
  }

  private static Map<String, Double> defaultLexicon() {
    Map<String, Double> m = new LinkedHashMap<>();
    m.put("wire transfer", 0.7);
    m.put("gift card", 0.8);
    m.put("bitcoin", 0.6);
    m.put("crypto", 0.4);
    m.put("urgent", 0.4);
    m.put("verify your account", 0.8);
    m.put("password", 0.5);
    m.put("ssn", 0.7);
    m.put("social security", 0.7);
    m.put("routing number", 0.7);
    m.put("click this link", 0.6);
    m.put("suspended", 0.4);
    m.put("act now", 0.5);
    m.put("limited time", 0.3);
    m.put("refund", 0.3);
    return m;
  }

  @Override
  public InferenceClient bind(RuntimeContext runtimeContext) {
    return new Client();
  }

  @Override
  public String providerName() {
    return "lexicon";
  }

  /** Stateless lexicon scorer serving the classifier + scorer task surfaces. */
  private final class Client implements InferenceClient {
    @Override
    public boolean supports(TaskKind kind) {
      return kind == TaskKind.CLASSIFIER || kind == TaskKind.SCORER;
    }

    @Override
    public Classifier asClassifier() {
      return (input, setup) -> {
        double s = suspicion(input);
        boolean positive = s >= 0.5;
        Map<String, Double> probs = new LinkedHashMap<>();
        probs.put(positiveLabel, s);
        probs.put(negativeLabel, 1.0 - s);
        return new ClassificationResult(positive ? positiveLabel : negativeLabel, s, probs);
      };
    }

    @Override
    public Scorer asScorer() {
      return (input, setup) -> suspicion(input);
    }

    @Override
    public EmbeddingClient asEmbedder() {
      throw new UnsupportedOperationException("LexiconInferenceConnection does not embed");
    }

    @Override
    public GenericInferenceModel asGeneric() {
      throw new UnsupportedOperationException("LexiconInferenceConnection has no generic task");
    }

    @Override
    public String providerName() {
      return "lexicon";
    }
  }

  /** Sum matched term weights, squash to [0,1) via 1 - e^-x. Deterministic. */
  private double suspicion(String input) {
    if (input == null || input.isBlank()) return 0.0;
    String text = input.toLowerCase();
    double sum = 0.0;
    for (Map.Entry<String, Double> e : lexicon.entrySet()) {
      if (text.contains(e.getKey())) {
        sum += e.getValue();
      }
    }
    return 1.0 - Math.exp(-sum);
  }

  // Kept for callers that want to pre-tokenize; not used by the simple contains() path above.
  static final Pattern WORD = Pattern.compile("\\w+");

  /** Expose the default lexicon terms (e.g. to build a cheap pre-filter). */
  public static List<String> defaultTerms() {
    return List.copyOf(DEFAULT_LEXICON.keySet());
  }
}
