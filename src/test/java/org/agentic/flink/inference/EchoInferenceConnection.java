package org.agentic.flink.inference;

import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingSetup;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Deterministic stub {@link InferenceConnection} for tests.
 *
 * <p>Echoes scripted outputs from each task surface so tests can exercise the wiring without a
 * real model. Default labels: {@code "safe"} for classification, {@code 0.5} for scoring, a
 * zero-vector for embedding, and an empty map for the generic task. Callers override per-test.
 */
public final class EchoInferenceConnection implements InferenceConnection {
  private static final long serialVersionUID = 1L;

  private final String classifierLabel;
  private final double classifierScore;
  private final Map<String, Double> classifierProbabilities;
  private final double scorerScore;
  private final int embeddingDimension;

  public EchoInferenceConnection() {
    this("safe", 1.0, Map.of("safe", 1.0), 0.5, 8);
  }

  public EchoInferenceConnection(
      String classifierLabel,
      double classifierScore,
      Map<String, Double> classifierProbabilities,
      double scorerScore,
      int embeddingDimension) {
    this.classifierLabel = classifierLabel;
    this.classifierScore = classifierScore;
    this.classifierProbabilities =
        classifierProbabilities == null ? Map.of() : new LinkedHashMap<>(classifierProbabilities);
    this.scorerScore = scorerScore;
    this.embeddingDimension = embeddingDimension;
  }

  public static EchoInferenceConnection withLabel(String label) {
    return new EchoInferenceConnection(label, 1.0, Map.of(label, 1.0), 0.5, 8);
  }

  @Override
  public InferenceClient bind(RuntimeContext runtimeContext) {
    return new Client();
  }

  @Override
  public String providerName() {
    return "echo";
  }

  /** Live client. Captures the spec's outputs and serves every task surface. */
  private final class Client implements InferenceClient {
    @Override
    public boolean supports(TaskKind kind) {
      return true;
    }

    @Override
    public Classifier asClassifier() {
      return (input, setup) ->
          new ClassificationResult(
              classifierLabel, classifierScore, classifierProbabilities);
    }

    @Override
    public Scorer asScorer() {
      return (input, setup) -> scorerScore;
    }

    @Override
    public EmbeddingClient asEmbedder() {
      return new EmbeddingClient() {
        @Override
        public float[] embed(String text, EmbeddingSetup setup) {
          float[] out = new float[embeddingDimension];
          // Cheap deterministic content: hashCode → first slot. Keeps tests reproducible.
          out[0] = text == null ? 0f : (float) (text.hashCode() & 0xFF) / 255f;
          return out;
        }

        @Override
        public String providerName() {
          return "echo";
        }
      };
    }

    @Override
    public GenericInferenceModel asGeneric() {
      return (input, setup) -> {
        Map<String, Object> echoed = new HashMap<>(input == null ? Map.of() : input);
        echoed.put("_echo", true);
        return echoed;
      };
    }

    @Override
    public String providerName() {
      return "echo";
    }
  }
}
