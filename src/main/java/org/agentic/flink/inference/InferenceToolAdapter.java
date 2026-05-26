package org.agentic.flink.inference;

import org.agentic.flink.tools.ToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Wraps a {@link Classifier} or {@link Scorer} surface of an {@link InferenceConnection} as a
 * {@link ToolExecutor} so the LLM can invoke an inference model through the regular tool-call
 * path.
 *
 * <p>The adapter expects the LLM's argument map to contain a {@code "text"} field for the model
 * input. For classifiers, the returned object is a map of {@code label}, {@code score}, and
 * {@code probabilities}. For scorers, it's a map containing {@code score}.
 *
 * <p>The {@link InferenceConnection} ships in the job graph; the live {@link InferenceClient} is
 * built lazily on the task side via {@link InferenceConnection#bind(RuntimeContext)}.
 */
public final class InferenceToolAdapter implements ToolExecutor {
  private static final long serialVersionUID = 1L;

  /** Which task surface this adapter calls. */
  public enum TaskKind {
    CLASSIFIER,
    SCORER
  }

  private final String toolId;
  private final String description;
  private final InferenceConnection connection;
  private final InferenceSetup setup;
  private final TaskKind kind;

  private transient InferenceClient client;

  public InferenceToolAdapter(
      String toolId,
      String description,
      InferenceConnection connection,
      InferenceSetup setup,
      TaskKind kind) {
    this.toolId = Objects.requireNonNull(toolId, "toolId");
    this.description = description == null ? toolId : description;
    this.connection = Objects.requireNonNull(connection, "connection");
    this.setup = Objects.requireNonNull(setup, "setup");
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(
        () -> {
          Object raw = parameters == null ? null : parameters.get("text");
          if (raw == null) {
            throw new IllegalArgumentException(
                "InferenceToolAdapter requires a 'text' parameter; got: " + parameters);
          }
          String text = raw.toString();
          InferenceClient c = client();
          Map<String, Object> result = new HashMap<>();
          switch (kind) {
            case CLASSIFIER:
              ClassificationResult cls = c.asClassifier().classify(text, setup);
              result.put("label", cls.getLabel());
              result.put("score", cls.getScore());
              result.put("probabilities", cls.getProbabilities());
              return result;
            case SCORER:
              double score = c.asScorer().score(text, setup);
              result.put("score", score);
              return result;
            default:
              throw new IllegalStateException("Unsupported task kind: " + kind);
          }
        });
  }

  @Override
  public String getToolId() {
    return toolId;
  }

  @Override
  public String getDescription() {
    return description;
  }

  private synchronized InferenceClient client() {
    if (client == null) {
      try {
        client = connection.bind(null);
      } catch (Exception e) {
        throw new RuntimeException(
            "InferenceToolAdapter failed to bind connection: " + e.getMessage(), e);
      }
    }
    return client;
  }

  public InferenceConnection getConnection() {
    return connection;
  }

  public InferenceSetup getSetup() {
    return setup;
  }

  public TaskKind getKind() {
    return kind;
  }
}
