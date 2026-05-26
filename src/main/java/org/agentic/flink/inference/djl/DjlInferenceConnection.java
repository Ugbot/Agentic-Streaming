package org.agentic.flink.inference.djl;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.huggingface.translator.TextClassificationTranslatorFactory;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.inference.ClassificationResult;
import org.agentic.flink.inference.Classifier;
import org.agentic.flink.inference.GenericInferenceModel;
import org.agentic.flink.inference.InferenceClient;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceModelCache;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Scorer;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default DJL-backed {@link InferenceConnection}.
 *
 * <p>Supports two task surfaces directly through DJL's HuggingFace translator factories:
 *
 * <ul>
 *   <li>{@link InferenceClient.TaskKind#CLASSIFIER} via {@code TextClassificationTranslatorFactory}
 *   <li>{@link InferenceClient.TaskKind#EMBEDDER} via {@code TextEmbeddingTranslatorFactory}
 * </ul>
 *
 * <p>{@link InferenceClient.TaskKind#SCORER} reuses the classifier — a binary classifier's
 * positive-class probability is a usable score for ranking and quality estimation. Generic
 * inference is not supported by this backend; callers wanting arbitrary tensors should plug an
 * ONNX or PyTorch backend directly.
 *
 * <p>The connection is serializable and ships in the job graph. The actual {@code ZooModel}
 * loads lazily inside {@link #bind} through {@link InferenceModelCache} so multiple operators in
 * the same task slot share weights.
 */
public final class DjlInferenceConnection implements InferenceConnection {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(DjlInferenceConnection.class);

  /**
   * Which task surface this connection serves. DJL needs to know up front so the right
   * translator factory wires into the criteria.
   */
  public enum Task {
    CLASSIFICATION,
    EMBEDDING
  }

  private final Task task;
  private final String defaultModelUri;
  private final String engine;

  /** No-arg constructor for {@link java.util.ServiceLoader} — defaults to PyTorch + classification. */
  public DjlInferenceConnection() {
    this(Task.CLASSIFICATION, null, "PyTorch");
  }

  public DjlInferenceConnection(Task task, String defaultModelUri, String engine) {
    this.task = Objects.requireNonNull(task, "task");
    this.defaultModelUri = defaultModelUri;
    this.engine = engine == null ? "PyTorch" : engine;
  }

  /** Build a classification connection over the given model URI. */
  public static DjlInferenceConnection classification(String modelUri) {
    return new DjlInferenceConnection(Task.CLASSIFICATION, modelUri, "PyTorch");
  }

  /** Build a text-embedding connection over the given model URI. */
  public static DjlInferenceConnection embedding(String modelUri) {
    return new DjlInferenceConnection(Task.EMBEDDING, modelUri, "PyTorch");
  }

  public Task getTask() {
    return task;
  }

  public String getDefaultModelUri() {
    return defaultModelUri;
  }

  public String getEngine() {
    return engine;
  }

  @Override
  public InferenceClient bind(RuntimeContext runtimeContext) {
    return new DjlClient();
  }

  @Override
  public String providerName() {
    return "djl:" + task.name().toLowerCase() + ":" + engine.toLowerCase();
  }

  /** Runtime client. Lazily loads ZooModels on first task-typed call. */
  private final class DjlClient implements InferenceClient {

    @Override
    public boolean supports(TaskKind kind) {
      switch (kind) {
        case CLASSIFIER:
        case SCORER:
          return task == Task.CLASSIFICATION;
        case EMBEDDER:
          return task == Task.EMBEDDING;
        case GENERIC:
          return false;
        default:
          return false;
      }
    }

    @Override
    public Classifier asClassifier() {
      if (!supports(TaskKind.CLASSIFIER)) {
        throw new UnsupportedOperationException(
            "DjlInferenceConnection task=" + task + " does not support classification");
      }
      return (input, setup) -> {
        long started = System.nanoTime();
        try {
          ZooModel<String, Classifications> model = classificationModel(setup);
          try (Predictor<String, Classifications> p = model.newPredictor()) {
            Classifications res = p.predict(input);
            Map<String, Double> probabilities = new LinkedHashMap<>();
            for (Classifications.Classification c : res.items()) {
              probabilities.put(c.getClassName(), c.getProbability());
            }
            Classifications.Classification best = res.best();
            return new ClassificationResult(
                best.getClassName(), best.getProbability(), probabilities);
          }
        } catch (TranslateException e) {
          throw new RuntimeException("DJL classification failed: " + e.getMessage(), e);
        } finally {
          LOG.debug(
              "djl.classify model={} durationMs={}",
              setup.getModelName(), (System.nanoTime() - started) / 1_000_000);
        }
      };
    }

    @Override
    public Scorer asScorer() {
      if (!supports(TaskKind.SCORER)) {
        throw new UnsupportedOperationException(
            "DjlInferenceConnection task=" + task + " does not support scoring");
      }
      Classifier classifier = asClassifier();
      return (input, setup) -> {
        ClassificationResult result = classifier.classify(input, setup);
        // For a binary classifier, the score is the positive-class probability.
        // For a multi-class classifier, the best-class probability is a usable signal.
        return result.getScore();
      };
    }

    @Override
    public EmbeddingClient asEmbedder() {
      if (!supports(TaskKind.EMBEDDER)) {
        throw new UnsupportedOperationException(
            "DjlInferenceConnection task=" + task + " does not support embedding");
      }
      return new EmbeddingClient() {
        @Override
        public float[] embed(String text, EmbeddingSetup esetup) {
          long started = System.nanoTime();
          try {
            // The InferenceSetup's modelUri lives on the InferenceConnection's default for the
            // embedder path — we don't get one per call here. Build a one-off InferenceSetup
            // that wires the embedding model URI in.
            InferenceSetup isetup =
                InferenceSetup.builder()
                    .withModelName(esetup.getModelName())
                    .withModelUri(
                        defaultModelUri == null ? esetup.getModelName() : defaultModelUri)
                    .build();
            ZooModel<String, float[]> model = embeddingModel(isetup);
            try (Predictor<String, float[]> p = model.newPredictor()) {
              float[] vec = p.predict(text);
              if (esetup.shouldNormalize()) {
                normalize(vec);
              }
              return vec;
            }
          } catch (TranslateException e) {
            throw new RuntimeException("DJL embedding failed: " + e.getMessage(), e);
          } finally {
            LOG.debug(
                "djl.embed model={} durationMs={}",
                esetup.getModelName(), (System.nanoTime() - started) / 1_000_000);
          }
        }

        @Override
        public String providerName() {
          return "djl:embedding";
        }
      };
    }

    @Override
    public GenericInferenceModel asGeneric() {
      throw new UnsupportedOperationException(
          "DjlInferenceConnection does not implement the generic task surface. "
              + "Use a backend-specific connection (ONNX, PyTorch) for raw tensor I/O.");
    }

    @Override
    public String providerName() {
      return DjlInferenceConnection.this.providerName();
    }

    private ZooModel<String, Classifications> classificationModel(InferenceSetup setup) {
      String uri = resolveUri(setup);
      return InferenceModelCache.global()
          .computeIfAbsent(
              uri,
              setup.getDeviceType().name(),
              () -> {
                try {
                  Criteria<String, Classifications> criteria =
                      Criteria.builder()
                          .setTypes(String.class, Classifications.class)
                          .optModelUrls(uri)
                          .optEngine(engine)
                          .optDevice(resolveDevice(setup))
                          .optTranslatorFactory(new TextClassificationTranslatorFactory())
                          .build();
                  return criteria.loadModel();
                } catch (IOException | ModelNotFoundException | MalformedModelException e) {
                  throw new RuntimeException(
                      "Failed to load DJL classification model from " + uri + ": " + e.getMessage(),
                      e);
                }
              });
    }

    private ZooModel<String, float[]> embeddingModel(InferenceSetup setup) {
      String uri = resolveUri(setup);
      return InferenceModelCache.global()
          .computeIfAbsent(
              uri,
              setup.getDeviceType().name(),
              () -> {
                try {
                  Criteria<String, float[]> criteria =
                      Criteria.builder()
                          .setTypes(String.class, float[].class)
                          .optModelUrls(uri)
                          .optEngine(engine)
                          .optDevice(resolveDevice(setup))
                          .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                          .build();
                  return criteria.loadModel();
                } catch (IOException | ModelNotFoundException | MalformedModelException e) {
                  throw new RuntimeException(
                      "Failed to load DJL embedding model from " + uri + ": " + e.getMessage(),
                      e);
                }
              });
    }

    private String resolveUri(InferenceSetup setup) {
      if (setup != null && setup.getModelUri() != null && !setup.getModelUri().isEmpty()) {
        return setup.getModelUri();
      }
      if (defaultModelUri != null && !defaultModelUri.isEmpty()) {
        return defaultModelUri;
      }
      throw new IllegalStateException(
          "DJL inference requires either a setup.modelUri or a default URI on the connection");
    }

    private Device resolveDevice(InferenceSetup setup) {
      if (setup == null) return Device.cpu();
      switch (setup.getDeviceType()) {
        case CPU:
          return Device.cpu();
        case CUDA:
          return Device.gpu();
        case MPS:
          // DJL doesn't expose MPS as a first-class Device; fall back to CPU and let the engine
          // pick. PyTorch will use MPS automatically on macOS if the native binary supports it.
          return Device.cpu();
        case AUTO:
        default:
          return null; // DJL picks the default device for the engine
      }
    }

    private static void normalize(float[] v) {
      double sum = 0.0;
      for (float f : v) sum += f * f;
      double norm = Math.sqrt(sum);
      if (norm == 0.0) return;
      for (int i = 0; i < v.length; i++) {
        v[i] = (float) (v[i] / norm);
      }
    }
  }
}
