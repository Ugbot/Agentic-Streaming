package org.agentic.flink.inference;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Per-call configuration for a model loaded by an {@link InferenceConnection}.
 *
 * <p>Mirrors {@link org.agentic.flink.llm.ChatSetup}: one {@link InferenceConnection}
 * can load many models, and each call selects which one to invoke via {@link #getModelName()}
 * and (when first used) where its weights live via {@link #getModelUri()}. All other fields tune
 * the inference run itself.
 *
 * <p>This class carries no live state. Backends serialize it into the Flink job graph; the
 * actual model handle is built lazily inside the corresponding {@link InferenceClient}.
 */
public final class InferenceSetup implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Where the model runs. */
  public enum DeviceType {
    CPU,
    CUDA,
    MPS,
    AUTO
  }

  private final String modelName;
  private final String modelUri;
  private final DeviceType deviceType;
  private final int threads;
  private final int maxBatchSize;
  private final List<String> warmupInputs;

  private InferenceSetup(Builder b) {
    this.modelName = Objects.requireNonNull(b.modelName, "modelName");
    this.modelUri = Objects.requireNonNull(b.modelUri, "modelUri");
    this.deviceType = b.deviceType == null ? DeviceType.AUTO : b.deviceType;
    if (b.threads <= 0) {
      throw new IllegalArgumentException("threads must be positive, got " + b.threads);
    }
    this.threads = b.threads;
    if (b.maxBatchSize <= 0) {
      throw new IllegalArgumentException(
          "maxBatchSize must be positive, got " + b.maxBatchSize);
    }
    this.maxBatchSize = b.maxBatchSize;
    this.warmupInputs =
        b.warmupInputs == null ? Collections.emptyList() : List.copyOf(b.warmupInputs);
  }

  public String getModelName() {
    return modelName;
  }

  public String getModelUri() {
    return modelUri;
  }

  public DeviceType getDeviceType() {
    return deviceType;
  }

  public int getThreads() {
    return threads;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public List<String> getWarmupInputs() {
    return warmupInputs;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .withModelName(modelName)
        .withModelUri(modelUri)
        .withDevice(deviceType)
        .withThreads(threads)
        .withMaxBatchSize(maxBatchSize)
        .withWarmupInputs(warmupInputs);
  }

  public static final class Builder {
    private String modelName;
    private String modelUri;
    private DeviceType deviceType = DeviceType.AUTO;
    private int threads = 1;
    private int maxBatchSize = 1;
    private List<String> warmupInputs;

    public Builder withModelName(String modelName) {
      this.modelName = modelName;
      return this;
    }

    public Builder withModelUri(String modelUri) {
      this.modelUri = modelUri;
      return this;
    }

    public Builder withDevice(DeviceType deviceType) {
      this.deviceType = deviceType;
      return this;
    }

    public Builder withThreads(int threads) {
      this.threads = threads;
      return this;
    }

    public Builder withMaxBatchSize(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public Builder withWarmupInputs(List<String> warmupInputs) {
      this.warmupInputs = warmupInputs;
      return this;
    }

    public InferenceSetup build() {
      return new InferenceSetup(this);
    }
  }
}
