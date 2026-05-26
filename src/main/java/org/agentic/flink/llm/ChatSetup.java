package org.agentic.flink.llm;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Per-agent chat configuration, decoupled from any vendor transport.
 *
 * <p>Carries the bits that change <i>between agents</i> against a single shared {@link
 * ChatConnection}: model name, temperature, response shape. One {@link ChatConnection} (one
 * Ollama service) can feed many agents with different {@link ChatSetup}s.
 */
public final class ChatSetup implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String modelName;
  private final double temperature;
  private final int maxResponseTokens;
  private final List<String> stopSequences;
  private final Long seed;
  private final OutputSchema<?> outputSchema;

  private ChatSetup(Builder b) {
    this.modelName = b.modelName;
    this.temperature = b.temperature;
    this.maxResponseTokens = b.maxResponseTokens;
    this.stopSequences =
        b.stopSequences == null ? Collections.emptyList() : List.copyOf(b.stopSequences);
    this.seed = b.seed;
    this.outputSchema = b.outputSchema;
  }

  public String getModelName() {
    return modelName;
  }

  public double getTemperature() {
    return temperature;
  }

  public int getMaxResponseTokens() {
    return maxResponseTokens;
  }

  public List<String> getStopSequences() {
    return stopSequences;
  }

  public Long getSeed() {
    return seed;
  }

  public OutputSchema<?> getOutputSchema() {
    return outputSchema;
  }

  public boolean hasOutputSchema() {
    return outputSchema != null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .withModel(modelName)
        .withTemperature(temperature)
        .withMaxResponseTokens(maxResponseTokens)
        .withStopSequences(stopSequences)
        .withSeed(seed)
        .withOutputSchema(outputSchema);
  }

  public static final class Builder {
    private String modelName;
    private double temperature = 0.7;
    private int maxResponseTokens = 1000;
    private List<String> stopSequences;
    private Long seed;
    private OutputSchema<?> outputSchema;

    public Builder withModel(String modelName) {
      this.modelName = modelName;
      return this;
    }

    public Builder withTemperature(double temperature) {
      this.temperature = temperature;
      return this;
    }

    public Builder withMaxResponseTokens(int maxResponseTokens) {
      this.maxResponseTokens = maxResponseTokens;
      return this;
    }

    public Builder withStopSequences(List<String> stopSequences) {
      this.stopSequences = stopSequences;
      return this;
    }

    public Builder withSeed(Long seed) {
      this.seed = seed;
      return this;
    }

    public Builder withOutputSchema(OutputSchema<?> outputSchema) {
      this.outputSchema = outputSchema;
      return this;
    }

    public ChatSetup build() {
      if (modelName == null || modelName.isEmpty()) {
        throw new IllegalStateException("modelName is required");
      }
      return new ChatSetup(this);
    }
  }
}
