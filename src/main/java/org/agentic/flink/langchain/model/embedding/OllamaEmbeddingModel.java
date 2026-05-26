package org.agentic.flink.langchain.model.embedding;

import org.agentic.flink.langchain.model.AiModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import java.util.Map;

public class OllamaEmbeddingModel implements LangChainEmbeddingModel {

  @Override
  public EmbeddingModel getModel(final Map<String, String> properties) {
    return dev.langchain4j.model.ollama.OllamaEmbeddingModel.builder()
        .baseUrl(properties.get("baseUrl"))
        .modelName(properties.get("modelName"))
        .build();
  }

  @Override
  public AiModel getName() {
    return AiModel.OLLAMA;
  }
}
