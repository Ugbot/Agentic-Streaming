package org.agentic.flink.langchain.model.embedding;

import org.agentic.flink.langchain.model.AiModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import java.io.Serializable;
import java.util.Map;

public interface LangChainEmbeddingModel extends Serializable {

  LangChainEmbeddingModel DEFAULT_MODEL = new DefaultEmbeddingModel();

  EmbeddingModel getModel(Map<String, String> properties);

  AiModel getName();
}
