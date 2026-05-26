package org.agentic.flink.langchain.store;

import dev.langchain4j.data.segment.TextSegment;
import java.io.Serializable;
import java.util.Map;

public interface LangChainEmbeddingStore extends Serializable {

  LangChainEmbeddingStore DEFAULT_MODEL = new DefaultEmbeddingStore();

  dev.langchain4j.store.embedding.EmbeddingStore<TextSegment> getStore(
      Map<String, String> properties);

  EmbeddingStore getName();
}
