package org.agentic.flink.langchain.store;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore;
import java.util.Map;

public class DefaultEmbeddingStore implements LangChainEmbeddingStore {

  private static InMemoryEmbeddingStore<TextSegment> store;

  @Override
  public dev.langchain4j.store.embedding.EmbeddingStore<TextSegment> getStore(
      final Map<String, String> properties) {
    if (store == null) {
      store = new InMemoryEmbeddingStore<>();
    }
    return store;
  }

  @Override
  public EmbeddingStore getName() {
    return EmbeddingStore.DEFAULT;
  }
}
