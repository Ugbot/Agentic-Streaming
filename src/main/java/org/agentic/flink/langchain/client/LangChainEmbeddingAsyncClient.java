package org.agentic.flink.langchain.client;

import org.agentic.flink.langchain.model.embedding.LangChainEmbeddingModel;
import org.agentic.flink.langchain.store.EmbeddingConfig;
import org.agentic.flink.langchain.store.LangChainEmbeddingStore;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class LangChainEmbeddingAsyncClient implements Serializable {

  private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

  private final List<LangChainEmbeddingModel> supportedModels;
  private final List<LangChainEmbeddingStore> supportedStores;

  public LangChainEmbeddingAsyncClient(
      List<LangChainEmbeddingModel> supportedModels,
      List<LangChainEmbeddingStore> supportedStores) {
    this.supportedModels = supportedModels;
    this.supportedStores = supportedStores;
  }

  public List<TextSegment> search(String input, EmbeddingConfig embeddingConfig) {

    for (LangChainEmbeddingModel supportedModel : supportedModels) {
      if (embeddingConfig.getEmbedModel().equals(supportedModel.getName())) {
        final var langChainModel = supportedModel.getModel(embeddingConfig.getModelProperties());
        final var embedding = langChainModel.embed(input);
        return searchEmbeddings(embedding.content(), embeddingConfig);
      }
    }

    throw new RuntimeException(
        "LangChain embedding model " + embeddingConfig.getEmbedModel() + " not supported!");
  }

  public CompletableFuture<Response<Embedding>> embed(
      String input, EmbeddingConfig embeddingConfig) {

    for (LangChainEmbeddingModel supportedModel : supportedModels) {
      if (embeddingConfig.getEmbedModel().equals(supportedModel.getName())) {
        final var langChainModel = supportedModel.getModel(embeddingConfig.getModelProperties());
        return CompletableFuture.supplyAsync(
            () -> {
              final var embed = langChainModel.embed(input);
              saveToStore(embed.content(), input, embeddingConfig);
              return embed;
            },
            EXECUTOR_SERVICE);
      }
    }

    throw new RuntimeException(
        "LangChain embedding model " + embeddingConfig.getEmbedModel() + " not supported!");
  }

  private void saveToStore(
      final Embedding embed, final String input, final EmbeddingConfig embeddingConfig) {
    for (final LangChainEmbeddingStore supportedStore : supportedStores) {
      if (supportedStore.getName().equals(embeddingConfig.getEmbedStore())) {
        final var store = supportedStore.getStore(embeddingConfig.getStoreProperties());
        store.add(embed, TextSegment.from(input));
        return;
      }
    }

    throw new RuntimeException(
        "LangChain embedding store " + embeddingConfig.getEmbedStore() + " not supported!");
  }

  private List<TextSegment> searchEmbeddings(Embedding embedding, EmbeddingConfig config) {
    for (final LangChainEmbeddingStore supportedStore : supportedStores) {
      if (supportedStore.getName().equals(config.getEmbedStore())) {
        final var store = supportedStore.getStore(config.getStoreProperties());

        final var minScore = Double.valueOf(config.getStoreProperties().get("minScore"));
        final var maxResults = Integer.valueOf(config.getStoreProperties().get("maxResults"));

        final var embeddingSearchRequest =
            EmbeddingSearchRequest.builder()
                .queryEmbedding(embedding)
                .minScore(minScore)
                .maxResults(maxResults)
                .build();

        return store.search(embeddingSearchRequest).matches().stream()
            .map(EmbeddingMatch::embedded)
            .collect(Collectors.toList());
      }
    }

    throw new RuntimeException(
        "LangChain embedding store " + config.getEmbedStore() + " not supported!");
  }
}
