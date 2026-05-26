package org.agentic.flink.tools.rag;

import org.agentic.flink.tools.AbstractToolExecutor;
import org.agentic.flink.langchain.model.embedding.LangChainEmbeddingModel;
import org.agentic.flink.langchain.model.embedding.OllamaEmbeddingModel;
import org.agentic.flink.langchain.store.LangChainEmbeddingStore;
import org.agentic.flink.langchain.store.QdrantEmbeddingStore;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Semantic Search Tool Executor Searches vector store for semantically similar documents
 */
public class SemanticSearchToolExecutor extends AbstractToolExecutor {

  private final LangChainEmbeddingModel embeddingModelProvider;
  private final LangChainEmbeddingStore embeddingStoreProvider;
  private final Map<String, String> config;

  public SemanticSearchToolExecutor(Map<String, String> config) {
    super("semantic_search", "Search knowledge base using semantic similarity");
    this.config = config != null ? config : new HashMap<>();
    this.embeddingModelProvider = new OllamaEmbeddingModel();
    this.embeddingStoreProvider = new QdrantEmbeddingStore();
  }

  public SemanticSearchToolExecutor(Map<String, String> config,
      LangChainEmbeddingModel embeddingModel, LangChainEmbeddingStore embeddingStore) {
    super("semantic_search", "Search knowledge base using semantic similarity");
    this.config = config != null ? config : new HashMap<>();
    this.embeddingModelProvider = embeddingModel;
    this.embeddingStoreProvider = embeddingStore;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String query = getRequiredParameter(parameters, "query", String.class);
            Integer maxResults =
                getOptionalParameter(parameters, "max_results", Integer.class, 10);
            Double minScore = getOptionalParameter(parameters, "min_score", Double.class, 0.7);

            // Create query embedding
            EmbeddingModel embeddingModel = embeddingModelProvider.getModel(config);
            Response<Embedding> embeddingResponse = embeddingModel.embed(query);
            Embedding queryEmbedding = embeddingResponse.content();

            // Search vector store
            dev.langchain4j.store.embedding.EmbeddingStore<TextSegment> embeddingStore =
                embeddingStoreProvider.getStore(config);

            EmbeddingSearchRequest searchRequest =
                EmbeddingSearchRequest.builder()
                    .queryEmbedding(queryEmbedding)
                    .maxResults(maxResults)
                    .minScore(minScore)
                    .build();

            EmbeddingSearchResult<TextSegment> searchResult = embeddingStore.search(searchRequest);

            LOG.info(
                "Semantic search found {} results for query: {}", searchResult.matches().size(), query);

            Map<String, Object> result = new HashMap<>();
            result.put("query", query);
            result.put("result_count", searchResult.matches().size());
            result.put("results", formatResults(searchResult.matches()));

            return result;

          } catch (Exception e) {
            LOG.error("Semantic search execution failed", e);
            throw new RuntimeException("Semantic search execution failed: " + e.getMessage(), e);
          }
        });
  }

  private List<Map<String, Object>> formatResults(List<EmbeddingMatch<TextSegment>> matches) {
    return matches.stream()
        .map(
            match -> {
              Map<String, Object> resultItem = new HashMap<>();
              resultItem.put("text", match.embedded().text());
              resultItem.put("score", match.score());
              resultItem.put("embedding_id", match.embeddingId());
              return resultItem;
            })
        .collect(Collectors.toList());
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    if (!super.validateParameters(parameters)) {
      return false;
    }
    return parameters.containsKey("query") && parameters.get("query") instanceof String;
  }
}
