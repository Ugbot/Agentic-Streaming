package org.agentic.flink.tools.rag;

import org.agentic.flink.tools.AbstractToolExecutor;
import org.agentic.flink.langchain.model.embedding.LangChainEmbeddingModel;
import org.agentic.flink.langchain.model.embedding.OllamaEmbeddingModel;
import org.agentic.flink.langchain.model.language.LangChainLanguageModel;
import org.agentic.flink.langchain.model.language.OllamaLanguageModel;
import org.agentic.flink.langchain.store.LangChainEmbeddingStore;
import org.agentic.flink.langchain.store.QdrantEmbeddingStore;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.UserMessage;
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
 * RAG (Retrieval-Augmented Generation) Tool Executor Performs: 1. Query embedding 2. Semantic
 * search in vector store 3. Context retrieval 4. LLM generation with retrieved context
 */
public class RagToolExecutor extends AbstractToolExecutor {

  private final LangChainEmbeddingModel embeddingModelProvider;
  private final LangChainEmbeddingStore embeddingStoreProvider;
  private final LangChainLanguageModel languageModelProvider;
  private final Map<String, String> config;

  public RagToolExecutor(Map<String, String> config) {
    super("rag", "Retrieval-Augmented Generation - searches knowledge base and generates answers");
    this.config = config != null ? config : new HashMap<>();

    // Initialize providers
    this.embeddingModelProvider = new OllamaEmbeddingModel();
    this.embeddingStoreProvider = new QdrantEmbeddingStore();
    this.languageModelProvider = new OllamaLanguageModel();
  }

  public RagToolExecutor(Map<String, String> config,
      LangChainEmbeddingModel embeddingModel, LangChainEmbeddingStore embeddingStore,
      LangChainLanguageModel languageModel) {
    super("rag", "Retrieval-Augmented Generation - searches knowledge base and generates answers");
    this.config = config != null ? config : new HashMap<>();
    this.embeddingModelProvider = embeddingModel;
    this.embeddingStoreProvider = embeddingStore;
    this.languageModelProvider = languageModel;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Extract parameters
            String query = getRequiredParameter(parameters, "query", String.class);
            Integer maxResults =
                getOptionalParameter(parameters, "max_results", Integer.class, 5);
            Double minScore = getOptionalParameter(parameters, "min_score", Double.class, 0.7);

            // Step 1: Create embedding for the query
            EmbeddingModel embeddingModel = embeddingModelProvider.getModel(config);
            Response<Embedding> embeddingResponse = embeddingModel.embed(query);
            Embedding queryEmbedding = embeddingResponse.content();

            LOG.info("Created embedding for query: {}", query);

            // Step 2: Search vector store
            dev.langchain4j.store.embedding.EmbeddingStore<TextSegment> embeddingStore =
                embeddingStoreProvider.getStore(config);

            EmbeddingSearchRequest searchRequest =
                EmbeddingSearchRequest.builder()
                    .queryEmbedding(queryEmbedding)
                    .maxResults(maxResults)
                    .minScore(minScore)
                    .build();

            EmbeddingSearchResult<TextSegment> searchResult = embeddingStore.search(searchRequest);

            LOG.info("Found {} relevant documents", searchResult.matches().size());

            if (searchResult.matches().isEmpty()) {
              return createResponse(
                  query, "No relevant information found in the knowledge base.", null);
            }

            // Step 3: Build context from retrieved documents
            String context = buildContext(searchResult.matches());

            // Step 4: Generate answer with LLM using context
            String answer = generateAnswer(query, context);

            return createResponse(query, answer, searchResult.matches());

          } catch (Exception e) {
            LOG.error("RAG execution failed", e);
            throw new RuntimeException("RAG execution failed: " + e.getMessage(), e);
          }
        });
  }

  private String buildContext(List<EmbeddingMatch<TextSegment>> matches) {
    return matches.stream()
        .map(
            match -> {
              TextSegment segment = match.embedded();
              return "Score: "
                  + String.format("%.2f", match.score())
                  + "\n"
                  + segment.text()
                  + "\n";
            })
        .collect(Collectors.joining("\n---\n"));
  }

  private String generateAnswer(String query, String context) {
    String prompt =
        buildPrompt(
            "Answer the following question based on the provided context:\n\n"
                + "Context:\n"
                + context
                + "\n\n"
                + "Question: "
                + query
                + "\n\n"
                + "Answer:");

    dev.langchain4j.model.chat.ChatLanguageModel languageModel =
        languageModelProvider.getModel(config);

    Response<AiMessage> response = languageModel.generate(new UserMessage(prompt));

    return response.content().text();
  }

  private String buildPrompt(String template) {
    return template;
  }

  private Map<String, Object> createResponse(
      String query, String answer, List<EmbeddingMatch<TextSegment>> sources) {
    Map<String, Object> response = new HashMap<>();
    response.put("query", query);
    response.put("answer", answer);

    if (sources != null && !sources.isEmpty()) {
      List<Map<String, Object>> sourcesList =
          sources.stream()
              .map(
                  match -> {
                    Map<String, Object> source = new HashMap<>();
                    source.put("text", match.embedded().text());
                    source.put("score", match.score());
                    return source;
                  })
              .collect(Collectors.toList());
      response.put("sources", sourcesList);
      response.put("source_count", sourcesList.size());
    }

    return response;
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    if (!super.validateParameters(parameters)) {
      return false;
    }
    return parameters.containsKey("query") && parameters.get("query") instanceof String;
  }
}
