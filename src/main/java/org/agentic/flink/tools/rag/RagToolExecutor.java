package org.agentic.flink.tools.rag;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.OllamaEmbeddingConnection;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.agentic.flink.tools.AbstractToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * RAG (Retrieval-Augmented Generation) Tool Executor. Performs:
 *
 * <ol>
 *   <li>Query embedding via an {@link EmbeddingConnection}.
 *   <li>Semantic search in a {@link VectorStore}.
 *   <li>Context assembly from retrieved chunk text.
 *   <li>Answer generation through a {@link ChatConnection}.
 * </ol>
 *
 * <p>Migrated off the legacy {@code langchain/model} + {@code langchain/store} packages onto the
 * framework embedding/vector/chat SPIs.
 */
public class RagToolExecutor extends AbstractToolExecutor {

  private final EmbeddingConnection embeddingConnection;
  private final VectorStore vectorStore;
  private final ChatConnection chatConnection;
  private final EmbeddingSetup embeddingSetup;
  private final ChatSetup chatSetup;
  private final Map<String, String> config;

  public RagToolExecutor(Map<String, String> config) {
    super("rag", "Retrieval-Augmented Generation - searches knowledge base and generates answers");
    this.config = config != null ? config : new HashMap<>();
    String baseUrl = this.config.getOrDefault("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    String modelName = this.config.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(this.config.getOrDefault("dimension", "768"));
    String chatModel = this.config.getOrDefault("chatModel", ConfigKeys.DEFAULT_OLLAMA_MODEL);

    this.embeddingConnection = new OllamaEmbeddingConnection(baseUrl);
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
    this.vectorStore = buildVectorStore(this.config);
    this.chatConnection = LangChain4jChatConnection.ollama(baseUrl);
    this.chatSetup = ChatSetup.builder().withModel(chatModel).withTemperature(0.4).build();
  }

  public RagToolExecutor(
      EmbeddingConnection embeddingConnection,
      VectorStore vectorStore,
      ChatConnection chatConnection) {
    this(null, embeddingConnection, vectorStore, chatConnection);
  }

  public RagToolExecutor(
      Map<String, String> config,
      EmbeddingConnection embeddingConnection,
      VectorStore vectorStore,
      ChatConnection chatConnection) {
    super("rag", "Retrieval-Augmented Generation - searches knowledge base and generates answers");
    this.config = config != null ? config : new HashMap<>();
    String modelName = this.config.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(this.config.getOrDefault("dimension", "768"));
    String chatModel = this.config.getOrDefault("chatModel", ConfigKeys.DEFAULT_OLLAMA_MODEL);

    this.embeddingConnection = embeddingConnection;
    this.vectorStore = vectorStore;
    this.chatConnection = chatConnection;
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
    this.chatSetup = ChatSetup.builder().withModel(chatModel).withTemperature(0.4).build();
  }

  private static VectorStore buildVectorStore(Map<String, String> config) {
    String backend = config.get("vector.backend");
    try {
      if (backend != null && !backend.isBlank()) {
        return StorageFactory.createVectorStore(backend, config);
      }
      InMemoryVectorStore store = new InMemoryVectorStore();
      store.initialize(config);
      return store;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create vector store: " + e.getMessage(), e);
    }
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    logExecution(parameters);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String query = getRequiredParameter(parameters, "query", String.class);
            Integer maxResults = getOptionalParameter(parameters, "max_results", Integer.class, 5);
            Double minScore = getOptionalParameter(parameters, "min_score", Double.class, 0.7);

            // Step 1: Embed the query.
            EmbeddingClient embeddingClient = embeddingConnection.bind(null);
            float[] queryEmbedding = embeddingClient.embed(query, embeddingSetup);

            LOG.info("Created embedding for query: {}", query);

            // Step 2: Search the vector store.
            List<VectorStore.VectorSearchResult> allMatches =
                vectorStore.searchSimilar(queryEmbedding, maxResults);
            List<VectorStore.VectorSearchResult> matches = new ArrayList<>();
            for (VectorStore.VectorSearchResult m : allMatches) {
              if (m.getScore() >= minScore) {
                matches.add(m);
              }
            }

            LOG.info("Found {} relevant documents", matches.size());

            if (matches.isEmpty()) {
              return createResponse(
                  query, "No relevant information found in the knowledge base.", null);
            }

            // Step 3: Build context from retrieved documents.
            String context = buildContext(matches);

            // Step 4: Generate answer with the LLM using the retrieved context.
            String answer = generateAnswer(query, context);

            return createResponse(query, answer, matches);

          } catch (Exception e) {
            LOG.error("RAG execution failed", e);
            throw new RuntimeException("RAG execution failed: " + e.getMessage(), e);
          }
        });
  }

  private String buildContext(List<VectorStore.VectorSearchResult> matches) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < matches.size(); i++) {
      VectorStore.VectorSearchResult match = matches.get(i);
      if (i > 0) {
        sb.append("\n---\n");
      }
      sb.append("Score: ")
          .append(String.format("%.2f", match.getScore()))
          .append("\n")
          .append(textOf(match))
          .append("\n");
    }
    return sb.toString();
  }

  private String generateAnswer(String query, String context) throws Exception {
    String prompt =
        "Answer the following question based on the provided context:\n\n"
            + "Context:\n"
            + context
            + "\n\n"
            + "Question: "
            + query
            + "\n\n"
            + "Answer:";

    ChatClient chatClient = chatConnection.bind(null);
    ChatResponse response =
        chatClient.chat(
            List.of(
                ChatMessage.system(
                    "You are a helpful assistant that answers questions strictly from the "
                        + "provided context."),
                ChatMessage.user(prompt)),
            chatSetup);

    return response.getText();
  }

  private static String textOf(VectorStore.VectorSearchResult match) {
    Object text = match.getMetadata() != null ? match.getMetadata().get("text") : null;
    return text != null ? text.toString() : "";
  }

  private Map<String, Object> createResponse(
      String query, String answer, List<VectorStore.VectorSearchResult> sources) {
    Map<String, Object> response = new HashMap<>();
    response.put("query", query);
    response.put("answer", answer);

    if (sources != null && !sources.isEmpty()) {
      List<Map<String, Object>> sourcesList = new ArrayList<>();
      for (VectorStore.VectorSearchResult match : sources) {
        Map<String, Object> source = new HashMap<>();
        source.put("text", textOf(match));
        source.put("score", (double) match.getScore());
        sourcesList.add(source);
      }
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
