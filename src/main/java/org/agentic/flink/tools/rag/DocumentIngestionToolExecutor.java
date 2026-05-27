package org.agentic.flink.tools.rag;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.OllamaEmbeddingConnection;
import org.agentic.flink.ingest.Chunk;
import org.agentic.flink.ingest.RecursiveTextChunker;
import org.agentic.flink.storage.StorageFactory;
import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.agentic.flink.tools.AbstractToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Document Ingestion Tool Executor. Ingests documents into the vector store:
 *
 * <ol>
 *   <li>Splits the document into chunks ({@link RecursiveTextChunker}).
 *   <li>Creates an embedding for each chunk via an {@link EmbeddingConnection}.
 *   <li>Stores each embedding in a {@link VectorStore}, with the chunk text and incoming metadata.
 * </ol>
 *
 * <p>Migrated off the legacy {@code langchain/model} + {@code langchain/store} packages onto the
 * framework embedding/vector SPIs. The default constructor builds an
 * {@link OllamaEmbeddingConnection} and a {@link VectorStore} selected by the {@code vector.backend}
 * config key (defaulting to the zero-infra {@link InMemoryVectorStore}).
 */
public class DocumentIngestionToolExecutor extends AbstractToolExecutor {

  private final EmbeddingConnection embeddingConnection;
  private final VectorStore vectorStore;
  private final EmbeddingSetup embeddingSetup;
  private final Map<String, String> config;

  public DocumentIngestionToolExecutor(Map<String, String> config) {
    super("document_ingestion", "Ingest documents into the knowledge base");
    this.config = config != null ? config : new HashMap<>();
    String baseUrl = this.config.getOrDefault("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    String modelName = this.config.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(this.config.getOrDefault("dimension", "768"));
    this.embeddingConnection = new OllamaEmbeddingConnection(baseUrl);
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
    this.vectorStore = buildVectorStore(this.config);
  }

  public DocumentIngestionToolExecutor(
      EmbeddingConnection embeddingConnection, VectorStore vectorStore) {
    this(null, embeddingConnection, vectorStore);
  }

  public DocumentIngestionToolExecutor(
      Map<String, String> config,
      EmbeddingConnection embeddingConnection,
      VectorStore vectorStore) {
    super("document_ingestion", "Ingest documents into the knowledge base");
    this.config = config != null ? config : new HashMap<>();
    String modelName = this.config.getOrDefault("modelName", "nomic-embed-text");
    int dimension = Integer.parseInt(this.config.getOrDefault("dimension", "768"));
    this.embeddingConnection = embeddingConnection;
    this.vectorStore = vectorStore;
    this.embeddingSetup = EmbeddingSetup.of(modelName, dimension);
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
            String content = getRequiredParameter(parameters, "content", String.class);
            Integer chunkSize = getOptionalParameter(parameters, "chunk_size", Integer.class, 500);
            Integer chunkOverlap =
                getOptionalParameter(parameters, "chunk_overlap", Integer.class, 50);
            @SuppressWarnings("unchecked")
            Map<String, String> metadata =
                getOptionalParameter(parameters, "metadata", Map.class, new HashMap<>());

            // Step 1: Split document into chunks.
            String sourceId =
                metadata.getOrDefault("document_id", UUID.randomUUID().toString());
            RecursiveTextChunker chunker = new RecursiveTextChunker(chunkSize, chunkOverlap);
            List<Chunk> chunks = chunker.chunk(sourceId, content);

            LOG.info("Split document into {} chunks", chunks.size());

            // Step 2 + 3: Embed each chunk and store it.
            EmbeddingClient client = embeddingConnection.bind(null);
            List<String> storedIds = new ArrayList<>(chunks.size());

            for (Chunk chunk : chunks) {
              float[] vector = client.embed(chunk.getText(), embeddingSetup);

              Map<String, Object> chunkMetadata = new HashMap<>();
              chunkMetadata.putAll(metadata);
              chunkMetadata.put("text", chunk.getText());
              chunkMetadata.put("position", chunk.getPosition());
              chunkMetadata.put("sourceId", chunk.getSourceId());
              String flowId = metadata.get("flow_id");
              if (flowId != null) {
                chunkMetadata.put("flowId", flowId);
              }

              vectorStore.storeEmbedding(chunk.getId(), vector, chunkMetadata);
              storedIds.add(chunk.getId());
            }

            LOG.info("Stored {} embeddings in vector store", storedIds.size());

            Map<String, Object> result = new HashMap<>();
            result.put("segments_created", chunks.size());
            result.put("embeddings_created", storedIds.size());
            result.put("stored_ids", storedIds);
            result.put("chunk_size", chunkSize);
            result.put("chunk_overlap", chunkOverlap);

            return result;

          } catch (Exception e) {
            LOG.error("Document ingestion failed", e);
            throw new RuntimeException("Document ingestion failed: " + e.getMessage(), e);
          }
        });
  }

  @Override
  public boolean validateParameters(Map<String, Object> parameters) {
    if (!super.validateParameters(parameters)) {
      return false;
    }
    return parameters.containsKey("content") && parameters.get("content") instanceof String;
  }
}
