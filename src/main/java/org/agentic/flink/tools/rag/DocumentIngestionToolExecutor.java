package org.agentic.flink.tools.rag;

import org.agentic.flink.tools.AbstractToolExecutor;
import org.agentic.flink.langchain.model.embedding.LangChainEmbeddingModel;
import org.agentic.flink.langchain.model.embedding.OllamaEmbeddingModel;
import org.agentic.flink.langchain.store.LangChainEmbeddingStore;
import org.agentic.flink.langchain.store.QdrantEmbeddingStore;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Document Ingestion Tool Executor Ingests documents into the vector store: 1. Splits document
 * into chunks 2. Creates embeddings for each chunk 3. Stores in vector database
 */
public class DocumentIngestionToolExecutor extends AbstractToolExecutor {

  private final LangChainEmbeddingModel embeddingModelProvider;
  private final LangChainEmbeddingStore embeddingStoreProvider;
  private final Map<String, String> config;

  public DocumentIngestionToolExecutor(Map<String, String> config) {
    super("document_ingestion", "Ingest documents into the knowledge base");
    this.config = config != null ? config : new HashMap<>();
    this.embeddingModelProvider = new OllamaEmbeddingModel();
    this.embeddingStoreProvider = new QdrantEmbeddingStore();
  }

  public DocumentIngestionToolExecutor(Map<String, String> config,
      LangChainEmbeddingModel embeddingModel, LangChainEmbeddingStore embeddingStore) {
    super("document_ingestion", "Ingest documents into the knowledge base");
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
            String content = getRequiredParameter(parameters, "content", String.class);
            Integer chunkSize = getOptionalParameter(parameters, "chunk_size", Integer.class, 500);
            Integer chunkOverlap =
                getOptionalParameter(parameters, "chunk_overlap", Integer.class, 50);
            @SuppressWarnings("unchecked")
            Map<String, String> metadata =
                getOptionalParameter(parameters, "metadata", Map.class, new HashMap<>());

            // Step 1: Create document
            Document document = Document.from(content, new dev.langchain4j.data.document.Metadata(metadata));

            // Step 2: Split document into chunks
            DocumentSplitter splitter =
                DocumentSplitters.recursive(chunkSize, chunkOverlap);
            List<TextSegment> segments = splitter.split(document);

            LOG.info("Split document into {} segments", segments.size());

            // Step 3: Create embeddings for all segments
            EmbeddingModel embeddingModel = embeddingModelProvider.getModel(config);
            List<Embedding> embeddings = new ArrayList<>();

            for (TextSegment segment : segments) {
              Response<Embedding> response = embeddingModel.embed(segment.text());
              embeddings.add(response.content());
            }

            LOG.info("Created {} embeddings", embeddings.size());

            // Step 4: Store in vector database
            dev.langchain4j.store.embedding.EmbeddingStore<TextSegment> embeddingStore =
                embeddingStoreProvider.getStore(config);

            List<String> ids = embeddingStore.addAll(embeddings, segments);

            LOG.info("Stored {} embeddings in vector store", ids.size());

            Map<String, Object> result = new HashMap<>();
            result.put("segments_created", segments.size());
            result.put("embeddings_created", embeddings.size());
            result.put("stored_ids", ids);
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
