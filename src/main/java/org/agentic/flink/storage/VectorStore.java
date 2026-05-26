package org.agentic.flink.storage;

import org.agentic.flink.context.core.ContextItem;
import java.util.List;
import java.util.Map;

/**
 * Storage interface for vector embeddings and semantic search.
 *
 * <p>Vector stores enable semantic search over conversation history, documents, and knowledge
 * bases. They store high-dimensional embeddings generated from text and support similarity search.
 *
 * <p>Characteristics:
 *
 * <ul>
 *   <li>Tier: VECTOR
 *   <li>Latency: 5-50ms (depends on index size and backend)
 *   <li>Scope: Semantic search, RAG retrieval, knowledge base queries
 *   <li>TTL: Indefinite or application-specific
 *   <li>Backends: Qdrant, Pinecone, Weaviate, pgvector
 * </ul>
 *
 * <p>Use cases:
 *
 * <ul>
 *   <li>Retrieval Augmented Generation (RAG)
 *   <li>Semantic search over conversation history
 *   <li>Knowledge base queries
 *   <li>Similar conversation retrieval
 *   <li>Duplicate detection
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * VectorStore store = new QdrantVectorStore();
 * store.initialize(config);
 *
 * // Store an embedding
 * float[] embedding = embeddingModel.embed("Order #12345 shipped yesterday");
 * store.storeEmbedding("item-001", embedding, metadata);
 *
 * // Semantic search
 * float[] queryEmbedding = embeddingModel.embed("where is my order?");
 * List<VectorSearchResult> results = store.searchSimilar(queryEmbedding, 5);
 *
 * // Store context items with embeddings
 * ContextItem item = new ContextItem("Premium customer", ContextPriority.MUST);
 * store.storeContextItem("flow-001", item);
 * }</pre>
 *
 * @author Agentic Flink Team
 */
public interface VectorStore extends StorageProvider<String, float[]> {

  /**
   * Store an embedding vector with associated metadata.
   *
   * <p>The embedding should be normalized if the vector store uses cosine similarity. Metadata can
   * include original text, source, timestamp, etc.
   *
   * @param id Unique identifier for this embedding
   * @param embedding Embedding vector (typically 384, 768, or 1536 dimensions)
   * @param metadata Associated metadata (original text, source, timestamp, etc.)
   * @throws Exception if storage operation fails
   */
  void storeEmbedding(String id, float[] embedding, Map<String, Object> metadata)
      throws Exception;

  /**
   * Store multiple embeddings in batch for efficiency.
   *
   * <p>Batch operations are significantly faster for bulk ingestion.
   *
   * @param embeddings Map of IDs to embedding vectors
   * @param metadata Map of IDs to metadata objects
   * @throws Exception if batch storage fails
   */
  void storeEmbeddingsBatch(
      Map<String, float[]> embeddings, Map<String, Map<String, Object>> metadata)
      throws Exception;

  /**
   * Search for similar embeddings using vector similarity.
   *
   * <p>Returns the top K most similar embeddings based on the similarity metric (cosine similarity
   * or Euclidean distance).
   *
   * @param queryEmbedding Query embedding vector
   * @param topK Number of results to return
   * @return List of search results ordered by similarity (highest first)
   * @throws Exception if search operation fails
   */
  List<VectorSearchResult> searchSimilar(float[] queryEmbedding, int topK) throws Exception;

  /**
   * Search for similar embeddings with metadata filtering.
   *
   * <p>Filters results based on metadata before or during similarity search. Example filters:
   *
   * <ul>
   *   <li>flowId = "flow-001"
   *   <li>userId = "user-123"
   *   <li>timestamp &gt; 1234567890
   *   <li>priority = "MUST"
   * </ul>
   *
   * @param queryEmbedding Query embedding vector
   * @param topK Number of results to return
   * @param metadataFilter Metadata filters to apply
   * @return List of search results matching filter and ordered by similarity
   * @throws Exception if search operation fails
   */
  List<VectorSearchResult> searchSimilarWithFilter(
      float[] queryEmbedding, int topK, Map<String, Object> metadataFilter) throws Exception;

  /**
   * Store a context item with automatic embedding generation.
   *
   * <p>This is a convenience method that generates the embedding from the context item's content
   * and stores it. Requires an embedding model to be configured.
   *
   * @param flowId Conversation flow identifier
   * @param item Context item to store
   * @throws Exception if embedding generation or storage fails
   */
  void storeContextItem(String flowId, ContextItem item) throws Exception;

  /**
   * Search for similar context items by text query.
   *
   * <p>This is a convenience method that generates an embedding from the query text and searches
   * for similar items.
   *
   * @param queryText Text query
   * @param topK Number of results to return
   * @return List of context items ordered by similarity
   * @throws Exception if embedding generation or search fails
   */
  List<ContextItem> searchContextItems(String queryText, int topK) throws Exception;

  /**
   * Retrieve an embedding by ID.
   *
   * @param id Embedding identifier
   * @return Embedding vector
   * @throws Exception if retrieval fails
   */
  float[] getEmbedding(String id) throws Exception;

  /**
   * Retrieve metadata for an embedding.
   *
   * @param id Embedding identifier
   * @return Metadata map
   * @throws Exception if retrieval fails
   */
  Map<String, Object> getMetadata(String id) throws Exception;

  /**
   * Delete an embedding and its metadata.
   *
   * @param id Embedding identifier
   * @throws Exception if deletion fails
   */
  void deleteEmbedding(String id) throws Exception;

  /**
   * Delete all embeddings for a conversation flow.
   *
   * <p>Removes all embeddings where metadata.flowId matches the given flowId.
   *
   * @param flowId Conversation flow identifier
   * @throws Exception if deletion fails
   */
  void deleteByFlowId(String flowId) throws Exception;

  /**
   * Get the dimension of embeddings stored in this vector store.
   *
   * <p>All embeddings in a vector store must have the same dimension (e.g., 384 for
   * all-MiniLM-L6-v2, 1536 for OpenAI ada-002).
   *
   * @return Embedding dimension
   */
  int getEmbeddingDimension();

  /**
   * Get the similarity metric used by this vector store.
   *
   * @return Similarity metric ("cosine", "euclidean", "dot_product")
   */
  String getSimilarityMetric();

  /**
   * Get collection or index statistics.
   *
   * <p>Returns metrics like:
   *
   * <ul>
   *   <li>total_vectors: Total number of embeddings stored
   *   <li>index_size_mb: Size of the index in megabytes
   *   <li>avg_search_latency_ms: Average search latency
   * </ul>
   *
   * @return Map of statistic names to values
   * @throws Exception if statistics retrieval fails
   */
  Map<String, Object> getStatistics() throws Exception;

  /**
   * Create a collection or index for organizing embeddings.
   *
   * <p>Some vector stores require explicit collection creation. This is a no-op for stores that
   * create collections automatically.
   *
   * @param collectionName Collection name
   * @param dimension Embedding dimension
   * @param config Collection-specific configuration
   * @throws Exception if creation fails
   */
  void createCollection(String collectionName, int dimension, Map<String, Object> config)
      throws Exception;

  @Override
  default StorageTier getTier() {
    return StorageTier.VECTOR;
  }

  @Override
  default long getExpectedLatencyMs() {
    return 20; // 5-50ms depending on index size
  }

  /**
   * Result from a vector similarity search.
   *
   * <p>Contains the embedding ID, similarity score, and associated metadata.
   */
  class VectorSearchResult {
    private final String id;
    private final float score;
    private final Map<String, Object> metadata;

    public VectorSearchResult(String id, float score, Map<String, Object> metadata) {
      this.id = id;
      this.score = score;
      this.metadata = metadata;
    }

    /** Get the embedding ID. */
    public String getId() {
      return id;
    }

    /**
     * Get the similarity score.
     *
     * <p>Higher scores indicate greater similarity. Scale depends on similarity metric (cosine:
     * 0-1, euclidean: 0+).
     */
    public float getScore() {
      return score;
    }

    /** Get the associated metadata. */
    public Map<String, Object> getMetadata() {
      return metadata;
    }
  }
}
