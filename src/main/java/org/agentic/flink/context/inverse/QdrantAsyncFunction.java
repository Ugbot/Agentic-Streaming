package org.agentic.flink.context.inverse;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.tools.rag.DocumentIngestionToolExecutor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async function for storing context items to Qdrant vector database.
 *
 * <p>This function performs non-blocking Inverse RAG operations: high-value context items are
 * pushed to Qdrant for long-term storage and future retrieval.
 *
 * <p><b>Benefits of AsyncIO:</b>
 *
 * <ul>
 *   <li>✅ Non-blocking - doesn't halt stream processing
 *   <li>✅ High throughput - multiple requests in flight
 *   <li>✅ Fault tolerance - integrates with Flink checkpointing
 *   <li>✅ Backpressure handling - automatic when queue fills
 * </ul>
 *
 * <p><b>Usage Pattern:</b>
 *
 * <pre>{@code
 * DataStream<InverseRagResult> results = AsyncDataStream.unorderedWait(
 *     contextItems,
 *     new QdrantAsyncFunction(config),
 *     5000,  // 5 second timeout
 *     TimeUnit.MILLISECONDS,
 *     100    // max 100 concurrent requests
 * );
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see InverseRagResult
 * @see DocumentIngestionToolExecutor
 */
public class QdrantAsyncFunction
    extends RichAsyncFunction<ContextItem, InverseRagResult> {

  private static final Logger LOG = LoggerFactory.getLogger(QdrantAsyncFunction.class);

  private final Map<String, String> qdrantConfig;
  private final String flowId;
  private final String agentId;

  private transient DocumentIngestionToolExecutor ingestionExecutor;

  /**
   * Creates async function with Qdrant configuration.
   *
   * @param qdrantConfig Configuration for Qdrant connection and embedding model
   * @param flowId Flow ID for metadata tagging
   * @param agentId Agent ID for metadata tagging
   */
  public QdrantAsyncFunction(
      Map<String, String> qdrantConfig,
      String flowId,
      String agentId) {
    this.qdrantConfig = qdrantConfig != null ? qdrantConfig : createDefaultConfig();
    this.flowId = flowId;
    this.agentId = agentId;
  }

  /**
   * Creates async function with default configuration.
   *
   * @param flowId Flow ID for metadata tagging
   * @param agentId Agent ID for metadata tagging
   */
  public QdrantAsyncFunction(String flowId, String agentId) {
    this(null, flowId, agentId);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    // Initialize document ingestion tool for Qdrant
    this.ingestionExecutor = new DocumentIngestionToolExecutor(qdrantConfig);

    LOG.info(
        "QdrantAsyncFunction initialized for agent {}, flow {}: config={}",
        agentId,
        flowId,
        qdrantConfig);
  }

  /**
   * Asynchronously stores context item to Qdrant.
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Creates embedding parameters with item content
   *   <li>Adds metadata for future retrieval (agent_id, flow_id, priority, etc.)
   *   <li>Executes async ingestion via CompletableFuture
   *   <li>Completes ResultFuture when done or on error
   * </ol>
   *
   * @param item Context item to store
   * @param resultFuture Future to complete with result or error
   */
  @Override
  public void asyncInvoke(ContextItem item, ResultFuture<InverseRagResult> resultFuture) {
    try {
      // Create parameters for document ingestion
      Map<String, Object> params = new HashMap<>();
      params.put("content", item.getContent());
      params.put("chunk_size", 500);
      params.put("chunk_overlap", 50);

      // Add metadata for retrieval
      Map<String, String> metadata = new HashMap<>();
      metadata.put("item_id", item.getItemId());
      metadata.put("priority", item.getPriority().name());
      metadata.put("intent_tag", item.getIntentTag() != null ? item.getIntentTag() : "");
      metadata.put("created_at", String.valueOf(item.getCreatedAt()));
      metadata.put("agent_id", agentId);
      metadata.put("flow_id", flowId);
      metadata.put("memory_type", item.getMemoryType().name());
      metadata.put("relevancy_score", String.valueOf(item.getRelevancyScore()));
      params.put("metadata", metadata);

      LOG.debug(
          "Starting async ingestion for item {} (agent={}, flow={})",
          item.getItemId(),
          agentId,
          flowId);

      // Execute ingestion asynchronously
      CompletableFuture<Object> ingestionFuture = ingestionExecutor.execute(params);

      // Handle completion
      ingestionFuture.whenComplete(
          (result, error) -> {
            if (error != null) {
              // Ingestion failed
              LOG.error(
                  "Failed to store item {} to Qdrant (agent={}, flow={})",
                  item.getItemId(),
                  agentId,
                  flowId,
                  error);

              InverseRagResult failureResult =
                  new InverseRagResult(item.getItemId(), flowId);
              failureResult.addFailedItem(item.getItemId(), error.getMessage());

              resultFuture.complete(Collections.singleton(failureResult));
            } else {
              // Ingestion succeeded
              LOG.debug(
                  "Successfully stored item {} to Qdrant: {}",
                  item.getItemId(),
                  result);

              InverseRagResult successResult =
                  new InverseRagResult(item.getItemId(), flowId);
              successResult.addStoredItem(item.getItemId(), result.toString());

              resultFuture.complete(Collections.singleton(successResult));
            }
          });

    } catch (Exception e) {
      LOG.error("Error initiating async ingestion for item: {}", item.getItemId(), e);

      InverseRagResult errorResult = new InverseRagResult(item.getItemId(), flowId);
      errorResult.addFailedItem(item.getItemId(), e.getMessage());

      resultFuture.complete(Collections.singleton(errorResult));
    }
  }

  /**
   * Handles timeout for async operations.
   *
   * <p>If an async operation takes too long, this method is called to provide a timeout result.
   *
   * @param item Context item that timed out
   * @param resultFuture Future to complete with timeout result
   */
  @Override
  public void timeout(ContextItem item, ResultFuture<InverseRagResult> resultFuture) {
    LOG.warn(
        "Async ingestion timed out for item {} (agent={}, flow={})",
        item.getItemId(),
        agentId,
        flowId);

    InverseRagResult timeoutResult = new InverseRagResult(item.getItemId(), flowId);
    timeoutResult.addFailedItem(item.getItemId(), "Timeout waiting for Qdrant ingestion");

    resultFuture.complete(Collections.singleton(timeoutResult));
  }

  /** Creates default Qdrant configuration. */
  private static Map<String, String> createDefaultConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL); // Ollama for embeddings
    config.put("modelName", "nomic-embed-text:latest");
    config.put("host", ConfigKeys.DEFAULT_QDRANT_HOST); // Qdrant host
    config.put("port", ConfigKeys.DEFAULT_QDRANT_PORT); // Qdrant port
    config.put("collectionName", "agent-long-term-memory");
    return config;
  }

  @Override
  public void close() throws Exception {
    super.close();
    LOG.info("QdrantAsyncFunction closed for agent {}, flow {}", agentId, flowId);
  }
}
