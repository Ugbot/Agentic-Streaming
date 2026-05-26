package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.langchain.model.embedding.DefaultEmbeddingModel;
import org.agentic.flink.langchain.store.DefaultEmbeddingStore;
import org.agentic.flink.tools.rag.DocumentIngestionToolExecutor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async function for ingesting documents into the vector store via the Flink async I/O pattern.
 *
 * <p>Wraps {@link DocumentIngestionToolExecutor} to provide non-blocking document ingestion
 * within a Flink streaming pipeline. Accepts {@link AgentEvent} inputs containing document
 * content and produces result events indicating success or failure.
 *
 * <p><b>Expected input data fields:</b>
 * <ul>
 *   <li>{@code document_content} (required) - The text content to ingest</li>
 *   <li>{@code document_id} (optional) - ID for the document; generated UUID if absent</li>
 *   <li>{@code source} (optional) - Source identifier for the document</li>
 *   <li>{@code chunk_size} (optional) - Chunk size for splitting, default 500</li>
 *   <li>{@code chunk_overlap} (optional) - Chunk overlap for splitting, default 50</li>
 * </ul>
 */
public class DocumentIngestionFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(DocumentIngestionFunction.class);

  private final Map<String, String> config;
  private final boolean useDefaults;

  private transient DocumentIngestionToolExecutor executor;

  /**
   * Creates a DocumentIngestionFunction.
   *
   * @param config Configuration map for the embedding model and store
   * @param useDefaults If true, uses DefaultEmbeddingModel (hash-based) and DefaultEmbeddingStore
   *     (in-memory). If false, uses the default Ollama + Qdrant providers.
   */
  public DocumentIngestionFunction(Map<String, String> config, boolean useDefaults) {
    this.config = config != null ? config : new HashMap<>();
    this.useDefaults = useDefaults;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    if (useDefaults) {
      this.executor = new DocumentIngestionToolExecutor(
          config, new DefaultEmbeddingModel(), new DefaultEmbeddingStore());
    } else {
      this.executor = new DocumentIngestionToolExecutor(config);
    }

    LOG.info("DocumentIngestionFunction initialized: useDefaults={}, config={}", useDefaults, config);
  }

  @Override
  public void asyncInvoke(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    try {
      Map<String, Object> inputData = input.getData();

      // Extract required document_content
      Object documentContentObj = inputData != null ? inputData.get("document_content") : null;
      if (documentContentObj == null || !(documentContentObj instanceof String)
          || ((String) documentContentObj).isEmpty()) {
        AgentEvent failEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
        failEvent.setErrorMessage("Missing required field: document_content");
        failEvent.setErrorCode("MISSING_PARAMETER");
        resultFuture.complete(Collections.singleton(failEvent));
        return;
      }
      String documentContent = (String) documentContentObj;

      // Extract optional fields
      String documentId = extractString(inputData, "document_id", UUID.randomUUID().toString());
      String source = extractString(inputData, "source", "unknown");
      int chunkSize = extractInt(inputData, "chunk_size", 500);
      int chunkOverlap = extractInt(inputData, "chunk_overlap", 50);

      // Build parameters for the executor (it expects "content" as the key)
      Map<String, Object> params = new HashMap<>();
      params.put("content", documentContent);
      params.put("chunk_size", chunkSize);
      params.put("chunk_overlap", chunkOverlap);

      Map<String, String> metadata = new HashMap<>();
      metadata.put("document_id", documentId);
      metadata.put("source", source);
      metadata.put("flow_id", input.getFlowId());
      metadata.put("agent_id", input.getAgentId());
      params.put("metadata", metadata);

      LOG.debug("Starting document ingestion for flow={}, documentId={}, chunkSize={}, chunkOverlap={}",
          input.getFlowId(), documentId, chunkSize, chunkOverlap);

      CompletableFuture<Object> executionFuture = executor.execute(params);

      executionFuture.whenComplete((result, error) -> {
        if (error != null) {
          LOG.error("Document ingestion failed for flow={}, documentId={}",
              input.getFlowId(), documentId, error);

          AgentEvent failEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
          failEvent.setErrorMessage("Document ingestion failed: " + error.getMessage());
          failEvent.setErrorCode("INGESTION_ERROR");
          resultFuture.complete(Collections.singleton(failEvent));
        } else {
          LOG.info("Document ingestion completed for flow={}, documentId={}",
              input.getFlowId(), documentId);

          AgentEvent successEvent = createResultEvent(input, AgentEventType.TOOL_CALL_COMPLETED);
          successEvent.putData("document_id", documentId);
          successEvent.putData("source", source);

          if (result instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> resultMap = (Map<String, Object>) result;
            successEvent.putData("segments_created", resultMap.get("segments_created"));
            successEvent.putData("stored_ids", resultMap.get("stored_ids"));
          } else {
            successEvent.putData("result", result != null ? result.toString() : "null");
          }

          resultFuture.complete(Collections.singleton(successEvent));
        }
      });

    } catch (Exception e) {
      LOG.error("Error initiating document ingestion for flow={}", input.getFlowId(), e);

      AgentEvent failEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
      failEvent.setErrorMessage("Document ingestion initiation failed: " + e.getMessage());
      failEvent.setErrorCode("INITIATION_ERROR");
      resultFuture.complete(Collections.singleton(failEvent));
    }
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn("Document ingestion timed out for flow={}", input.getFlowId());

    AgentEvent timeoutEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
    timeoutEvent.setErrorMessage("Document ingestion timed out");
    timeoutEvent.setErrorCode("TIMEOUT");
    resultFuture.complete(Collections.singleton(timeoutEvent));
  }

  private AgentEvent createResultEvent(AgentEvent input, AgentEventType eventType) {
    AgentEvent event = new AgentEvent(
        input.getFlowId(), input.getUserId(), input.getAgentId(), eventType);
    event.setCurrentStage("document_ingestion");
    event.setCorrelationId(input.getCorrelationId());
    event.setParentFlowId(input.getParentFlowId());
    return event;
  }

  private String extractString(Map<String, Object> data, String key, String defaultValue) {
    if (data == null) {
      return defaultValue;
    }
    Object value = data.get(key);
    if (value instanceof String && !((String) value).isEmpty()) {
      return (String) value;
    }
    return defaultValue;
  }

  private int extractInt(Map<String, Object> data, String key, int defaultValue) {
    if (data == null) {
      return defaultValue;
    }
    Object value = data.get(key);
    if (value instanceof Integer) {
      return (Integer) value;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }
}
