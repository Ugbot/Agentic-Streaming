package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.langchain.model.embedding.DefaultEmbeddingModel;
import org.agentic.flink.langchain.store.DefaultEmbeddingStore;
import org.agentic.flink.tools.rag.SemanticSearchToolExecutor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async function for performing semantic search against the vector store via the Flink async I/O
 * pattern.
 *
 * <p>Wraps {@link SemanticSearchToolExecutor} to provide non-blocking semantic search within a
 * Flink streaming pipeline. Accepts {@link AgentEvent} inputs containing a query and produces
 * result events with matched documents.
 *
 * <p><b>Expected input data fields:</b>
 * <ul>
 *   <li>{@code query} (required) - The search query text</li>
 *   <li>{@code max_results} (optional) - Maximum number of results to return, default 10</li>
 *   <li>{@code min_score} (optional) - Minimum similarity score threshold, default 0.7</li>
 * </ul>
 */
public class SemanticSearchFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SemanticSearchFunction.class);

  private final Map<String, String> config;
  private final boolean useDefaults;

  private transient SemanticSearchToolExecutor executor;

  /**
   * Creates a SemanticSearchFunction.
   *
   * @param config Configuration map for the embedding model and store
   * @param useDefaults If true, uses DefaultEmbeddingModel (hash-based) and DefaultEmbeddingStore
   *     (in-memory). If false, uses the default Ollama + Qdrant providers.
   */
  public SemanticSearchFunction(Map<String, String> config, boolean useDefaults) {
    this.config = config != null ? config : new HashMap<>();
    this.useDefaults = useDefaults;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    if (useDefaults) {
      this.executor = new SemanticSearchToolExecutor(
          config, new DefaultEmbeddingModel(), new DefaultEmbeddingStore());
    } else {
      this.executor = new SemanticSearchToolExecutor(config);
    }

    LOG.info("SemanticSearchFunction initialized: useDefaults={}, config={}", useDefaults, config);
  }

  @Override
  public void asyncInvoke(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    try {
      Map<String, Object> inputData = input.getData();

      // Extract required query
      Object queryObj = inputData != null ? inputData.get("query") : null;
      if (queryObj == null || !(queryObj instanceof String)
          || ((String) queryObj).isEmpty()) {
        AgentEvent failEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
        failEvent.setErrorMessage("Missing required field: query");
        failEvent.setErrorCode("MISSING_PARAMETER");
        resultFuture.complete(Collections.singleton(failEvent));
        return;
      }
      String query = (String) queryObj;

      // Extract optional fields
      int maxResults = extractInt(inputData, "max_results", 10);
      double minScore = extractDouble(inputData, "min_score", 0.7);

      // Build parameters for the executor
      Map<String, Object> params = new HashMap<>();
      params.put("query", query);
      params.put("max_results", maxResults);
      params.put("min_score", minScore);

      LOG.debug("Starting semantic search for flow={}, query='{}', maxResults={}, minScore={}",
          input.getFlowId(), query, maxResults, minScore);

      CompletableFuture<Object> executionFuture = executor.execute(params);

      executionFuture.whenComplete((result, error) -> {
        if (error != null) {
          LOG.error("Semantic search failed for flow={}, query='{}'",
              input.getFlowId(), query, error);

          AgentEvent failEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
          failEvent.setErrorMessage("Semantic search failed: " + error.getMessage());
          failEvent.setErrorCode("SEARCH_ERROR");
          resultFuture.complete(Collections.singleton(failEvent));
        } else {
          LOG.info("Semantic search completed for flow={}, query='{}'",
              input.getFlowId(), query);

          AgentEvent successEvent = createResultEvent(input, AgentEventType.TOOL_CALL_COMPLETED);
          successEvent.putData("query", query);

          if (result instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> resultMap = (Map<String, Object>) result;
            successEvent.putData("search_results", resultMap.get("results"));
            successEvent.putData("result_count", resultMap.get("result_count"));
          } else {
            successEvent.putData("result", result != null ? result.toString() : "null");
          }

          resultFuture.complete(Collections.singleton(successEvent));
        }
      });

    } catch (Exception e) {
      LOG.error("Error initiating semantic search for flow={}", input.getFlowId(), e);

      AgentEvent failEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
      failEvent.setErrorMessage("Semantic search initiation failed: " + e.getMessage());
      failEvent.setErrorCode("INITIATION_ERROR");
      resultFuture.complete(Collections.singleton(failEvent));
    }
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn("Semantic search timed out for flow={}", input.getFlowId());

    AgentEvent timeoutEvent = createResultEvent(input, AgentEventType.TOOL_CALL_FAILED);
    timeoutEvent.setErrorMessage("Semantic search timed out");
    timeoutEvent.setErrorCode("TIMEOUT");
    resultFuture.complete(Collections.singleton(timeoutEvent));
  }

  private AgentEvent createResultEvent(AgentEvent input, AgentEventType eventType) {
    AgentEvent event = new AgentEvent(
        input.getFlowId(), input.getUserId(), input.getAgentId(), eventType);
    event.setCurrentStage("semantic_search");
    event.setCorrelationId(input.getCorrelationId());
    event.setParentFlowId(input.getParentFlowId());
    return event;
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

  private double extractDouble(Map<String, Object> data, String key, double defaultValue) {
    if (data == null) {
      return defaultValue;
    }
    Object value = data.get(key);
    if (value instanceof Double) {
      return (Double) value;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof String) {
      try {
        return Double.parseDouble((String) value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }
}
