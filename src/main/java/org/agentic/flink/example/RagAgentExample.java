package org.agentic.flink.example;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.*;
import org.agentic.flink.function.ToolCallAsyncFunctionV2;
import org.agentic.flink.serde.ToolCallRequest;
import org.agentic.flink.serde.ToolCallResponse;
import org.agentic.flink.tools.ToolExecutorRegistry;
import org.agentic.flink.tools.rag.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

/**
 * Example demonstrating RAG (Retrieval-Augmented Generation) capabilities
 *
 * <p>Demonstrates: 1. Document ingestion into vector store 2. Semantic search 3. RAG query with
 * context retrieval 4. Embedding generation
 */
public class RagAgentExample {

  public static void main(String[] args) throws Exception {

    // Setup Flink environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration config = new Configuration();
    config.set(PipelineOptions.GENERIC_TYPES, false);
    config.set(PipelineOptions.AUTO_GENERATE_UIDS, false);
    env.configure(config);
    env.setParallelism(1); // For demo purposes

    // Step 1: Create tool registry with definitions
    Map<String, ToolDefinition> toolRegistry = createRagToolRegistry();

    // Step 2: Create executor registry with implementations
    ToolExecutorRegistry executorRegistry = createExecutorRegistry();

    // Step 3: Create sample requests demonstrating RAG workflow
    DataStream<ToolCallRequest> requests = createRagWorkflowRequests(env);

    // Step 4: Execute tools using the V2 async function
    AsyncRetryStrategy<ToolCallResponse> asyncRetryStrategy =
        new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<ToolCallResponse>(3, 1000L)
            .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
            .build();

    DataStream<ToolCallResponse> responses =
        AsyncDataStream.orderedWaitWithRetry(
                requests,
                new ToolCallAsyncFunctionV2(toolRegistry, executorRegistry),
                60000, // 60 second timeout
                TimeUnit.MILLISECONDS,
                100,
                asyncRetryStrategy)
            .uid(ToolCallAsyncFunctionV2.UID)
            .name("rag-tool-execution");

    // Step 5: Convert responses to events and print
    responses.map(ToolCallAsyncFunctionV2::responseToEvent).print().name("rag-results");

    // Execute
    env.execute("RAG Agent Example");
  }

  private static Map<String, ToolDefinition> createRagToolRegistry() {
    Map<String, ToolDefinition> registry = new HashMap<>();

    // Tool 1: Document Ingestion
    ToolDefinition ingestion = new ToolDefinition("document_ingestion", "Document Ingestion", "Ingest documents into knowledge base");
    ingestion.addInputParameter("content", "string", "Document content to ingest", true);
    ingestion.addInputParameter("chunk_size", "number", "Chunk size for splitting", false);
    ingestion.addInputParameter("chunk_overlap", "number", "Overlap between chunks", false);
    ingestion.addInputParameter("metadata", "object", "Document metadata", false);
    ingestion.setVersion("1.0");
    registry.put("document_ingestion", ingestion);

    // Tool 2: Semantic Search
    ToolDefinition search =
        new ToolDefinition(
            "semantic_search", "Semantic Search", "Search knowledge base by semantic similarity");
    search.addInputParameter("query", "string", "Search query", true);
    search.addInputParameter("max_results", "number", "Maximum results to return", false);
    search.addInputParameter("min_score", "number", "Minimum similarity score", false);
    search.setVersion("1.0");
    registry.put("semantic_search", search);

    // Tool 3: RAG Query
    ToolDefinition rag =
        new ToolDefinition(
            "rag",
            "RAG Query",
            "Answer questions using retrieval-augmented generation");
    rag.addInputParameter("query", "string", "Question to answer", true);
    rag.addInputParameter("max_results", "number", "Max context documents", false);
    rag.addInputParameter("min_score", "number", "Minimum relevance score", false);
    rag.setVersion("1.0");
    registry.put("rag", rag);

    // Tool 4: Embedding
    ToolDefinition embedding =
        new ToolDefinition("embedding", "Text Embedding", "Convert text to vector embedding");
    embedding.addInputParameter("text", "string", "Text to embed", true);
    embedding.addInputParameter("return_vector", "boolean", "Return full vector", false);
    embedding.setVersion("1.0");
    registry.put("embedding", embedding);

    return registry;
  }

  private static ToolExecutorRegistry createExecutorRegistry() {
    ToolExecutorRegistry registry = new ToolExecutorRegistry();

    // Configuration for tools (Ollama + Qdrant)
    Map<String, String> toolConfig = new HashMap<>();
    toolConfig.put("baseUrl", ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    toolConfig.put("modelName", "nomic-embed-text:latest");
    toolConfig.put("host", ConfigKeys.DEFAULT_QDRANT_HOST);
    toolConfig.put("port", ConfigKeys.DEFAULT_QDRANT_PORT);
    toolConfig.put("collectionName", "ververica-agent-knowledge");

    // Register executors
    registry.register(new DocumentIngestionToolExecutor(toolConfig));
    registry.register(new SemanticSearchToolExecutor(toolConfig));
    registry.register(new RagToolExecutor(toolConfig));
    registry.register(new EmbeddingToolExecutor(toolConfig));

    return registry;
  }

  private static DataStream<ToolCallRequest> createRagWorkflowRequests(
      StreamExecutionEnvironment env) {

    // Sample workflow:
    // 1. Ingest some documents
    // 2. Perform semantic search
    // 3. Ask a RAG question
    // 4. Generate an embedding

    return env.fromElements(
            // Request 1: Ingest document about Apache Flink
            createIngestionRequest(
                "flow-001",
                "user-001",
                "Apache Flink is a framework and distributed processing engine for stateful "
                    + "computations over unbounded and bounded data streams. Flink has been designed "
                    + "to run in all common cluster environments, perform computations at in-memory "
                    + "speed and at any scale. It provides exactly-once processing guarantees."),

            // Request 2: Ingest document about CEP
            createIngestionRequest(
                "flow-002",
                "user-001",
                "Complex Event Processing (CEP) in Apache Flink allows you to detect event patterns "
                    + "in an endless stream of events. Flink's CEP library provides an API to specify "
                    + "patterns of events to detect. Patterns can be created using a Pattern API."),

            // Request 3: Ingest document about Saga pattern
            createIngestionRequest(
                "flow-003",
                "user-001",
                "The Saga pattern is a way to manage data consistency across microservices in "
                    + "distributed transaction scenarios. A saga is a sequence of local transactions "
                    + "where each transaction updates data within a single service."),

            // Request 4: Semantic search
            createSearchRequest("flow-004", "user-001", "What is Complex Event Processing?", 3),

            // Request 5: RAG query
            createRagRequest("flow-005", "user-001", "How does Apache Flink handle state?", 5),

            // Request 6: Create embedding
            createEmbeddingRequest("flow-006", "user-001", "Distributed computing with Flink"))
        .name("rag-workflow-requests");
  }

  private static ToolCallRequest createIngestionRequest(
      String flowId, String userId, String content) {
    Map<String, Object> params = new HashMap<>();
    params.put("content", content);
    params.put("chunk_size", 200);
    params.put("chunk_overlap", 20);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("source", "example");
    metadata.put("ingestion_time", String.valueOf(System.currentTimeMillis()));
    params.put("metadata", metadata);

    ToolCallRequest request =
        new ToolCallRequest(flowId, userId, "agent-rag", "document_ingestion", params);
    request.setToolName("Document Ingestion");
    return request;
  }

  private static ToolCallRequest createSearchRequest(
      String flowId, String userId, String query, int maxResults) {
    Map<String, Object> params = new HashMap<>();
    params.put("query", query);
    params.put("max_results", maxResults);
    params.put("min_score", 0.7);

    ToolCallRequest request =
        new ToolCallRequest(flowId, userId, "agent-rag", "semantic_search", params);
    request.setToolName("Semantic Search");
    return request;
  }

  private static ToolCallRequest createRagRequest(
      String flowId, String userId, String query, int maxResults) {
    Map<String, Object> params = new HashMap<>();
    params.put("query", query);
    params.put("max_results", maxResults);
    params.put("min_score", 0.7);

    ToolCallRequest request = new ToolCallRequest(flowId, userId, "agent-rag", "rag", params);
    request.setToolName("RAG Query");
    return request;
  }

  private static ToolCallRequest createEmbeddingRequest(
      String flowId, String userId, String text) {
    Map<String, Object> params = new HashMap<>();
    params.put("text", text);
    params.put("return_vector", false);

    ToolCallRequest request = new ToolCallRequest(flowId, userId, "agent-rag", "embedding", params);
    request.setToolName("Text Embedding");
    return request;
  }
}
