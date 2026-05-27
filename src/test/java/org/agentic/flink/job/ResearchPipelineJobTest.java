package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.embedding.HashEmbeddingConnection;
import org.agentic.flink.function.DocumentIngestionFunction;
import org.agentic.flink.function.SemanticSearchFunction;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.agentic.flink.tools.rag.DocumentIngestionToolExecutor;
import org.agentic.flink.tools.rag.RagToolExecutor;
import org.agentic.flink.tools.rag.SemanticSearchToolExecutor;
import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.junit.jupiter.api.*;

/**
 * Unit tests for the research pipeline: {@link DocumentIngestionFunction},
 * {@link SemanticSearchFunction}, {@link ResearchPipelineJob}, and the injectable
 * RAG tool executors.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>Construction with default and custom providers</li>
 *   <li>Serialization (required for Flink distribution across task managers)</li>
 *   <li>Type hierarchy (RichAsyncFunction)</li>
 *   <li>Pipeline job configuration, agent building, and tool config</li>
 *   <li>End-to-end ingestion and search using in-memory defaults</li>
 * </ul>
 *
 * <p>All test data uses randomized identifiers via {@link UUID#randomUUID()} and
 * {@link ThreadLocalRandom}. The injectable tool executors are wired with a deterministic
 * {@link HashEmbeddingConnection} and a per-test {@link InMemoryVectorStore} so the suite runs
 * with no external embedding/vector server. The RAG generation step uses an in-test offline
 * {@link ChatConnection} (see {@code EchoChatConnection}) so it never reaches a live LLM.
 *
 * @author Agentic Flink Team
 * @see DocumentIngestionFunction
 * @see SemanticSearchFunction
 * @see ResearchPipelineJob
 */
@DisplayName("Research Pipeline")
class ResearchPipelineJobTest {

  // ==================== Helpers ====================

  /**
   * Creates an AgentEvent with randomized identifiers and the given event type.
   */
  private static AgentEvent randomEvent(AgentEventType type) {
    AgentEvent event = new AgentEvent(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        "agent-" + UUID.randomUUID().toString().substring(0, 8),
        type);
    event.setCurrentStage("test-stage-" + ThreadLocalRandom.current().nextInt(100));
    event.setIterationNumber(ThreadLocalRandom.current().nextInt(0, 10));
    return event;
  }

  /**
   * Creates a randomized collection name for isolation between test runs.
   */
  private static String randomCollectionName() {
    return "test-collection-" + UUID.randomUUID().toString().substring(0, 8);
  }

  /**
   * Creates a randomized config map with unique identifiers.
   */
  private static Map<String, String> randomConfigMap() {
    Map<String, String> props = new HashMap<>();
    props.put("baseUrl", "http://localhost:11434");
    props.put("modelName", "nomic-embed-text:latest");
    props.put("host", "localhost");
    props.put("port", "6334");
    props.put("collectionName", randomCollectionName());
    return props;
  }

  /**
   * Simple ResultFuture implementation that captures the collected results.
   */
  private static class CapturingResultFuture implements ResultFuture<AgentEvent> {
    private final AtomicReference<Collection<AgentEvent>> results = new AtomicReference<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    @Override
    public void complete(Collection<AgentEvent> result) {
      results.set(result);
    }

    @Override
    public void completeExceptionally(Throwable throwable) {
      error.set(throwable);
    }

    public Collection<AgentEvent> getResults() {
      return results.get();
    }

    public Throwable getError() {
      return error.get();
    }
  }

  /**
   * Serializes and deserializes an object, returning the deserialized version.
   */
  private static <T extends Serializable> T roundTrip(T object) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(object);
    }
    byte[] bytes = bos.toByteArray();
    assertTrue(bytes.length > 0, "Serialized bytes should not be empty");
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      @SuppressWarnings("unchecked")
      T result = (T) ois.readObject();
      return result;
    }
  }

  /** Builds a fresh, initialized in-JVM vector store for a single test. */
  private static InMemoryVectorStore newVectorStore() {
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(new HashMap<>());
    return store;
  }

  /**
   * Offline {@link ChatConnection} that echoes the last user message. Mirrors the deterministic,
   * server-free behaviour the legacy {@code DefaultLanguageModel} provided, so the RAG generation
   * step can be exercised without a live LLM.
   */
  private static final class EchoChatConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          String last = messages.isEmpty() ? "" : messages.get(messages.size() - 1).getContent();
          return new ChatResponse(
              "You shared this message with the AI model: " + last,
              setup.getModelName(),
              List.of(),
              null,
              ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "echo";
        }
      };
    }

    @Override
    public String providerName() {
      return "echo";
    }
  }

  // ==================== DocumentIngestionFunction Tests ====================

  @Nested
  @DisplayName("DocumentIngestionFunction")
  class DocumentIngestionFunctionTests {

    @Test
    @DisplayName("should construct with default providers (useDefaults=true)")
    void constructsWithDefaultProviders() {
      Map<String, String> config = randomConfigMap();
      DocumentIngestionFunction function = new DocumentIngestionFunction(config, true);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with custom providers (useDefaults=false)")
    void constructsWithCustomProviders() {
      Map<String, String> config = randomConfigMap();
      DocumentIngestionFunction function = new DocumentIngestionFunction(config, false);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with null config")
    void constructsWithNullConfig() {
      DocumentIngestionFunction function = new DocumentIngestionFunction(null, true);
      assertNotNull(function);
    }

    @RepeatedTest(3)
    @DisplayName("should be serializable with randomized config")
    void isSerializable() throws Exception {
      Map<String, String> config = randomConfigMap();
      boolean useDefaults = ThreadLocalRandom.current().nextBoolean();
      DocumentIngestionFunction original = new DocumentIngestionFunction(config, useDefaults);

      DocumentIngestionFunction deserialized = roundTrip(original);
      assertNotNull(deserialized);
      assertInstanceOf(DocumentIngestionFunction.class, deserialized);
    }

    @Test
    @DisplayName("should implement RichAsyncFunction")
    void implementsRichAsyncFunction() {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      assertInstanceOf(RichAsyncFunction.class, function);
    }

    @Test
    @DisplayName("should open and process events with default providers")
    void opensAndProcessesWithDefaults() throws Exception {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        String uniqueContent = "Test document about Apache Flink streaming " + UUID.randomUUID();
        event.putData("document_content", uniqueContent);
        event.putData("chunk_size", 100);
        event.putData("chunk_overlap", 10);

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        // Wait for the async operation to complete (bounded wait)
        long deadline = System.currentTimeMillis() + 10_000;
        while (resultFuture.getResults() == null && System.currentTimeMillis() < deadline) {
          Thread.sleep(50);
        }

        assertNotNull(resultFuture.getResults(), "Async invoke should complete within timeout");
        assertEquals(1, resultFuture.getResults().size());

        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(AgentEventType.TOOL_CALL_COMPLETED, result.getEventType());
        assertEquals("document_ingestion", result.getCurrentStage());
      } finally {
        function.close();
      }
    }

    @Test
    @DisplayName("should fail gracefully when document_content is missing")
    void failsOnMissingContent() throws Exception {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        // Deliberately do NOT set document_content

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        assertNotNull(resultFuture.getResults(), "Should complete immediately for missing content");
        assertEquals(1, resultFuture.getResults().size());

        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(AgentEventType.TOOL_CALL_FAILED, result.getEventType());
        assertEquals("MISSING_PARAMETER", result.getErrorCode());
      } finally {
        function.close();
      }
    }

    @Test
    @DisplayName("should pass event through on timeout without loss")
    void handlesTimeout() throws Exception {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        event.putData("document_content", "Timeout test " + UUID.randomUUID());

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.timeout(event, resultFuture);

        assertNotNull(resultFuture.getResults());
        assertEquals(1, resultFuture.getResults().size());

        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(AgentEventType.TOOL_CALL_FAILED, result.getEventType());
        assertEquals("TIMEOUT", result.getErrorCode());
      } finally {
        function.close();
      }
    }
  }

  // ==================== SemanticSearchFunction Tests ====================

  @Nested
  @DisplayName("SemanticSearchFunction")
  class SemanticSearchFunctionTests {

    @Test
    @DisplayName("should construct with default providers (useDefaults=true)")
    void constructsWithDefaultProviders() {
      Map<String, String> config = randomConfigMap();
      SemanticSearchFunction function = new SemanticSearchFunction(config, true);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with custom providers (useDefaults=false)")
    void constructsWithCustomProviders() {
      Map<String, String> config = randomConfigMap();
      SemanticSearchFunction function = new SemanticSearchFunction(config, false);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with null config")
    void constructsWithNullConfig() {
      SemanticSearchFunction function = new SemanticSearchFunction(null, true);
      assertNotNull(function);
    }

    @RepeatedTest(3)
    @DisplayName("should be serializable with randomized config")
    void isSerializable() throws Exception {
      Map<String, String> config = randomConfigMap();
      boolean useDefaults = ThreadLocalRandom.current().nextBoolean();
      SemanticSearchFunction original = new SemanticSearchFunction(config, useDefaults);

      SemanticSearchFunction deserialized = roundTrip(original);
      assertNotNull(deserialized);
      assertInstanceOf(SemanticSearchFunction.class, deserialized);
    }

    @Test
    @DisplayName("should implement RichAsyncFunction")
    void implementsRichAsyncFunction() {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      assertInstanceOf(RichAsyncFunction.class, function);
    }

    @Test
    @DisplayName("should fail gracefully when query is missing")
    void failsOnMissingQuery() throws Exception {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        // Deliberately do NOT set query

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        assertNotNull(resultFuture.getResults(), "Should complete immediately for missing query");
        assertEquals(1, resultFuture.getResults().size());

        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(AgentEventType.TOOL_CALL_FAILED, result.getEventType());
        assertEquals("MISSING_PARAMETER", result.getErrorCode());
      } finally {
        function.close();
      }
    }

    @Test
    @DisplayName("should pass event through on timeout without loss")
    void handlesTimeout() throws Exception {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        event.putData("query", "timeout query " + UUID.randomUUID());

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.timeout(event, resultFuture);

        assertNotNull(resultFuture.getResults());
        assertEquals(1, resultFuture.getResults().size());

        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(AgentEventType.TOOL_CALL_FAILED, result.getEventType());
        assertEquals("TIMEOUT", result.getErrorCode());
      } finally {
        function.close();
      }
    }

    @Test
    @DisplayName("should open and process search events with default providers")
    void opensAndProcessesWithDefaults() throws Exception {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        event.putData("query", "stream processing " + UUID.randomUUID());
        event.putData("max_results", 5);
        event.putData("min_score", 0.0);

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        // Wait for the async operation to complete
        long deadline = System.currentTimeMillis() + 10_000;
        while (resultFuture.getResults() == null && System.currentTimeMillis() < deadline) {
          Thread.sleep(50);
        }

        assertNotNull(resultFuture.getResults(), "Async invoke should complete within timeout");
        assertEquals(1, resultFuture.getResults().size());

        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(AgentEventType.TOOL_CALL_COMPLETED, result.getEventType());
        assertEquals("semantic_search", result.getCurrentStage());
      } finally {
        function.close();
      }
    }
  }

  // ==================== ResearchPipelineJob Tests ====================

  @Nested
  @DisplayName("ResearchPipelineJob")
  class PipelineJobTests {

    @Test
    @DisplayName("should construct with config and collection name")
    void constructsWithConfig() {
      AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
      String collection = randomCollectionName();

      ResearchPipelineJob job = new ResearchPipelineJob(config, collection, true);
      assertNotNull(job);
      assertEquals(collection, job.getCollectionName());
      assertTrue(job.isUseDefaults());
    }

    @Test
    @DisplayName("should construct with useDefaults=false")
    void constructsWithoutDefaults() {
      AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
      String collection = randomCollectionName();

      ResearchPipelineJob job = new ResearchPipelineJob(config, collection, false);
      assertNotNull(job);
      assertFalse(job.isUseDefaults());
    }

    @Test
    @DisplayName("should be serializable (Flink requirement)")
    void isSerializable() throws Exception {
      AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
      String collection = randomCollectionName();

      ResearchPipelineJob original = new ResearchPipelineJob(config, collection, true);
      ResearchPipelineJob deserialized = roundTrip(original);

      assertNotNull(deserialized);
      assertEquals(collection, deserialized.getCollectionName());
      assertTrue(deserialized.isUseDefaults());
    }

    @Test
    @DisplayName("buildResearchAgent should produce RESEARCHER-type agent with correct tools")
    void buildResearchAgentHasCorrectTools() {
      AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
      ResearchPipelineJob job = new ResearchPipelineJob(config, randomCollectionName(), true);

      Agent agent = job.buildResearchAgent();

      assertNotNull(agent);
      assertEquals("research-agent", agent.getAgentId());
      assertEquals("Research Specialist", agent.getAgentName());
      assertEquals(AgentType.RESEARCHER, agent.getAgentType());
      assertTrue(agent.getAllowedTools().contains("semantic_search"),
          "Agent should have semantic_search tool");
      assertTrue(agent.getAllowedTools().contains("rag"),
          "Agent should have rag tool");
      assertTrue(agent.getRequiredTools().contains("semantic_search"),
          "semantic_search should be a required tool");
      assertEquals(10, agent.getMaxIterations());
      assertTrue(agent.getSystemPrompt().contains("research specialist"),
          "System prompt should mention research specialist");
    }

    @Test
    @DisplayName("buildResearchToolRegistry should contain semantic_search and rag tools")
    void buildResearchToolRegistryContainsTools() {
      AgenticFlinkConfig config = AgenticFlinkConfig.forTesting();
      ResearchPipelineJob job = new ResearchPipelineJob(config, randomCollectionName(), true);

      ToolRegistry registry = job.buildResearchToolRegistry();
      assertNotNull(registry);

      // Verify the registry has the expected tools
      assertTrue(registry.hasTool("semantic_search"),
          "Registry should contain semantic_search tool");
      assertTrue(registry.hasTool("rag"),
          "Registry should contain rag tool");
    }

    @RepeatedTest(3)
    @DisplayName("should construct with randomized collection name and serialize")
    void randomizedConstructionAndSerialization() throws Exception {
      Map<String, String> props = new HashMap<>();
      props.put(ConfigKeys.OLLAMA_BASE_URL, "http://host-" + UUID.randomUUID().toString().substring(0, 6) + ":11434");
      props.put(ConfigKeys.QDRANT_HOST, "qdrant-" + UUID.randomUUID().toString().substring(0, 6));
      AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
      String collection = randomCollectionName();
      boolean useDefaults = ThreadLocalRandom.current().nextBoolean();

      ResearchPipelineJob original = new ResearchPipelineJob(config, collection, useDefaults);
      ResearchPipelineJob deserialized = roundTrip(original);

      assertNotNull(deserialized);
      assertEquals(collection, deserialized.getCollectionName());
      assertEquals(useDefaults, deserialized.isUseDefaults());
    }
  }

  // ==================== Tool Executor Injectable Constructor Tests ====================

  @Nested
  @DisplayName("Tool executor injectable constructors")
  class ToolExecutorTests {

    @Test
    @DisplayName("DocumentIngestionToolExecutor should accept custom providers")
    void documentIngestionAcceptsCustomProviders() {
      DocumentIngestionToolExecutor executor = new DocumentIngestionToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore());
      assertNotNull(executor);
      assertEquals("document_ingestion", executor.getToolId());
    }

    @Test
    @DisplayName("SemanticSearchToolExecutor should accept custom providers")
    void semanticSearchAcceptsCustomProviders() {
      SemanticSearchToolExecutor executor = new SemanticSearchToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore());
      assertNotNull(executor);
      assertEquals("semantic_search", executor.getToolId());
    }

    @Test
    @DisplayName("RagToolExecutor should accept custom providers")
    void ragAcceptsCustomProviders() {
      RagToolExecutor executor = new RagToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore(), new EchoChatConnection());
      assertNotNull(executor);
      assertEquals("rag", executor.getToolId());
    }

    @Test
    @DisplayName("DocumentIngestionToolExecutor with defaults should ingest a document")
    void documentIngestionWithDefaultsCanIngest() throws Exception {
      DocumentIngestionToolExecutor executor = new DocumentIngestionToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore());

      Map<String, Object> params = new HashMap<>();
      params.put("content", "Apache Flink is a stream processing framework. " + UUID.randomUUID());
      params.put("chunk_size", 100);
      params.put("chunk_overlap", 10);

      Object result = executor.execute(params).join();
      assertNotNull(result);
      assertInstanceOf(Map.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = (Map<String, Object>) result;
      assertTrue((int) resultMap.get("segments_created") > 0,
          "Should have created at least one segment");
      assertTrue((int) resultMap.get("embeddings_created") > 0,
          "Should have created at least one embedding");
      assertNotNull(resultMap.get("stored_ids"),
          "Should have stored embedding IDs");
    }

    @Test
    @DisplayName("SemanticSearchToolExecutor with defaults should search after ingestion")
    void semanticSearchWithDefaultsCanSearch() throws Exception {
      HashEmbeddingConnection embedding = new HashEmbeddingConnection();
      InMemoryVectorStore store = newVectorStore();

      // Ingest a document first. Keep it short so the chunker produces a single chunk whose text
      // matches the query exactly, which the deterministic hash embedder ranks first.
      DocumentIngestionToolExecutor ingester = new DocumentIngestionToolExecutor(
          new HashMap<>(), embedding, store);
      String uniqueContent = "streaming data pipelines " + UUID.randomUUID();
      Map<String, Object> ingestParams = new HashMap<>();
      ingestParams.put("content", uniqueContent);
      ingestParams.put("chunk_size", 200);
      ingestParams.put("chunk_overlap", 20);
      Object ingestResult = ingester.execute(ingestParams).join();
      assertNotNull(ingestResult, "Ingestion should succeed before search");

      // Search for it. The exact ingested text round-trips to the same hash vector, so it is the
      // top hit with score ~1.0.
      SemanticSearchToolExecutor searcher = new SemanticSearchToolExecutor(
          new HashMap<>(), embedding, store);
      Map<String, Object> searchParams = new HashMap<>();
      searchParams.put("query", uniqueContent);
      searchParams.put("max_results", 5);
      searchParams.put("min_score", 0.0);
      Object result = searcher.execute(searchParams).join();

      assertNotNull(result);
      assertInstanceOf(Map.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = (Map<String, Object>) result;
      assertNotNull(resultMap.get("results"), "Should return results list");
      assertNotNull(resultMap.get("result_count"), "Should return result count");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> results = (List<Map<String, Object>>) resultMap.get("results");
      assertFalse(results.isEmpty(), "Round-trip search should return the ingested chunk");
      assertEquals(uniqueContent, results.get(0).get("text"),
          "Exact-text match should rank first under the deterministic hash embedder");
    }

    @Test
    @DisplayName("RagToolExecutor with defaults should perform retrieval-augmented generation")
    void ragWithDefaultsCanGenerate() throws Exception {
      HashEmbeddingConnection embedding = new HashEmbeddingConnection();
      InMemoryVectorStore store = newVectorStore();
      EchoChatConnection chat = new EchoChatConnection();

      // Ingest a document first.
      DocumentIngestionToolExecutor ingester = new DocumentIngestionToolExecutor(
          new HashMap<>(), embedding, store);
      String uniqueContent = "exactly-once processing semantics " + UUID.randomUUID();
      Map<String, Object> ingestParams = new HashMap<>();
      ingestParams.put("content", uniqueContent);
      ingestParams.put("chunk_size", 200);
      ingestParams.put("chunk_overlap", 20);
      ingester.execute(ingestParams).join();

      // Run RAG query. The exact ingested text retrieves its own chunk, and the offline echo chat
      // connection produces a deterministic answer with no live LLM.
      RagToolExecutor rag = new RagToolExecutor(
          new HashMap<>(), embedding, store, chat);
      Map<String, Object> ragParams = new HashMap<>();
      ragParams.put("query", uniqueContent);
      ragParams.put("max_results", 5);
      ragParams.put("min_score", 0.0);
      Object result = rag.execute(ragParams).join();

      assertNotNull(result);
      assertInstanceOf(Map.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = (Map<String, Object>) result;
      assertNotNull(resultMap.get("query"), "Should echo the query");
      assertNotNull(resultMap.get("answer"), "Should produce an answer");
      assertNotNull(resultMap.get("sources"), "Should cite retrieved sources");
    }

    @Test
    @DisplayName("DocumentIngestionToolExecutor should validate parameters correctly")
    void documentIngestionValidatesParameters() {
      DocumentIngestionToolExecutor executor = new DocumentIngestionToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore());

      // Valid parameters
      Map<String, Object> validParams = new HashMap<>();
      validParams.put("content", "Some content " + UUID.randomUUID());
      assertTrue(executor.validateParameters(validParams), "Should accept valid parameters");

      // Missing content
      Map<String, Object> missingContent = new HashMap<>();
      missingContent.put("chunk_size", 100);
      assertFalse(executor.validateParameters(missingContent), "Should reject missing content");

      // Null parameters
      assertFalse(executor.validateParameters(null), "Should reject null parameters");
    }

    @Test
    @DisplayName("SemanticSearchToolExecutor should validate parameters correctly")
    void semanticSearchValidatesParameters() {
      SemanticSearchToolExecutor executor = new SemanticSearchToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore());

      // Valid parameters
      Map<String, Object> validParams = new HashMap<>();
      validParams.put("query", "search query " + UUID.randomUUID());
      assertTrue(executor.validateParameters(validParams), "Should accept valid parameters");

      // Missing query
      Map<String, Object> missingQuery = new HashMap<>();
      missingQuery.put("max_results", 10);
      assertFalse(executor.validateParameters(missingQuery), "Should reject missing query");

      // Non-string query
      Map<String, Object> nonStringQuery = new HashMap<>();
      nonStringQuery.put("query", 42);
      assertFalse(executor.validateParameters(nonStringQuery), "Should reject non-string query");
    }

    @Test
    @DisplayName("RagToolExecutor should validate parameters correctly")
    void ragValidatesParameters() {
      RagToolExecutor executor = new RagToolExecutor(
          new HashMap<>(), new HashEmbeddingConnection(), newVectorStore(),
          new EchoChatConnection());

      // Valid parameters
      Map<String, Object> validParams = new HashMap<>();
      validParams.put("query", "rag query " + UUID.randomUUID());
      assertTrue(executor.validateParameters(validParams), "Should accept valid parameters");

      // Missing query
      assertFalse(executor.validateParameters(new HashMap<>()), "Should reject empty params");
    }
  }

  // ==================== End-to-End Integration Tests ====================

  @Nested
  @DisplayName("End-to-end integration (in-memory)")
  class EndToEndTests {

    @Test
    @DisplayName("should ingest via DocumentIngestionFunction then search via SemanticSearchFunction")
    void ingestThenSearch() throws Exception {
      // Step 1: Ingest a document via DocumentIngestionFunction
      DocumentIngestionFunction ingestionFn = new DocumentIngestionFunction(new HashMap<>(), true);
      ingestionFn.open((OpenContext) null);

      AgentEvent ingestEvent = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
      String uniqueContent = "Stateful stream processing enables complex event processing "
          + "and windowed aggregations with fault tolerance. " + UUID.randomUUID();
      ingestEvent.putData("document_content", uniqueContent);
      ingestEvent.putData("chunk_size", 200);
      ingestEvent.putData("chunk_overlap", 20);

      CapturingResultFuture ingestResult = new CapturingResultFuture();
      ingestionFn.asyncInvoke(ingestEvent, ingestResult);

      // Wait for ingestion
      long deadline = System.currentTimeMillis() + 10_000;
      while (ingestResult.getResults() == null && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertNotNull(ingestResult.getResults(), "Ingestion should complete");
      AgentEvent ingestResultEvent = ingestResult.getResults().iterator().next();
      assertEquals(AgentEventType.TOOL_CALL_COMPLETED, ingestResultEvent.getEventType(),
          "Ingestion should succeed");
      ingestionFn.close();

      // Step 2: Search via SemanticSearchFunction
      SemanticSearchFunction searchFn = new SemanticSearchFunction(new HashMap<>(), true);
      searchFn.open((OpenContext) null);

      AgentEvent searchEvent = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
      searchEvent.putData("query", "stream processing");
      searchEvent.putData("max_results", 5);
      searchEvent.putData("min_score", 0.0);

      CapturingResultFuture searchResult = new CapturingResultFuture();
      searchFn.asyncInvoke(searchEvent, searchResult);

      deadline = System.currentTimeMillis() + 10_000;
      while (searchResult.getResults() == null && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertNotNull(searchResult.getResults(), "Search should complete");
      AgentEvent searchResultEvent = searchResult.getResults().iterator().next();
      assertEquals(AgentEventType.TOOL_CALL_COMPLETED, searchResultEvent.getEventType(),
          "Search should succeed");
      assertEquals("semantic_search", searchResultEvent.getCurrentStage());
      searchFn.close();
    }

    @Test
    @DisplayName("should preserve event flow metadata through ingestion pipeline")
    void preservesEventMetadata() throws Exception {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        String expectedFlowId = event.getFlowId();
        String expectedUserId = event.getUserId();
        String expectedAgentId = event.getAgentId();
        event.setCorrelationId("corr-" + UUID.randomUUID());
        event.setParentFlowId("parent-" + UUID.randomUUID());
        String expectedCorrelationId = event.getCorrelationId();
        String expectedParentFlowId = event.getParentFlowId();

        event.putData("document_content", "Metadata preservation test " + UUID.randomUUID());

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        long deadline = System.currentTimeMillis() + 10_000;
        while (resultFuture.getResults() == null && System.currentTimeMillis() < deadline) {
          Thread.sleep(50);
        }

        assertNotNull(resultFuture.getResults());
        AgentEvent result = resultFuture.getResults().iterator().next();

        assertEquals(expectedFlowId, result.getFlowId(), "flowId should be preserved");
        assertEquals(expectedUserId, result.getUserId(), "userId should be preserved");
        assertEquals(expectedAgentId, result.getAgentId(), "agentId should be preserved");
        assertEquals(expectedCorrelationId, result.getCorrelationId(),
            "correlationId should be preserved");
        assertEquals(expectedParentFlowId, result.getParentFlowId(),
            "parentFlowId should be preserved");
      } finally {
        function.close();
      }
    }

    @Test
    @DisplayName("should preserve event flow metadata through search pipeline")
    void preservesSearchEventMetadata() throws Exception {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      function.open((OpenContext) null);

      try {
        AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_REQUESTED);
        String expectedFlowId = event.getFlowId();
        String expectedUserId = event.getUserId();
        String expectedAgentId = event.getAgentId();
        event.setCorrelationId("corr-" + UUID.randomUUID());
        event.setParentFlowId("parent-" + UUID.randomUUID());
        String expectedCorrelationId = event.getCorrelationId();
        String expectedParentFlowId = event.getParentFlowId();

        event.putData("query", "metadata test " + UUID.randomUUID());
        event.putData("min_score", 0.0);

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        long deadline = System.currentTimeMillis() + 10_000;
        while (resultFuture.getResults() == null && System.currentTimeMillis() < deadline) {
          Thread.sleep(50);
        }

        assertNotNull(resultFuture.getResults());
        AgentEvent result = resultFuture.getResults().iterator().next();

        assertEquals(expectedFlowId, result.getFlowId(), "flowId should be preserved");
        assertEquals(expectedUserId, result.getUserId(), "userId should be preserved");
        assertEquals(expectedAgentId, result.getAgentId(), "agentId should be preserved");
        assertEquals(expectedCorrelationId, result.getCorrelationId(),
            "correlationId should be preserved");
        assertEquals(expectedParentFlowId, result.getParentFlowId(),
            "parentFlowId should be preserved");
      } finally {
        function.close();
      }
    }
  }

  // ==================== Lifecycle Tests ====================

  @Nested
  @DisplayName("Lifecycle")
  class LifecycleTests {

    @Test
    @DisplayName("DocumentIngestionFunction should allow open then close without invoke")
    void ingestionOpenAndCloseWithoutInvoke() throws Exception {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      function.open((OpenContext) null);
      assertDoesNotThrow(() -> function.close());
    }

    @Test
    @DisplayName("SemanticSearchFunction should allow open then close without invoke")
    void searchOpenAndCloseWithoutInvoke() throws Exception {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      function.open((OpenContext) null);
      assertDoesNotThrow(() -> function.close());
    }

    @Test
    @DisplayName("DocumentIngestionFunction should handle close without prior open")
    void ingestionCloseWithoutOpen() {
      DocumentIngestionFunction function = new DocumentIngestionFunction(new HashMap<>(), true);
      assertDoesNotThrow(() -> function.close());
    }

    @Test
    @DisplayName("SemanticSearchFunction should handle close without prior open")
    void searchCloseWithoutOpen() {
      SemanticSearchFunction function = new SemanticSearchFunction(new HashMap<>(), true);
      assertDoesNotThrow(() -> function.close());
    }
  }
}
