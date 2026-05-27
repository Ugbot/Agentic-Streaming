package org.agentic.flink.job;

import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.embedding.HashEmbeddingConnection;
import org.agentic.flink.function.DocumentIngestionFunction;
import org.agentic.flink.function.SemanticSearchFunction;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.agentic.flink.stream.AgentExecutionFunction;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.rag.RagToolExecutor;
import org.agentic.flink.tools.rag.SemanticSearchToolExecutor;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Research pipeline job that orchestrates document ingestion and semantic search
 * as two independent Flink DataStreams.
 *
 * <p>This job provides two execution modes:
 * <ul>
 *   <li><b>Direct search</b> ({@link #execute}) -- documents are ingested and queries
 *       run semantic search directly against the vector store without an LLM in the
 *       recall path. This is the lightweight, low-latency option.</li>
 *   <li><b>Agent-based recall</b> ({@link #executeWithAgent}) -- the query stream is
 *       processed by a RESEARCHER-type agent equipped with {@code semantic_search} and
 *       {@code rag} tools, so the LLM can synthesize answers from retrieved context.</li>
 * </ul>
 *
 * <p>When {@code useDefaults} is {@code true}, in-memory embedding model and store
 * implementations are used, so the pipeline can run without any external infrastructure
 * (Ollama, Qdrant, etc.). This is useful for local development and integration tests.
 *
 * <p><b>Usage:</b>
 * <pre>{@code
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();
 *
 * DataStream<AgentEvent> documents = ...; // document events
 * DataStream<AgentEvent> queries   = ...; // query events
 *
 * ResearchPipelineJob pipeline = new ResearchPipelineJob(config, "my-collection", false);
 * pipeline.execute(env, documents, queries);
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see DocumentIngestionFunction
 * @see SemanticSearchFunction
 */
public class ResearchPipelineJob implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ResearchPipelineJob.class);

    private final AgenticFlinkConfig config;
    private final String collectionName;
    private final boolean useDefaults;

    /**
     * Creates a new research pipeline job.
     *
     * @param config         framework configuration (Ollama, Qdrant, etc.)
     * @param collectionName the vector store collection to ingest into and search against
     * @param useDefaults    when {@code true}, use in-memory defaults instead of external services
     */
    public ResearchPipelineJob(AgenticFlinkConfig config, String collectionName, boolean useDefaults) {
        this.config = config;
        this.collectionName = collectionName;
        this.useDefaults = useDefaults;
    }

    // ==================== Execution Mode 1: Direct Search ====================

    /**
     * Executes the pipeline with direct semantic search (no LLM in the recall path).
     *
     * <p>Stream 1 ingests documents into the vector store asynchronously.
     * Stream 2 performs semantic search queries against the same store.
     *
     * @param env            the Flink execution environment
     * @param documentSource stream of document events to ingest
     * @param querySource    stream of query events to search
     * @throws Exception if the Flink job fails to execute
     */
    public void execute(StreamExecutionEnvironment env,
                        DataStream<AgentEvent> documentSource,
                        DataStream<AgentEvent> querySource) throws Exception {

        LOG.info("Starting Research Pipeline (direct search) for collection: {}", collectionName);

        Map<String, String> toolConfig = buildToolConfig();

        // Stream 1: Document ingestion -- unordered for maximum throughput
        DataStream<AgentEvent> ingestionResults = AsyncDataStream.unorderedWait(
                documentSource,
                new DocumentIngestionFunction(toolConfig, useDefaults),
                30000, TimeUnit.MILLISECONDS, 100
        ).name("document-ingestion");

        ingestionResults.print().name("ingestion-results");

        // Stream 2: Semantic search recall -- ordered to preserve query sequence
        DataStream<AgentEvent> searchResults = AsyncDataStream.orderedWait(
                querySource,
                new SemanticSearchFunction(toolConfig, useDefaults),
                15000, TimeUnit.MILLISECONDS, 50
        ).name("semantic-search");

        searchResults.print().name("search-results");

        env.execute("Research Pipeline - " + collectionName);
    }

    // ==================== Execution Mode 2: Agent-Based Recall ====================

    /**
     * Executes the pipeline with an LLM-backed research agent for the query stream.
     *
     * <p>Stream 1 is identical to {@link #execute} (document ingestion).
     * Stream 2 routes queries through a RESEARCHER agent that has access to
     * {@code semantic_search} and {@code rag} tools, enabling the LLM to synthesize
     * answers from retrieved context.
     *
     * @param env            the Flink execution environment
     * @param documentSource stream of document events to ingest
     * @param querySource    stream of query events for the research agent
     * @throws Exception if the Flink job fails to execute
     */
    public void executeWithAgent(StreamExecutionEnvironment env,
                                 DataStream<AgentEvent> documentSource,
                                 DataStream<AgentEvent> querySource) throws Exception {

        LOG.info("Starting Research Pipeline (agent-based recall) for collection: {}", collectionName);

        Map<String, String> toolConfig = buildToolConfig();

        // Stream 1: Document ingestion (same as direct mode)
        DataStream<AgentEvent> ingestionResults = AsyncDataStream.unorderedWait(
                documentSource,
                new DocumentIngestionFunction(toolConfig, useDefaults),
                30000, TimeUnit.MILLISECONDS, 100
        ).name("document-ingestion");

        ingestionResults.print().name("ingestion-results");

        // Stream 2: Agent-based query processing
        Agent researchAgent = buildResearchAgent();
        ToolRegistry toolRegistry = buildResearchToolRegistry();

        LLMClient llmClient = LLMClient.builder()
                .withModel(config.get(ConfigKeys.OLLAMA_MODEL, ConfigKeys.DEFAULT_OLLAMA_MODEL))
                .withBaseUrl(config.get(ConfigKeys.OLLAMA_BASE_URL, ConfigKeys.DEFAULT_OLLAMA_BASE_URL))
                .withTemperature(0.4)
                .withTimeout(Duration.ofSeconds(60))
                .build();

        DataStream<AgentEvent> agentResults = AsyncDataStream.orderedWait(
                querySource,
                new AgentExecutionFunction(researchAgent, toolRegistry, llmClient),
                60000, TimeUnit.MILLISECONDS, 20
        ).name("research-agent-execution");

        agentResults.print().name("agent-results");

        env.execute("Research Pipeline (Agent) - " + collectionName);
    }

    // ==================== Agent & Tool Builders ====================

    /**
     * Builds a RESEARCHER-type agent configured for knowledge base search and synthesis.
     *
     * @return an immutable Agent definition
     */
    public Agent buildResearchAgent() {
        return Agent.builder()
                .withId("research-agent")
                .withName("Research Specialist")
                .withType(AgentType.RESEARCHER)
                .withSystemPrompt(
                        "You are a research specialist that answers questions by searching a knowledge base. "
                        + "When you receive a query:\n"
                        + "1. Use TOOL_CALL: semantic_search {\"query\": \"<your search query>\"} to find relevant documents.\n"
                        + "2. If the results are sufficient, synthesize a clear, concise answer.\n"
                        + "3. If you need a more detailed answer with full context retrieval and LLM generation, "
                        + "use TOOL_CALL: rag {\"query\": \"<your question>\"}.\n"
                        + "4. Always cite the source documents and their relevance scores.\n"
                        + "5. If no relevant documents are found, state that clearly.")
                .withChatSetup(ChatSetup.builder().withModel(config.get(ConfigKeys.OLLAMA_MODEL, ConfigKeys.DEFAULT_OLLAMA_MODEL)).withTemperature(0.4).build())
                .withMaxIterations(10)
                .withTimeout(Duration.ofMinutes(3))
                .withTools("semantic_search", "rag")
                .withRequiredTools("semantic_search")
                .withStateMachine(buildResearchStateMachine())
                .build();
    }

    /**
     * Builds a valid state machine for the research agent.
     *
     * <p>The standard transitions generated by {@code withStandardTransitions()} do not include
     * transitions from the INITIALIZED state or several other non-terminal states. This method
     * builds a complete state machine with all required transitions to pass validation.
     */
    private AgentStateMachine buildResearchStateMachine() {
        return AgentStateMachine.builder()
                .withId("research-agent-sm")
                .withGlobalTimeout(180)
                // INITIALIZED transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.INITIALIZED)
                        .to(AgentState.VALIDATING)
                        .on(AgentEventType.VALIDATION_REQUESTED)
                        .withDescription("Start validation from initialized")
                        .build())
                .addTransition(AgentTransition.builder()
                        .from(AgentState.INITIALIZED)
                        .to(AgentState.EXECUTING)
                        .on(AgentEventType.FLOW_STARTED)
                        .withDescription("Start execution from initialized")
                        .build())
                // Validation transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.VALIDATING)
                        .to(AgentState.EXECUTING)
                        .on(AgentEventType.VALIDATION_PASSED)
                        .withDescription("Validation passed")
                        .build())
                .addTransition(AgentTransition.builder()
                        .from(AgentState.VALIDATING)
                        .to(AgentState.CORRECTING)
                        .on(AgentEventType.VALIDATION_FAILED)
                        .withDescription("Validation failed, attempting correction")
                        .withPriority(10)
                        .build())
                // Correction transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.CORRECTING)
                        .to(AgentState.EXECUTING)
                        .on(AgentEventType.CORRECTION_COMPLETED)
                        .withDescription("Correction completed")
                        .build())
                // Execution transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.EXECUTING)
                        .to(AgentState.SUPERVISOR_REVIEW)
                        .on(AgentEventType.SUPERVISOR_REVIEW_REQUESTED)
                        .withDescription("Execution complete, supervisor review")
                        .withPriority(10)
                        .build())
                .addTransition(AgentTransition.builder()
                        .from(AgentState.EXECUTING)
                        .to(AgentState.COMPLETED)
                        .on(AgentEventType.FLOW_COMPLETED)
                        .withDescription("Execution complete, no review")
                        .withPriority(5)
                        .build())
                // Supervisor transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.SUPERVISOR_REVIEW)
                        .to(AgentState.COMPLETED)
                        .on(AgentEventType.SUPERVISOR_APPROVED)
                        .withDescription("Supervisor approved")
                        .build())
                .addTransition(AgentTransition.builder()
                        .from(AgentState.SUPERVISOR_REVIEW)
                        .to(AgentState.CORRECTING)
                        .on(AgentEventType.SUPERVISOR_REJECTED)
                        .withDescription("Supervisor rejected, correcting")
                        .build())
                // Paused transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.PAUSED)
                        .to(AgentState.EXECUTING)
                        .on(AgentEventType.FLOW_RESUMED)
                        .withDescription("Resume from paused")
                        .build())
                // Offloading transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.OFFLOADING)
                        .to(AgentState.EXECUTING)
                        .on(AgentEventType.STATE_OFFLOADED)
                        .withDescription("Return from offloading")
                        .build())
                // Compensating transitions
                .addTransition(AgentTransition.builder()
                        .from(AgentState.COMPENSATING)
                        .to(AgentState.COMPENSATED)
                        .on(AgentEventType.COMPENSATION_COMPLETED)
                        .withDescription("Compensation completed")
                        .build())
                .build();
    }

    /**
     * Builds a ToolRegistry containing the semantic search and RAG tool executors.
     *
     * <p>When {@code useDefaults} is true, the executors are configured with in-memory
     * embedding model, embedding store, and language model implementations.
     *
     * @return a ToolRegistry with semantic_search and rag tools registered
     */
    public ToolRegistry buildResearchToolRegistry() {
        Map<String, String> toolConfig = buildToolConfig();

        ToolRegistry.ToolRegistryBuilder builder = ToolRegistry.builder();

        if (useDefaults) {
            // Zero-infra defaults: deterministic hash embedder + shared in-JVM vector store so
            // both tools observe the same corpus. The chat connection is only exercised by the
            // RAG generation step, which the offline tests drive with min_score gating.
            HashEmbeddingConnection embeddingConnection = new HashEmbeddingConnection();
            InMemoryVectorStore vectorStore = new InMemoryVectorStore();
            vectorStore.initialize(toolConfig);
            LangChain4jChatConnection chatConnection = LangChain4jChatConnection.ollama(
                    config.get(ConfigKeys.OLLAMA_BASE_URL, ConfigKeys.DEFAULT_OLLAMA_BASE_URL));

            builder.registerTool("semantic_search",
                    new SemanticSearchToolExecutor(toolConfig, embeddingConnection, vectorStore));
            builder.registerTool("rag",
                    new RagToolExecutor(toolConfig, embeddingConnection, vectorStore, chatConnection));
        } else {
            builder.registerTool("semantic_search",
                    new SemanticSearchToolExecutor(toolConfig));
            builder.registerTool("rag",
                    new RagToolExecutor(toolConfig));
        }

        return builder.build();
    }

    // ==================== Configuration ====================

    /**
     * Builds the tool configuration map from the framework config and constructor parameters.
     *
     * <p>Maps framework config keys to the flat keys that tool executors expect:
     * <ul>
     *   <li>{@code baseUrl} -- Ollama base URL for embedding model</li>
     *   <li>{@code modelName} -- embedding model name</li>
     *   <li>{@code host} -- Qdrant host</li>
     *   <li>{@code port} -- Qdrant port</li>
     *   <li>{@code collectionName} -- vector collection name</li>
     * </ul>
     *
     * @return tool configuration map
     */
    private Map<String, String> buildToolConfig() {
        Map<String, String> toolConfig = new HashMap<>();
        toolConfig.put("baseUrl", config.get(ConfigKeys.OLLAMA_BASE_URL, ConfigKeys.DEFAULT_OLLAMA_BASE_URL));
        toolConfig.put("modelName", "nomic-embed-text:latest");
        toolConfig.put("host", config.get(ConfigKeys.QDRANT_HOST, ConfigKeys.DEFAULT_QDRANT_HOST));
        toolConfig.put("port", config.get(ConfigKeys.QDRANT_PORT, ConfigKeys.DEFAULT_QDRANT_PORT));
        toolConfig.put("collectionName", collectionName);
        return toolConfig;
    }

    // ==================== Accessors ====================

    /**
     * Returns the collection name this pipeline operates on.
     *
     * @return the vector store collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns whether this pipeline uses in-memory defaults.
     *
     * @return true if in-memory implementations are used
     */
    public boolean isUseDefaults() {
        return useDefaults;
    }
}
