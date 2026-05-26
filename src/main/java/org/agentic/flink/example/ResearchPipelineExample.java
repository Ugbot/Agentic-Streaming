package org.agentic.flink.example;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.job.ResearchPipelineJob;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example demonstrating the Research Pipeline with document ingestion and semantic search.
 *
 * <p>This example runs entirely with in-memory defaults, so no external infrastructure
 * (Ollama, Qdrant, etc.) is required. It ingests a small corpus of technical documents
 * into an in-memory vector store, then executes semantic search queries against them.
 *
 * <p><b>What it demonstrates:</b>
 * <ul>
 *   <li>Two independent Flink DataStreams (ingestion + search) in a single job</li>
 *   <li>Async document ingestion with chunking and embedding</li>
 *   <li>Async semantic search with cosine similarity matching</li>
 *   <li>In-memory defaults for infrastructure-free local execution</li>
 * </ul>
 *
 * <p><b>To run:</b>
 * <pre>
 * mvn exec:java -Dexec.mainClass="org.agentic.flink.example.ResearchPipelineExample"
 * </pre>
 *
 * @author Agentic Flink Team
 * @see ResearchPipelineJob
 */
public class ResearchPipelineExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        AgenticFlinkConfig config = AgenticFlinkConfig.fromEnvironment();

        // Stream 1: Documents to ingest into the knowledge base
        DataStream<AgentEvent> documents = env.fromElements(
                createDocumentEvent("doc-001",
                        "Apache Flink is a distributed stream processing framework designed for "
                        + "stateful computations over unbounded and bounded data streams. It supports "
                        + "exactly-once processing semantics and runs in all common cluster environments "
                        + "at in-memory speed and at any scale."),
                createDocumentEvent("doc-002",
                        "Complex Event Processing (CEP) detects meaningful patterns in continuous event "
                        + "streams by applying temporal and logical constraints. Apache Flink's CEP library "
                        + "provides a rich pattern API that supports sequence detection, iteration, and "
                        + "time-windowed conditions over keyed streams."),
                createDocumentEvent("doc-003",
                        "The Saga pattern ensures data consistency across microservices without requiring "
                        + "distributed transactions. Each service executes a local transaction and publishes "
                        + "an event; if a step fails, compensating transactions are triggered in reverse order "
                        + "to undo the preceding successful steps."),
                createDocumentEvent("doc-004",
                        "Large Language Models (LLMs) can be augmented with external tools for autonomous "
                        + "task execution in what is commonly called agentic AI. The agent loop alternates "
                        + "between LLM reasoning and tool invocation, allowing the model to gather information, "
                        + "perform actions, and refine its output iteratively."),
                createDocumentEvent("doc-005",
                        "Vector databases store high-dimensional embeddings and support efficient approximate "
                        + "nearest-neighbor search for similarity retrieval. They are a key building block for "
                        + "Retrieval-Augmented Generation (RAG) pipelines, enabling semantic search over large "
                        + "document corpora with sub-second latency.")
        );

        // Stream 2: Queries to run against the ingested knowledge base
        DataStream<AgentEvent> queries = env.fromElements(
                createQueryEvent("q-001", "What is Apache Flink?"),
                createQueryEvent("q-002", "How does CEP detect patterns?"),
                createQueryEvent("q-003", "Explain the Saga pattern for distributed systems"),
                createQueryEvent("q-004", "What are vector databases used for?")
        );

        // Execute with in-memory defaults (no external infrastructure needed)
        ResearchPipelineJob pipeline = new ResearchPipelineJob(config, "research-kb", true);
        pipeline.execute(env, documents, queries);
    }

    /**
     * Creates a document ingestion event with the given ID and content.
     *
     * @param docId   unique document identifier
     * @param content document text to ingest
     * @return an AgentEvent configured for document ingestion
     */
    private static AgentEvent createDocumentEvent(String docId, String content) {
        AgentEvent event = new AgentEvent(
                "ingest-" + docId,
                "system",
                "ingestion",
                AgentEventType.TOOL_CALL_REQUESTED
        );
        event.putData("document_content", content);
        event.putData("document_id", docId);
        event.putData("source", "example-corpus");
        return event;
    }

    /**
     * Creates a semantic search query event.
     *
     * @param queryId unique query identifier
     * @param query   the natural-language search query
     * @return an AgentEvent configured for semantic search
     */
    private static AgentEvent createQueryEvent(String queryId, String query) {
        AgentEvent event = new AgentEvent(
                "search-" + queryId,
                "researcher",
                "research-agent",
                AgentEventType.TOOL_CALL_REQUESTED
        );
        event.putData("query", query);
        return event;
    }
}
