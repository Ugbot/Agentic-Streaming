# RAG Tools for Agentic Framework

Production-ready RAG (Retrieval-Augmented Generation) tools built on LangChain4J and integrated with the agentic framework.

## Overview

The RAG toolkit provides four powerful tools for knowledge management and semantic search:

1. **Document Ingestion** - Ingest and vectorize documents
2. **Semantic Search** - Search knowledge base by similarity
3. **RAG Query** - Answer questions with context retrieval
4. **Embedding** - Generate text embeddings

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      RAG Tool Flow                           │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Documents ──> Chunking ──> Embedding ──> Vector Store       │
│                                                  │            │
│                                                  │            │
│  User Query ──> Embedding ──> Similarity Search │            │
│                                     │                         │
│                                     v                         │
│                              Retrieved Docs                   │
│                                     │                         │
│                                     v                         │
│                            LLM with Context                   │
│                                     │                         │
│                                     v                         │
│                                 Answer                        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Tool Implementations

### 1. Document Ingestion Tool

**ID:** `document_ingestion`

**Purpose:** Ingests documents into the vector knowledge base

**Process:**
1. Split document into chunks
2. Generate embeddings for each chunk
3. Store in vector database (Qdrant)

**Parameters:**
```json
{
  "content": "string (required) - Document text to ingest",
  "chunk_size": "number (optional, default: 500) - Size of each chunk",
  "chunk_overlap": "number (optional, default: 50) - Overlap between chunks",
  "metadata": "object (optional) - Additional metadata"
}
```

**Example:**
```java
Map<String, Object> params = new HashMap<>();
params.put("content", "Apache Flink is a distributed processing engine...");
params.put("chunk_size", 200);
params.put("chunk_overlap", 20);

Map<String, String> metadata = new HashMap<>();
metadata.put("source", "documentation");
metadata.put("version", "1.17");
params.put("metadata", metadata);

ToolCallRequest request = new ToolCallRequest(
    "flow-001", "user-001", "agent-001",
    "document_ingestion", params
);
```

**Response:**
```json
{
  "segments_created": 15,
  "embeddings_created": 15,
  "stored_ids": ["id1", "id2", ...],
  "chunk_size": 200,
  "chunk_overlap": 20
}
```

### 2. Semantic Search Tool

**ID:** `semantic_search`

**Purpose:** Search knowledge base using semantic similarity

**Process:**
1. Generate embedding for query
2. Perform similarity search in vector store
3. Return ranked results

**Parameters:**
```json
{
  "query": "string (required) - Search query",
  "max_results": "number (optional, default: 10) - Maximum results",
  "min_score": "number (optional, default: 0.7) - Minimum similarity score"
}
```

**Example:**
```java
Map<String, Object> params = new HashMap<>();
params.put("query", "How does Flink handle state?");
params.put("max_results", 5);
params.put("min_score", 0.75);

ToolCallRequest request = new ToolCallRequest(
    "flow-002", "user-001", "agent-001",
    "semantic_search", params
);
```

**Response:**
```json
{
  "query": "How does Flink handle state?",
  "result_count": 5,
  "results": [
    {
      "text": "Flink provides stateful computations...",
      "score": 0.92,
      "embedding_id": "abc123"
    },
    ...
  ]
}
```

### 3. RAG Query Tool

**ID:** `rag`

**Purpose:** Answer questions using retrieval-augmented generation

**Process:**
1. Generate query embedding
2. Retrieve relevant context from vector store
3. Provide context to LLM
4. Generate answer based on context

**Parameters:**
```json
{
  "query": "string (required) - Question to answer",
  "max_results": "number (optional, default: 5) - Max context docs",
  "min_score": "number (optional, default: 0.7) - Min relevance score"
}
```

**Example:**
```java
Map<String, Object> params = new HashMap<>();
params.put("query", "What are the key features of Apache Flink?");
params.put("max_results", 5);

ToolCallRequest request = new ToolCallRequest(
    "flow-003", "user-001", "agent-001",
    "rag", params
);
```

**Response:**
```json
{
  "query": "What are the key features of Apache Flink?",
  "answer": "Apache Flink's key features include: 1) Stateful stream processing...",
  "sources": [
    {
      "text": "Flink provides exactly-once processing...",
      "score": 0.95
    },
    ...
  ],
  "source_count": 5
}
```

### 4. Embedding Tool

**ID:** `embedding`

**Purpose:** Generate vector embeddings for text

**Process:**
1. Convert text to embedding vector
2. Return embedding metadata

**Parameters:**
```json
{
  "text": "string (required) - Text to embed",
  "return_vector": "boolean (optional, default: false) - Return full vector"
}
```

**Example:**
```java
Map<String, Object> params = new HashMap<>();
params.put("text", "Apache Flink distributed processing");
params.put("return_vector", true);

ToolCallRequest request = new ToolCallRequest(
    "flow-004", "user-001", "agent-001",
    "embedding", params
);
```

**Response:**
```json
{
  "text": "Apache Flink distributed processing",
  "dimension": 768,
  "vector": [0.123, -0.456, 0.789, ...]  // if return_vector is true
}
```

## Configuration

### Embedding Model Configuration

```java
Map<String, String> config = new HashMap<>();
config.put("baseUrl", "http://localhost:11434");  // Ollama endpoint
config.put("modelName", "nomic-embed-text:latest");  // Embedding model
```

### Vector Store Configuration (Qdrant)

```java
config.put("host", "localhost");
config.put("port", "6333");
config.put("apiKey", "your-api-key");  // Optional
config.put("collectionName", "knowledge-base");
```

### Language Model Configuration

```java
config.put("baseUrl", "http://localhost:11434");
config.put("modelName", "llama3.1:latest");  // LLM for generation
```

## Usage Example

### Complete RAG Workflow

```java
// 1. Setup
ToolExecutorRegistry registry = new ToolExecutorRegistry();
Map<String, String> config = createConfig();

registry.register(new DocumentIngestionToolExecutor(config));
registry.register(new SemanticSearchToolExecutor(config));
registry.register(new RagToolExecutor(config));
registry.register(new EmbeddingToolExecutor(config));

// 2. Ingest documents
ToolCallRequest ingestion = createIngestionRequest(
    "flow-001", "user-001",
    "Apache Flink is a framework for stateful computations..."
);

// 3. Search semantically
ToolCallRequest search = createSearchRequest(
    "flow-002", "user-001",
    "How does state management work?", 5
);

// 4. Ask RAG question
ToolCallRequest rag = createRagRequest(
    "flow-003", "user-001",
    "What are the benefits of stateful processing?", 5
);

// 5. Execute in Flink stream
DataStream<ToolCallRequest> requests = env.fromElements(
    ingestion, search, rag
);

DataStream<ToolCallResponse> responses = AsyncDataStream.orderedWait(
    requests,
    new ToolCallAsyncFunctionV2(toolRegistry, registry),
    60000, TimeUnit.MILLISECONDS, 100
);
```

## Integration with Agent Framework

### Register RAG Tools in Agent Config

```java
AgentConfig config = new AgentConfig("rag-agent", "RAG Agent");

// Add RAG tools to allowlist
config.addAllowedTool("document_ingestion");
config.addAllowedTool("semantic_search");
config.addAllowedTool("rag");
config.addAllowedTool("embedding");

// Configure validation for RAG results
config.setEnableValidation(true);
config.setValidationPrompt(
    "Validate that the RAG answer is accurate and based on the provided context."
);
```

### Use in Agentic Workflow

```java
// Create agent execution stream with RAG tools
AgentExecutionStream agentStream = new AgentExecutionStream(
    env, config, toolRegistry
);

// Input events requesting RAG operations
DataStream<AgentEvent> ragEvents = createRagEvents();

// Execute with validation and correction
SingleOutputStreamOperator<AgentEvent> results =
    agentStream.createAgentStream(ragEvents);
```

## Advanced Features

### Document Chunking Strategies

```java
// Recursive splitter (default)
params.put("chunk_size", 500);
params.put("chunk_overlap", 50);

// For code documents
params.put("chunk_size", 1000);
params.put("chunk_overlap", 100);

// For short-form content
params.put("chunk_size", 200);
params.put("chunk_overlap", 20);
```

### Metadata Filtering (Future)

```java
Map<String, String> metadata = new HashMap<>();
metadata.put("source", "documentation");
metadata.put("version", "1.17");
metadata.put("category", "state-management");

// Future: Filter searches by metadata
params.put("metadata_filter", metadata);
```

### Hybrid Search (Future)

Combine semantic search with keyword search for better results.

## Performance Considerations

### Batch Ingestion

For large document sets, batch multiple documents:

```java
// Process documents in parallel
DataStream<ToolCallRequest> batchedIngestion =
    documents
        .map(doc -> createIngestionRequest(flowId, userId, doc))
        .setParallelism(10);
```

### Caching Embeddings

Embeddings are expensive - cache when possible:
- Use Flink state for recent queries
- Cache in Redis for frequently accessed docs
- Pre-compute embeddings for known queries

### Vector Store Optimization

- Create indexes in Qdrant for faster search
- Use quantization for reduced memory footprint
- Batch vector insertions for better throughput

## Monitoring

### Key Metrics

```java
// Track these metrics:
- Ingestion rate (docs/sec)
- Search latency (p50, p99)
- RAG answer quality (validation pass rate)
- Embedding cache hit rate
- Vector store query performance
```

### Logging

All RAG tools log key events:
- Document chunks created
- Embeddings generated
- Search results count
- RAG context size
- Execution duration

## Running the Example

```bash
# 1. Start Ollama
ollama serve

# 2. Pull required models
ollama pull nomic-embed-text:latest
ollama pull llama3.1:latest

# 3. Start Qdrant
docker run -p 6333:6333 qdrant/qdrant

# 4. Build project
mvn clean install

# 5. Run example
java -cp target/agentic-flink-0.0.1-SNAPSHOT.jar \
  org.agentic.flink.example.RagAgentExample
```

## Future Enhancements

- **Multi-modal RAG**: Support images, audio, video
- **Hybrid search**: Combine semantic + keyword search
- **Re-ranking**: LLM-based reranking of results
- **Query expansion**: Expand queries for better recall
- **Citation tracking**: Track which sources were used
- **Update/delete**: Support updating and deleting documents
- **Multi-collection**: Support multiple knowledge bases

## References

- [LangChain4J Documentation](https://docs.langchain4j.dev/)
- [Qdrant Documentation](https://qdrant.tech/documentation/)
- [Ollama Models](https://ollama.ai/library)
- [Apache Flink](https://flink.apache.org/)
