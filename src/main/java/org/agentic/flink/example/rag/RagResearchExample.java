package org.agentic.flink.example.rag;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.djl.DjlEmbeddingConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Scorer;
import org.agentic.flink.inference.djl.DjlInferenceConnection;
import org.agentic.flink.listener.MetricsAgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import org.agentic.flink.memory.vector.FlinkStateVectorMemory;
import org.agentic.flink.memory.vector.ScoredItem;
import org.agentic.flink.memory.vector.VectorMemory;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Retrieval-augmented research assistant on Flink keyed state.
 *
 * <p>What the pipeline does, per query:
 *
 * <ol>
 *   <li>Embed the query with a sentence-transformers model
 *       ({@code DjlEmbeddingConnection}).
 *   <li>Top-k recall over {@link FlinkStateVectorMemory} (brute-force KNN backed by Flink
 *       {@code MapState}).
 *   <li>Rerank the top-k passages with a cross-encoder {@link Scorer}.
 *   <li>Iterate a small ReAct-style loop in the LLM: think, decide whether to retrieve more,
 *       answer when ready.
 * </ol>
 *
 * <p>The knowledge base is seeded once per key on the first event (see
 * {@link RagProcessFunction#open}). In production you'd hydrate from a long-term store or
 * stream new documents in via a {@code Channel<KeyedContextItem>}; the example keeps a tiny in-process corpus
 * so the demo is reproducible.
 *
 * <p>MCP tools are <i>not</i> wired in this demo to keep the runtime self-contained. To add
 * one — for example the official "everything" reference server — see the snippet in
 * {@code docs/examples/rag.md}.
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 *   docker compose up -d ollama
 *   docker compose exec ollama ollama pull qwen2.5:3b
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.rag.RagResearchExample"
 * </pre>
 */
public class RagResearchExample {

  /** A user query into the RAG assistant. Keyed by topic so each topic owns its own KB. */
  public record Query(String topic, String question) {}

  /** The assistant's answer. */
  public record Answer(String topic, String question, String answer, double topScore) {}

  /** Seed corpus per topic, indexed once on the first event for each key. */
  private static final java.util.Map<String, List<String>> SEED_CORPUS =
      java.util.Map.of(
          "flink",
          List.of(
              "Apache Flink is a stream-processing framework with exactly-once state guarantees.",
              "Flink's RocksDB state backend supports incremental checkpoints to S3 or HDFS.",
              "Flink CEP matches event patterns within a DataStream and supports time windows.",
              "Flink 1.20 introduced async sinks with backpressure-aware buffering."),
          "agents",
          List.of(
              "ReAct agents alternate between thought, action, and observation steps.",
              "MoSCoW context management prioritizes Must / Should / Could / Won't items.",
              "Tool-use is the act of an LLM calling a structured function with arguments.",
              "Guardrails are pre/post-LLM classifiers that can block or rewrite a call."));

  public static void main(String[] args) throws Exception {
    String ollamaUrl = ConfigKeys.DEFAULT_OLLAMA_BASE_URL;

    DjlEmbeddingConnection embeddings =
        DjlEmbeddingConnection.of(
            "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2");
    DjlInferenceConnection reranker =
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/cross-encoder/ms-marco-MiniLM-L-6-v2");
    LangChain4jChatConnection chat = LangChain4jChatConnection.ollama(ollamaUrl);
    VectorMemorySpec memorySpec = FlinkStateVectorMemory.spec(384); // MiniLM dimension

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    env.fromElements(
            new Query("flink", "How does Flink guarantee exactly-once state?"),
            new Query("agents", "What is a guardrail?"),
            new Query("flink", "How are checkpoints stored?"),
            new Query("agents", "How does the ReAct loop work?"))
        .keyBy(Query::topic)
        .process(new RagProcessFunction(embeddings, reranker, chat, memorySpec))
        .print()
        .name("answers");

    env.execute("rag-research-assistant");
  }

  /** Per-key RAG operator. Holds embedder/reranker/chat clients; vector memory is keyed state. */
  static final class RagProcessFunction extends KeyedProcessFunction<String, Query, Answer> {
    private static final long serialVersionUID = 1L;

    private final DjlEmbeddingConnection embeddingsConn;
    private final DjlInferenceConnection rerankerConn;
    private final LangChain4jChatConnection chatConn;
    private final VectorMemorySpec memorySpec;

    private transient EmbeddingClient embedder;
    private transient EmbeddingSetup embedderSetup;
    private transient Scorer reranker;
    private transient InferenceSetup rerankerSetup;
    private transient ChatClient chat;
    private transient ChatSetup chatSetup;
    private transient VectorMemory vector;
    private transient ValueState<Boolean> seededState;
    private transient MetricsAgentEventListener metrics;

    RagProcessFunction(
        DjlEmbeddingConnection embeddingsConn,
        DjlInferenceConnection rerankerConn,
        LangChain4jChatConnection chatConn,
        VectorMemorySpec memorySpec) {
      this.embeddingsConn = embeddingsConn;
      this.rerankerConn = rerankerConn;
      this.chatConn = chatConn;
      this.memorySpec = memorySpec;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      embedder = embeddingsConn.bind(getRuntimeContext());
      embedderSetup = EmbeddingSetup.of(embeddingsConn.getModelUri(), 384, true);
      reranker = rerankerConn.bind(getRuntimeContext()).asScorer();
      rerankerSetup =
          InferenceSetup.builder()
              .withModelName("ms-marco-MiniLM-L-6-v2")
              .withModelUri(rerankerConn.getDefaultModelUri())
              .build();
      chat = chatConn.bind(getRuntimeContext());
      chatSetup = ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.2).build();
      vector = memorySpec.bind(getRuntimeContext());
      seededState =
          getRuntimeContext().getState(new ValueStateDescriptor<>("rag.seeded", Boolean.class));
      metrics = new MetricsAgentEventListener();
    }

    @Override
    public void processElement(Query q, Context ctx, Collector<Answer> out) throws Exception {
      seedIfNeeded(q.topic());

      // Step 1: embed and recall.
      float[] queryVec = embedder.embed(q.question(), embedderSetup);
      List<ScoredItem> recall = vector.search(queryVec, 4);

      // Step 2: rerank with cross-encoder, against the original question.
      List<ScoredItem> reranked = new ArrayList<>(recall.size());
      for (ScoredItem si : recall) {
        double crossScore =
            reranker.scorePair(si.getItem().getContent(), q.question(), rerankerSetup);
        reranked.add(new ScoredItem(si.getId(), crossScore, si.getItem()));
      }
      reranked.sort(null);

      // Step 3: assemble prompt and answer.
      StringBuilder ctxBlock = new StringBuilder();
      double topScore = reranked.isEmpty() ? 0.0 : reranked.get(0).getScore();
      int take = Math.min(3, reranked.size());
      for (int i = 0; i < take; i++) {
        ScoredItem si = reranked.get(i);
        ctxBlock.append("[")
            .append(i + 1)
            .append("] ")
            .append(si.getItem().getContent())
            .append('\n');
      }

      ChatResponse resp =
          chat.chat(
              List.of(
                  ChatMessage.system(
                      "You are a research assistant. Answer the question using ONLY the numbered"
                          + " sources below. Cite them inline as [1], [2], etc. If the sources do"
                          + " not contain the answer, say so."),
                  ChatMessage.user("Sources:\n" + ctxBlock + "\nQuestion: " + q.question())),
              chatSetup);
      metrics.onChatResponse(
          "rag", chatSetup.getModelName(), resp.getText().length(), resp.getTokensUsed());

      out.collect(new Answer(q.topic(), q.question(), resp.getText().trim(), topScore));
    }

    private void seedIfNeeded(String topic) throws Exception {
      Boolean seeded = seededState.value();
      if (Boolean.TRUE.equals(seeded)) {
        return;
      }
      List<String> docs = SEED_CORPUS.getOrDefault(topic, List.of());
      for (int i = 0; i < docs.size(); i++) {
        String doc = docs.get(i);
        ContextItem item =
            new ContextItem(doc, ContextPriority.MUST, MemoryType.LONG_TERM);
        float[] vec = embedder.embed(doc, embedderSetup);
        vector.put(topic + "-" + i, vec, item);
      }
      seededState.update(Boolean.TRUE);
    }
  }
}
