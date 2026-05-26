package org.agentic.flink.retrieve;

import org.agentic.flink.corpus.Corpus;
import org.agentic.flink.corpus.CorpusSpec;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Scorer;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.memory.vector.ScoredItem;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin builder DSL for embed → search → rerank → answer pipelines.
 *
 * <p>Each {@code .stage()} call attaches a Flink operator with the right {@code
 * bind(RuntimeContext)} in its {@code open()}. The final result is a {@code DataStream<Answer>}.
 *
 * <pre>{@code
 * RetrievalPipeline.from(queryStream)
 *     .embed(djlEmbeddings)
 *     .search(corpusSpec, 6)
 *     .rerank(crossEncoderSpec)
 *     .answer(chatConn, chatSetup)
 *     .build()
 *     .print();
 * }</pre>
 *
 * <p>The {@code rerank} stage is optional — calling {@code .answer} directly after {@code
 * .search} works fine; the top-k from the embedder is used as-is.
 */
public final class RetrievalPipeline {

  private RetrievalPipeline() {}

  public static StageEmbed from(DataStream<String> queries) {
    return new StageEmbed(Objects.requireNonNull(queries, "queries"));
  }

  /** Embed stage. */
  public static final class StageEmbed {
    private final DataStream<String> upstream;

    StageEmbed(DataStream<String> upstream) {
      this.upstream = upstream;
    }

    public StageSearch embed(EmbeddingConnection conn) {
      return embed(conn, null);
    }

    public StageSearch embed(EmbeddingConnection conn, EmbeddingSetup defaultSetup) {
      Objects.requireNonNull(conn, "conn");
      DataStream<EmbeddedQuery> embedded =
          upstream.process(new EmbedQueryFn(conn, defaultSetup))
              .returns(EmbeddedQuery.class)
              .name("retrieve-embed");
      return new StageSearch(embedded);
    }
  }

  /** Search stage. */
  public static final class StageSearch {
    private final DataStream<EmbeddedQuery> upstream;

    StageSearch(DataStream<EmbeddedQuery> upstream) {
      this.upstream = upstream;
    }

    public StageRerank search(CorpusSpec corpusSpec, int k) {
      Objects.requireNonNull(corpusSpec, "corpusSpec");
      DataStream<QueryWithHits> hits =
          upstream.process(new SearchFn(corpusSpec, k))
              .returns(QueryWithHits.class)
              .name("retrieve-search[" + corpusSpec.name() + "]");
      return new StageRerank(hits);
    }
  }

  /** Rerank stage (optional). */
  public static final class StageRerank {
    private final DataStream<QueryWithHits> upstream;

    StageRerank(DataStream<QueryWithHits> upstream) {
      this.upstream = upstream;
    }

    /** Pass-through: skip reranking and go straight to answer. */
    public StageAnswer rerankSkip() {
      return new StageAnswer(upstream);
    }

    /** Rerank top-k via an inference Scorer (typically a cross-encoder). */
    public StageAnswer rerank(InferenceConnection scorerConn, InferenceSetup setup) {
      Objects.requireNonNull(scorerConn, "scorerConn");
      Objects.requireNonNull(setup, "setup");
      DataStream<QueryWithHits> reranked =
          upstream.process(new RerankFn(scorerConn, setup))
              .returns(QueryWithHits.class)
              .name("retrieve-rerank");
      return new StageAnswer(reranked);
    }
  }

  /** Answer stage. */
  public static final class StageAnswer {
    private final DataStream<QueryWithHits> upstream;

    StageAnswer(DataStream<QueryWithHits> upstream) {
      this.upstream = upstream;
    }

    public DataStream<Answer> answer(ChatConnection conn, ChatSetup setup) {
      Objects.requireNonNull(conn, "conn");
      Objects.requireNonNull(setup, "setup");
      return upstream.process(new AnswerFn(conn, setup)).returns(Answer.class).name("retrieve-answer");
    }
  }

  // ---------- internal POJOs ----------

  /** Internal: a query carrying its embedding. */
  public static final class EmbeddedQuery implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final String question;
    private final float[] embedding;

    public EmbeddedQuery(String question, float[] embedding) {
      this.question = question;
      this.embedding = embedding;
    }

    public String getQuestion() {
      return question;
    }

    public float[] getEmbedding() {
      return embedding;
    }
  }

  /** Internal: a query with retrieved passages. */
  public static final class QueryWithHits implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final String question;
    private final List<RetrievedPassage> hits;

    public QueryWithHits(String question, List<RetrievedPassage> hits) {
      this.question = question;
      this.hits = hits;
    }

    public String getQuestion() {
      return question;
    }

    public List<RetrievedPassage> getHits() {
      return hits;
    }
  }

  // ---------- per-stage operators ----------

  static final class EmbedQueryFn extends ProcessFunction<String, EmbeddedQuery> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EmbedQueryFn.class);

    private final EmbeddingConnection conn;
    private final EmbeddingSetup defaultSetup;
    private transient EmbeddingClient client;
    private transient EmbeddingSetup setup;

    EmbedQueryFn(EmbeddingConnection conn, EmbeddingSetup defaultSetup) {
      this.conn = conn;
      this.defaultSetup = defaultSetup;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      client = conn.bind(getRuntimeContext());
      setup = defaultSetup != null ? defaultSetup : EmbeddingSetup.of(conn.providerName(), 384);
    }

    @Override
    public void processElement(String q, Context ctx, Collector<EmbeddedQuery> out) {
      if (q == null || q.isEmpty()) return;
      try {
        out.collect(new EmbeddedQuery(q, client.embed(q, setup)));
      } catch (Exception e) {
        LOG.warn("embed-query failed for '{}': {}", q, e.getMessage());
      }
    }
  }

  static final class SearchFn extends ProcessFunction<EmbeddedQuery, QueryWithHits> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SearchFn.class);

    private final CorpusSpec corpusSpec;
    private final int k;
    private transient Corpus corpus;

    SearchFn(CorpusSpec corpusSpec, int k) {
      this.corpusSpec = corpusSpec;
      this.k = Math.max(1, k);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      corpus = corpusSpec.bind(getRuntimeContext());
    }

    @Override
    public void processElement(EmbeddedQuery q, Context ctx, Collector<QueryWithHits> out) {
      try {
        List<ScoredItem> hits = corpus.search(q.getEmbedding(), k).get();
        List<RetrievedPassage> passages = new ArrayList<>(hits.size());
        for (ScoredItem si : hits) {
          String text = si.getItem() == null ? "" : si.getItem().getContent();
          String url = si.getItem() == null ? null
              : (si.getItem().getMetadata() == null ? null
                  : si.getItem().getMetadata().get("source_url"));
          passages.add(new RetrievedPassage(si.getId(), text, si.getScore(), url));
        }
        out.collect(new QueryWithHits(q.getQuestion(), passages));
      } catch (Exception e) {
        LOG.warn("search failed for '{}': {}", q.getQuestion(), e.getMessage());
      }
    }
  }

  static final class RerankFn extends ProcessFunction<QueryWithHits, QueryWithHits> {
    private static final long serialVersionUID = 1L;
    private final InferenceConnection scorerConn;
    private final InferenceSetup setup;
    private transient Scorer scorer;

    RerankFn(InferenceConnection scorerConn, InferenceSetup setup) {
      this.scorerConn = scorerConn;
      this.setup = setup;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      scorer = scorerConn.bind(getRuntimeContext()).asScorer();
    }

    @Override
    public void processElement(QueryWithHits in, Context ctx, Collector<QueryWithHits> out) {
      List<RetrievedPassage> rescored = new ArrayList<>(in.getHits().size());
      for (RetrievedPassage p : in.getHits()) {
        double s = scorer.scorePair(p.getText(), in.getQuestion(), setup);
        rescored.add(new RetrievedPassage(p.getId(), p.getText(), s, p.getSourceUrl()));
      }
      rescored.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));
      out.collect(new QueryWithHits(in.getQuestion(), rescored));
    }
  }

  static final class AnswerFn extends ProcessFunction<QueryWithHits, Answer> {
    private static final long serialVersionUID = 1L;
    private final ChatConnection conn;
    private final ChatSetup setup;
    private transient ChatClient chat;

    AnswerFn(ChatConnection conn, ChatSetup setup) {
      this.conn = conn;
      this.setup = setup;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      chat = conn.bind(getRuntimeContext());
    }

    @Override
    public void processElement(QueryWithHits in, Context ctx, Collector<Answer> out) {
      StringBuilder ctxBlock = new StringBuilder();
      int take = Math.min(3, in.getHits().size());
      for (int i = 0; i < take; i++) {
        RetrievedPassage p = in.getHits().get(i);
        ctxBlock.append("[").append(i + 1).append("] ").append(p.getText()).append('\n');
      }
      ChatResponse resp =
          chat.chat(
              List.of(
                  ChatMessage.system(
                      "Answer using ONLY the numbered sources below. Cite them inline as [1], [2], …."
                          + " If the sources don't contain the answer, say so."),
                  ChatMessage.user("Sources:\n" + ctxBlock + "\nQuestion: " + in.getQuestion())),
              setup);
      out.collect(
          new Answer(
              in.getQuestion(),
              resp.getText().trim(),
              in.getHits().subList(0, take),
              System.currentTimeMillis()));
    }
  }
}
