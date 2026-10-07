package org.agentic.flink.retrieve;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.corpus.Corpus;
import org.agentic.flink.corpus.CorpusSpec;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Scorer;
import org.agentic.flink.ingest.IngestAck;
import org.agentic.flink.ingest.IngestionPipeline.EmbeddedChunk;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.memory.vector.ScoredItem;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
 * <p>The {@code rerank} stage is optional — calling {@code .answer} directly after {@code .search}
 * works fine; the top-k from the embedder is used as-is.
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
          upstream
              .process(new EmbedQueryFn(conn, defaultSetup))
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
          upstream
              .process(new SearchFn(corpusSpec, k))
              .returns(QueryWithHits.class)
              .name("retrieve-search[" + corpusSpec.name() + "]");
      return new StageRerank(hits);
    }

    /**
     * Search a corpus that lives in Flink keyed state ({@code SingleOperatorCorpus} over {@code
     * FlinkStateVectorMemory} or {@code FlinkStateHnswVectorMemory}). Both the embedded ingest
     * stream and the embedded queries are keyed by the corpus name and processed by one keyed
     * operator, so every write is visible to the next query on that operator and the vectors
     * checkpoint with the job. The ingest acknowledgements are available through {@link
     * StageRerank#ingestAcks()}.
     */
    public StageRerank search(CorpusSpec corpusSpec, int k, DataStream<EmbeddedChunk> ingest) {
      Objects.requireNonNull(corpusSpec, "corpusSpec");
      Objects.requireNonNull(ingest, "ingest");
      String name = corpusSpec.name();
      KeySelector<EmbeddedChunk, String> chunkKey = c -> name;
      KeySelector<EmbeddedQuery, String> queryKey = q -> name;
      SingleOutputStreamOperator<QueryWithHits> hits =
          ingest
              .keyBy(chunkKey, TypeInformation.of(String.class))
              .connect(upstream.keyBy(queryKey, TypeInformation.of(String.class)))
              .process(new KeyedSearchFn(corpusSpec, k))
              .returns(QueryWithHits.class)
              .name("retrieve-keyed-corpus[" + name + "]");
      return new StageRerank(hits, hits.getSideOutput(INGEST_ACKS));
    }

    /**
     * Live two-tier search: merge the {@link HotVectorIndex hot} tier (recent, just-ingested docs)
     * with the durable cold {@code corpusSpec}, de-duplicated by id, top-{@code k}. A document is
     * retrievable the instant it lands in the hot window, before the cold index catches up.
     */
    public StageRerank searchHotCold(CorpusSpec corpusSpec, HotVectorIndex hot, int k) {
      Objects.requireNonNull(corpusSpec, "corpusSpec");
      Objects.requireNonNull(hot, "hot");
      DataStream<QueryWithHits> hits =
          upstream
              .process(new HotColdSearchFn(corpusSpec, hot, k))
              .returns(QueryWithHits.class)
              .name("retrieve-search-hotcold[" + corpusSpec.name() + "]");
      return new StageRerank(hits);
    }
  }

  /** Rerank stage (optional). */
  public static final class StageRerank {
    private final DataStream<QueryWithHits> upstream;
    private final DataStream<IngestAck> ingestAcks;

    StageRerank(DataStream<QueryWithHits> upstream) {
      this(upstream, null);
    }

    StageRerank(DataStream<QueryWithHits> upstream, DataStream<IngestAck> ingestAcks) {
      this.upstream = upstream;
      this.ingestAcks = ingestAcks;
    }

    /**
     * Ingest acknowledgements of a keyed corpus search; see {@link StageSearch#search(CorpusSpec,
     * int, DataStream)}.
     */
    public DataStream<IngestAck> ingestAcks() {
      if (ingestAcks == null) {
        throw new IllegalStateException(
            "ingest acks are only produced by search(corpusSpec, k, ingest)");
      }
      return ingestAcks;
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
          upstream
              .process(new RerankFn(scorerConn, setup))
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

    /** The retrieved (and possibly reranked) passages, without an LLM answer stage. */
    public DataStream<QueryWithHits> hits() {
      return upstream;
    }

    public DataStream<Answer> answer(ChatConnection conn, ChatSetup setup) {
      Objects.requireNonNull(conn, "conn");
      Objects.requireNonNull(setup, "setup");
      return upstream
          .process(new AnswerFn(conn, setup))
          .returns(Answer.class)
          .name("retrieve-answer");
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

  static final OutputTag<IngestAck> INGEST_ACKS =
      new OutputTag<>("retrieve-ingest-acks", TypeInformation.of(IngestAck.class));

  static List<RetrievedPassage> toPassages(List<ScoredItem> hits) {
    List<RetrievedPassage> passages = new ArrayList<>(hits.size());
    for (ScoredItem si : hits) {
      String text = si.getItem() == null ? "" : si.getItem().getContent();
      String url =
          si.getItem() == null || si.getItem().getMetadata() == null
              ? null
              : si.getItem().getMetadata().get("source_url");
      passages.add(new RetrievedPassage(si.getId(), text, si.getScore(), url));
    }
    return passages;
  }

  /**
   * One keyed operator that both indexes embedded chunks and answers embedded queries against a
   * corpus kept in Flink keyed state. Keyed by the corpus name so a corpus is one state partition.
   */
  static final class KeyedSearchFn
      extends KeyedCoProcessFunction<String, EmbeddedChunk, EmbeddedQuery, QueryWithHits> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KeyedSearchFn.class);

    private final CorpusSpec corpusSpec;
    private final int k;
    private transient Corpus corpus;

    KeyedSearchFn(CorpusSpec corpusSpec, int k) {
      this.corpusSpec = corpusSpec;
      this.k = Math.max(1, k);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      corpus = corpusSpec.bind(getRuntimeContext());
    }

    @Override
    public void processElement1(EmbeddedChunk e, Context ctx, Collector<QueryWithHits> out) {
      try {
        ContextItem item =
            new ContextItem(e.getChunk().getText(), ContextPriority.SHOULD, MemoryType.LONG_TERM);
        item.setItemId(e.getChunk().getId());
        corpus.upsert(e.getChunk().getId(), e.getEmbedding(), item).get();
        ctx.output(
            INGEST_ACKS,
            new IngestAck(
                e.getChunk().getId(),
                e.getChunk().getSourceId(),
                corpusSpec.name(),
                System.currentTimeMillis()));
      } catch (Exception ex) {
        LOG.warn("upsert failed for chunk {}: {}", e.getChunk().getId(), ex.getMessage());
      }
    }

    @Override
    public void processElement2(EmbeddedQuery q, Context ctx, Collector<QueryWithHits> out) {
      try {
        List<ScoredItem> hits = corpus.search(q.getEmbedding(), k).get();
        out.collect(new QueryWithHits(q.getQuestion(), toPassages(hits)));
      } catch (Exception e) {
        LOG.warn("search failed for '{}': {}", q.getQuestion(), e.getMessage());
      }
    }

    @Override
    public void close() throws Exception {
      if (corpus != null) {
        corpus.close();
      }
    }
  }

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
          String url =
              si.getItem() == null
                  ? null
                  : (si.getItem().getMetadata() == null
                      ? null
                      : si.getItem().getMetadata().get("source_url"));
          passages.add(new RetrievedPassage(si.getId(), text, si.getScore(), url));
        }
        out.collect(new QueryWithHits(q.getQuestion(), passages));
      } catch (Exception e) {
        LOG.warn("search failed for '{}': {}", q.getQuestion(), e.getMessage());
      }
    }
  }

  /** Two-tier (hot + cold) search operator backing {@link StageSearch#searchHotCold}. */
  static final class HotColdSearchFn extends ProcessFunction<EmbeddedQuery, QueryWithHits> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HotColdSearchFn.class);

    private final CorpusSpec corpusSpec;
    private final HotVectorIndex hot;
    private final int k;
    private transient Corpus corpus;
    private transient TwoTierRetriever retriever;

    HotColdSearchFn(CorpusSpec corpusSpec, HotVectorIndex hot, int k) {
      this.corpusSpec = corpusSpec;
      this.hot = hot;
      this.k = Math.max(1, k);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      corpus = corpusSpec.bind(getRuntimeContext());
      TwoTierRetriever.ColdSearch cold = (q, kk) -> corpus.search(q, kk).get();
      retriever = new TwoTierRetriever(hot, cold, k, k);
    }

    @Override
    public void processElement(EmbeddedQuery q, Context ctx, Collector<QueryWithHits> out) {
      try {
        List<ScoredItem> hits = retriever.retrieve(q.getEmbedding(), k);
        List<RetrievedPassage> passages = new ArrayList<>(hits.size());
        for (ScoredItem si : hits) {
          String text = si.getItem() == null ? "" : si.getItem().getContent();
          String url =
              si.getItem() == null
                  ? null
                  : (si.getItem().getMetadata() == null
                      ? null
                      : si.getItem().getMetadata().get("source_url"));
          passages.add(new RetrievedPassage(si.getId(), text, si.getScore(), url));
        }
        out.collect(new QueryWithHits(q.getQuestion(), passages));
      } catch (Exception e) {
        LOG.warn("hot+cold search failed for '{}': {}", q.getQuestion(), e.getMessage());
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
