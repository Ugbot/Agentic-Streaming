package org.agentic.flink.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.channel.sink.ForEachSink;
import org.agentic.flink.channel.source.PollingSource;
import org.agentic.flink.corpus.CorpusSpec;
import org.agentic.flink.corpus.SingleOperatorCorpus;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.ingest.IngestAck;
import org.agentic.flink.ingest.IngestionPipeline;
import org.agentic.flink.ingest.RecursiveTextChunker;
import org.agentic.flink.memory.vector.FlinkStateHnswVectorMemory;
import org.agentic.flink.memory.vector.FlinkStateVectorMemory;
import org.agentic.flink.memory.vector.VectorMemorySpec;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.agentic.flink.web.CrawledPage;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs the shipped corpus API end to end on a MiniCluster: pages are chunked and embedded by {@link
 * IngestionPipeline}, queries are embedded and searched by {@link RetrievalPipeline}, and both meet
 * on the keyed corpus operator that holds a Flink-state vector memory. Queries are only released
 * once every chunk has been acknowledged, so the top hit of each query must be the chunk of the
 * page it was built from.
 */
final class RetrievalPipelineKeyedCorpusTest {

  private static final int DIM = 64;
  private static final Map<String, LinkedBlockingQueue<String>> QUERIES = new ConcurrentHashMap<>();
  private static final Map<String, List<IngestAck>> ACKS = new ConcurrentHashMap<>();
  private static final Map<String, List<RetrievalPipeline.QueryWithHits>> HITS =
      new ConcurrentHashMap<>();

  private MiniCluster cluster;
  private JobClient job;

  @BeforeEach
  void setUp() throws Exception {
    cluster = TestClusters.start(2);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (job != null && !job.getJobStatus().get().isGloballyTerminalState()) {
      job.cancel().get();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  @ParameterizedTest(name = "[{0}] ingest then retrieve on one keyed operator")
  @ValueSource(strings = {"brute-force", "hnsw"})
  void ingestThenRetrieveThroughShippedPipelines(String flavour) throws Exception {
    String id = UUID.randomUUID().toString();
    QUERIES.put(id, new LinkedBlockingQueue<>());
    ACKS.put(id, new CopyOnWriteArrayList<>());
    HITS.put(id, new CopyOnWriteArrayList<>());

    int pageCount = 3 + ThreadLocalRandom.current().nextInt(3);
    List<CrawledPage> pages = new ArrayList<>();
    List<List<String>> vocabularies = new ArrayList<>();
    for (int i = 0; i < pageCount; i++) {
      List<String> words = new ArrayList<>();
      for (int w = 0; w < 12; w++) {
        words.add("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 10));
      }
      vocabularies.add(words);
      pages.add(
          new CrawledPage(
              "https://example.test/" + id + "/" + i,
              null,
              "text/plain",
              "page " + i,
              String.join(" ", words),
              List.of(),
              System.currentTimeMillis(),
              0,
              Map.of()));
    }

    VectorMemorySpec vectorSpec =
        flavour.equals("hnsw")
            ? FlinkStateHnswVectorMemory.spec(DIM)
            : FlinkStateVectorMemory.spec(DIM);
    CorpusSpec corpus = SingleOperatorCorpus.spec("kb-" + id, vectorSpec);
    BagOfWordsEmbedding embeddings = new BagOfWordsEmbedding();

    StreamExecutionEnvironment env =
        new TestStreamEnvironment(cluster, new Configuration(), 1, List.of(), List.of());
    env.setParallelism(1);
    DataStream<CrawledPage> pageStream = env.fromData(pages, TypeInformation.of(CrawledPage.class));
    DataStream<IngestionPipeline.EmbeddedChunk> indexed =
        IngestionPipeline.from(pageStream)
            .chunk(new RecursiveTextChunker(2048))
            .embed(embeddings, EmbeddingSetup.of("bow", DIM))
            .embedded();
    DataStream<String> queries =
        env.fromSource(
            new PollingSource<>(new QueryPollFn(id)),
            WatermarkStrategy.noWatermarks(),
            "queries",
            TypeInformation.of(String.class));
    RetrievalPipeline.StageRerank searched =
        RetrievalPipeline.from(queries)
            .embed(embeddings, EmbeddingSetup.of("bow", DIM))
            .search(corpus, 2, indexed);
    searched.ingestAcks().sinkTo(new ForEachSink<>(new AckWriteFn(id))).name("acks");
    searched.rerankSkip().hits().sinkTo(new ForEachSink<>(new HitsWriteFn(id))).name("hits");
    job = env.executeAsync("keyed-corpus-" + id);

    await(() -> ACKS.get(id).size() >= pageCount, "all chunks acknowledged; " + failureInfo());
    assertEquals(pageCount, ACKS.get(id).size());
    assertTrue(ACKS.get(id).stream().allMatch(a -> a.getCorpusName().equals("kb-" + id)));

    for (int i = 0; i < pageCount; i++) {
      List<String> words = vocabularies.get(i);
      QUERIES.get(id).add(String.join(" ", words.subList(0, 6)));
    }
    await(() -> HITS.get(id).size() >= pageCount, "every query answered; " + failureInfo());

    for (RetrievalPipeline.QueryWithHits result : HITS.get(id)) {
      String firstWord = result.getQuestion().split(" ")[0];
      assertFalse(result.getHits().isEmpty(), "no hits for " + result.getQuestion());
      RetrievedPassage top = result.getHits().get(0);
      assertTrue(
          top.getText().contains(firstWord),
          "top hit for '" + result.getQuestion() + "' was '" + top.getText() + "'");
      assertTrue(top.getScore() > 0.5, "score " + top.getScore());
    }
    assertEquals(JobStatus.RUNNING, job.getJobStatus().get(10, TimeUnit.SECONDS));
  }

  private String failureInfo() {
    try {
      ErrorInfo info = cluster.getArchivedExecutionGraph(job.getJobID()).get().getFailureInfo();
      return info == null ? "job has no failure" : info.getExceptionAsString();
    } catch (Exception e) {
      return "failure info unavailable: " + e;
    }
  }

  private static void await(java.util.function.BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(20);
    }
  }

  /** Deterministic bag-of-words embedding: similar texts share word buckets. */
  static final class BagOfWordsEmbedding implements EmbeddingConnection {
    private static final long serialVersionUID = 1L;

    @Override
    public EmbeddingClient bind(RuntimeContext runtimeContext) {
      return new Client();
    }

    @Override
    public String providerName() {
      return "bow";
    }

    static final class Client implements EmbeddingClient, Serializable {
      private static final long serialVersionUID = 1L;

      @Override
      public float[] embed(String text, EmbeddingSetup setup) {
        float[] v = new float[setup.getDimension()];
        for (String word : text.split("\\s+")) {
          if (!word.isEmpty()) {
            v[Math.floorMod(word.hashCode(), v.length)] += 1f;
          }
        }
        return v;
      }

      @Override
      public String providerName() {
        return "bow";
      }
    }
  }

  static final class QueryPollFn implements PollingSource.PollFn<String> {
    private static final long serialVersionUID = 1L;
    private final String id;

    QueryPollFn(String id) {
      this.id = id;
    }

    @Override
    public String poll(long timeoutMs) throws InterruptedException {
      return QUERIES.get(id).poll(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);
    }
  }

  static final class AckWriteFn implements ForEachSink.WriteFn<IngestAck> {
    private static final long serialVersionUID = 1L;
    private final String id;

    AckWriteFn(String id) {
      this.id = id;
    }

    @Override
    public void write(IngestAck ack) {
      ACKS.get(id).add(ack);
    }
  }

  static final class HitsWriteFn implements ForEachSink.WriteFn<RetrievalPipeline.QueryWithHits> {
    private static final long serialVersionUID = 1L;
    private final String id;

    HitsWriteFn(String id) {
      this.id = id;
    }

    @Override
    public void write(RetrievalPipeline.QueryWithHits hits) {
      HITS.get(id).add(hits);
    }
  }
}
