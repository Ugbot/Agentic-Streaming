package org.agentic.flink.ingest;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.corpus.Corpus;
import org.agentic.flink.corpus.CorpusSpec;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.web.CrawledPage;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin builder DSL for chunk → embed → index pipelines.
 *
 * <p>Each {@code .stage()} call attaches a Flink operator with the right {@code
 * bind(RuntimeContext)} in its {@code open()}. The result is a {@code DataStream<IngestAck>}
 * that users can sink anywhere they like.
 *
 * <p>Example:
 *
 * <pre>{@code
 * IngestionPipeline.from(crawledPages)
 *     .chunk(new RecursiveTextChunker(512))
 *     .embed(djlEmbeddings)
 *     .into(corpusSpec)
 *     .build()
 *     .print();
 * }</pre>
 */
public final class IngestionPipeline {

  private IngestionPipeline() {}

  public static StageChunk from(DataStream<CrawledPage> pages) {
    return new StageChunk(Objects.requireNonNull(pages, "pages"));
  }

  /** Chunking stage. */
  public static final class StageChunk {
    private final DataStream<CrawledPage> upstream;

    StageChunk(DataStream<CrawledPage> upstream) {
      this.upstream = upstream;
    }

    public StageEmbed chunk(Chunker chunker) {
      Objects.requireNonNull(chunker, "chunker");
      DataStream<Chunk> chunks =
          upstream
              .flatMap(
                  (CrawledPage page, Collector<Chunk> out) -> {
                    for (Chunk c : chunker.chunk(page.getUrl(), page.getText())) {
                      out.collect(c);
                    }
                  })
              .returns(Chunk.class)
              .name("ingest-chunk");
      return new StageEmbed(chunks);
    }
  }

  /** Embedding stage. */
  public static final class StageEmbed {
    private final DataStream<Chunk> upstream;

    StageEmbed(DataStream<Chunk> upstream) {
      this.upstream = upstream;
    }

    public StageIndex embed(EmbeddingConnection conn) {
      return embed(conn, null);
    }

    public StageIndex embed(EmbeddingConnection conn, EmbeddingSetup defaultSetup) {
      Objects.requireNonNull(conn, "conn");
      DataStream<EmbeddedChunk> embedded =
          upstream
              .process(new EmbedFn(conn, defaultSetup))
              .returns(EmbeddedChunk.class)
              .name("ingest-embed");
      return new StageIndex(embedded);
    }
  }

  /** Index-into-corpus stage. */
  public static final class StageIndex {
    private final DataStream<EmbeddedChunk> upstream;

    StageIndex(DataStream<EmbeddedChunk> upstream) {
      this.upstream = upstream;
    }

    public DataStream<IngestAck> into(CorpusSpec corpus) {
      Objects.requireNonNull(corpus, "corpus");
      return upstream.process(new IndexFn(corpus)).returns(IngestAck.class).name("ingest-index");
    }
  }

  /** Internal: a chunk that has been embedded. */
  public static final class EmbeddedChunk implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final Chunk chunk;
    private final float[] embedding;

    public EmbeddedChunk(Chunk chunk, float[] embedding) {
      this.chunk = chunk;
      this.embedding = embedding;
    }

    public Chunk getChunk() {
      return chunk;
    }

    public float[] getEmbedding() {
      return embedding;
    }
  }

  /** Per-task embedder operator. */
  static final class EmbedFn extends ProcessFunction<Chunk, EmbeddedChunk> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EmbedFn.class);

    private final EmbeddingConnection conn;
    private final EmbeddingSetup defaultSetup;
    private transient EmbeddingClient client;
    private transient EmbeddingSetup setup;

    EmbedFn(EmbeddingConnection conn, EmbeddingSetup defaultSetup) {
      this.conn = conn;
      this.defaultSetup = defaultSetup;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      client = conn.bind(getRuntimeContext());
      setup =
          defaultSetup != null
              ? defaultSetup
              : EmbeddingSetup.of(conn.providerName(), 384);
    }

    @Override
    public void processElement(Chunk chunk, Context ctx, Collector<EmbeddedChunk> out) {
      try {
        float[] vec = client.embed(chunk.getText(), setup);
        out.collect(new EmbeddedChunk(chunk, vec));
      } catch (Exception e) {
        LOG.warn("embed failed for chunk {}: {}", chunk.getId(), e.getMessage());
      }
    }
  }

  /** Per-task corpus-write operator. */
  static final class IndexFn extends ProcessFunction<EmbeddedChunk, IngestAck> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IndexFn.class);

    private final CorpusSpec corpusSpec;
    private transient Corpus corpus;

    IndexFn(CorpusSpec corpusSpec) {
      this.corpusSpec = corpusSpec;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      corpus = corpusSpec.bind(getRuntimeContext());
    }

    @Override
    public void processElement(EmbeddedChunk e, Context ctx, Collector<IngestAck> out) {
      try {
        ContextItem item =
            new ContextItem(e.getChunk().getText(), ContextPriority.SHOULD, MemoryType.LONG_TERM);
        item.setItemId(e.getChunk().getId());
        corpus.upsert(e.getChunk().getId(), e.getEmbedding(), item).get();
        out.collect(
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
    public void close() throws Exception {
      if (corpus != null) corpus.close();
    }

    /** Cast-helper for the framework reference: pull the list shape that flatMap expects. */
    @SuppressWarnings("unused")
    private static <T> List<T> asList(Iterable<T> it) {
      List<T> out = new java.util.ArrayList<>();
      for (T t : it) out.add(t);
      return out;
    }
  }
}
