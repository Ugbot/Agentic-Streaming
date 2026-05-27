package org.agentic.flink.rag;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.embedding.EmbeddingConnection;
import org.agentic.flink.embedding.EmbeddingSetup;
import org.agentic.flink.embedding.OllamaEmbeddingConnection;
import org.agentic.flink.ingest.Chunk;
import org.agentic.flink.ingest.Chunker;
import org.agentic.flink.ingest.RecursiveTextChunker;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.storage.VectorStore;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import org.agentic.flink.web.WebFetchTool;
import org.agentic.flink.web.WebToolkitOptions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level, in-JVM Retrieval-Augmented-Generation helper — the "drive it from a notebook or a
 * single agent" front door over the framework's SPIs. It is NOT a Flink job: ingestion and
 * retrieval run synchronously in the calling thread so it is trivial to use interactively. For
 * distributed ingestion at scale use {@code IngestionPipeline} / {@code RetrievalPipeline}.
 *
 * <p>Wires together: {@link WebFetchTool} (scrape) → {@link Chunker} (split) →
 * {@link EmbeddingConnection} (embed; Ollama by default) → {@link VectorStore} (index; in-memory by
 * default) → {@link ChatConnection} (answer; Claude when an Anthropic key is supplied).
 *
 * <p>Defaults: Ollama {@code nomic-embed-text} at {@code localhost:11434} (768-dim),
 * {@link InMemoryVectorStore}, recursive 800-char chunks with 100-char overlap, Claude
 * {@code claude-sonnet-4-6} for answers. Override any piece via {@link Builder}.
 */
public final class KnowledgeBase {

  private static final Logger LOG = LoggerFactory.getLogger(KnowledgeBase.class);

  private final WebFetchTool fetchTool;
  private final Chunker chunker;
  private final EmbeddingClient embedder;
  private final EmbeddingSetup embedSetup;
  private final VectorStore store;
  private final ChatClient chat; // nullable until an answer is requested
  private final ChatSetup chatSetup;
  private final AtomicLong counter = new AtomicLong();

  private KnowledgeBase(Builder b) {
    try {
      this.fetchTool = b.fetchTool;
      this.chunker = b.chunker;
      this.embedder = b.embeddingConnection.bind(null);
      this.embedSetup = EmbeddingSetup.of(b.embedModel, b.embedDimension);
      this.store = b.store;
      this.chat = b.chatConnection == null ? null : b.chatConnection.bind(null);
      this.chatSetup = b.chatSetup;
    } catch (Exception e) {
      throw new RuntimeException("Failed to build KnowledgeBase: " + e.getMessage(), e);
    }
  }

  // ---------------- ingestion ----------------

  /** Chunk, embed, and index a raw text document. Returns the number of chunks stored. */
  public int ingestText(String sourceId, String text, Map<String, Object> extraMetadata) {
    if (text == null || text.isBlank()) return 0;
    List<Chunk> chunks = chunker.chunk(sourceId, text);
    try {
      for (Chunk c : chunks) {
        float[] vec = embedder.embed(c.getText(), embedSetup);
        Map<String, Object> meta = new HashMap<>();
        if (extraMetadata != null) meta.putAll(extraMetadata);
        meta.put("text", c.getText());
        meta.put("sourceId", sourceId);
        meta.put("position", c.getPosition());
        store.storeEmbedding(c.getId(), vec, meta);
      }
    } catch (Exception e) {
      throw new RuntimeException("Ingest failed for source " + sourceId + ": " + e.getMessage(), e);
    }
    LOG.info("Ingested {} chunks from source {}", chunks.size(), sourceId);
    return chunks.size();
  }

  /** Scrape a URL (Jsoup/Tika via {@link WebFetchTool}), then ingest its extracted text. */
  @SuppressWarnings("unchecked")
  public IngestResult ingestUrl(String url) {
    Map<String, Object> args = new HashMap<>();
    args.put("url", url);
    Map<String, Object> fetched;
    try {
      fetched = (Map<String, Object>) fetchTool.execute(args).get();
    } catch (Exception e) {
      throw new RuntimeException("Fetch failed for " + url + ": " + e.getMessage(), e);
    }
    boolean ok = Boolean.TRUE.equals(fetched.get("ok"));
    if (!ok) {
      String reason = String.valueOf(fetched.getOrDefault("error", fetched.get("status")));
      LOG.warn("Skipping {} — fetch not ok: {}", url, reason);
      return new IngestResult(url, String.valueOf(fetched.getOrDefault("title", "")), 0, false, reason);
    }
    String title = String.valueOf(fetched.getOrDefault("title", ""));
    String text = String.valueOf(fetched.getOrDefault("text", ""));
    Map<String, Object> meta = new HashMap<>();
    meta.put("url", url);
    meta.put("title", title);
    int n = ingestText(url, text, meta);
    return new IngestResult(url, title, n, true, null);
  }

  /** Convenience: ingest several URLs, collecting per-URL results. */
  public List<IngestResult> ingestUrls(List<String> urls) {
    List<IngestResult> out = new ArrayList<>();
    for (String u : urls) {
      try {
        out.add(ingestUrl(u));
      } catch (RuntimeException e) {
        out.add(new IngestResult(u, "", 0, false, e.getMessage()));
      }
    }
    return out;
  }

  // ---------------- retrieval ----------------

  /** Embed the query and return the top-k most similar indexed passages. */
  public List<Passage> search(String query, int k) {
    try {
      float[] q = embedder.embed(query, embedSetup);
      List<VectorStore.VectorSearchResult> hits = store.searchSimilar(q, k);
      List<Passage> passages = new ArrayList<>(hits.size());
      for (VectorStore.VectorSearchResult h : hits) {
        Map<String, Object> m = h.getMetadata();
        passages.add(
            new Passage(
                h.getId(),
                m == null ? "" : String.valueOf(m.getOrDefault("text", "")),
                m == null ? null : (String) m.get("url"),
                m == null ? null : (String) m.get("title"),
                h.getScore()));
      }
      return passages;
    } catch (Exception e) {
      throw new RuntimeException("Search failed: " + e.getMessage(), e);
    }
  }

  /**
   * Retrieve the top-k passages and ask the chat model to answer the question grounded in them.
   * Requires a {@link ChatConnection} to have been configured.
   */
  public Answer ask(String question, int k) {
    if (chat == null) {
      throw new IllegalStateException(
          "KnowledgeBase has no ChatConnection; configure one (e.g. withClaude(apiKey)) to ask");
    }
    List<Passage> passages = search(question, k);
    StringBuilder context = new StringBuilder();
    for (int i = 0; i < passages.size(); i++) {
      Passage p = passages.get(i);
      context
          .append("[")
          .append(i + 1)
          .append("] ")
          .append(p.title == null || p.title.isBlank() ? p.id : p.title)
          .append(p.url == null ? "" : " (" + p.url + ")")
          .append("\n")
          .append(p.text)
          .append("\n\n");
    }
    String system =
        "You answer questions using ONLY the provided sources. Cite sources by their bracket "
            + "number, e.g. [1]. If the sources do not contain the answer, say so plainly.";
    String user =
        "Sources:\n" + context + "\nQuestion: " + question + "\n\nAnswer with citations:";
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(ChatMessage.system(system));
    messages.add(ChatMessage.user(user));
    ChatResponse resp = chat.chat(messages, chatSetup);
    return new Answer(question, resp.getText(), passages);
  }

  /** Number of vectors currently indexed. */
  public Map<String, Object> stats() {
    try {
      return store.getStatistics();
    } catch (Exception e) {
      return Map.of("error", e.getMessage());
    }
  }

  public VectorStore vectorStore() {
    return store;
  }

  // ---------------- builder ----------------

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private WebFetchTool fetchTool =
        new WebFetchTool(WebToolkitOptions.defaults().withUserAgent("agentic-flink-kb/1.0"));
    private Chunker chunker = new RecursiveTextChunker(800, 100);
    private EmbeddingConnection embeddingConnection =
        new OllamaEmbeddingConnection(ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    private String embedModel = ConfigKeys.DEFAULT_OLLAMA_EMBED_MODEL;
    private int embedDimension = ConfigKeys.DEFAULT_OLLAMA_EMBED_DIMENSION;
    private VectorStore store = newInMemoryStore();
    private ChatConnection chatConnection;
    private ChatSetup chatSetup =
        ChatSetup.builder()
            .withModel(ConfigKeys.DEFAULT_ANTHROPIC_MODEL)
            .withTemperature(0.2)
            .withMaxResponseTokens(1024)
            .build();

    private static VectorStore newInMemoryStore() {
      InMemoryVectorStore s = new InMemoryVectorStore();
      s.initialize(Map.of("vector.similarity", "cosine"));
      return s;
    }

    public Builder withFetchTool(WebFetchTool fetchTool) {
      this.fetchTool = fetchTool;
      return this;
    }

    public Builder withChunker(Chunker chunker) {
      this.chunker = chunker;
      return this;
    }

    public Builder withEmbedding(EmbeddingConnection conn, String model, int dimension) {
      this.embeddingConnection = conn;
      this.embedModel = model;
      this.embedDimension = dimension;
      return this;
    }

    public Builder withOllamaEmbedding(String baseUrl, String model, int dimension) {
      return withEmbedding(new OllamaEmbeddingConnection(baseUrl), model, dimension);
    }

    public Builder withVectorStore(VectorStore store) {
      this.store = store;
      return this;
    }

    public Builder withChatConnection(ChatConnection conn, ChatSetup setup) {
      this.chatConnection = conn;
      if (setup != null) this.chatSetup = setup;
      return this;
    }

    /** Convenience: answer with Claude using the given Anthropic API key + default model. */
    public Builder withClaude(String apiKey) {
      this.chatConnection =
          org.agentic.flink.llm.langchain4j.LangChain4jChatConnection.anthropic(apiKey);
      return this;
    }

    public Builder withClaude(String apiKey, String model) {
      this.chatConnection =
          org.agentic.flink.llm.langchain4j.LangChain4jChatConnection.anthropic(apiKey);
      this.chatSetup = chatSetup.toBuilder().withModel(model).build();
      return this;
    }

    public KnowledgeBase build() {
      return new KnowledgeBase(this);
    }
  }

  // ---------------- result records ----------------

  /** Outcome of ingesting one URL. */
  public static final class IngestResult implements Serializable {
    public final String url;
    public final String title;
    public final int chunks;
    public final boolean ok;
    public final String error;

    public IngestResult(String url, String title, int chunks, boolean ok, String error) {
      this.url = url;
      this.title = title;
      this.chunks = chunks;
      this.ok = ok;
      this.error = error;
    }

    @Override
    public String toString() {
      return ok
          ? String.format("IngestResult[%s '%s' chunks=%d]", url, title, chunks)
          : String.format("IngestResult[%s FAILED: %s]", url, error);
    }
  }

  /** A retrieved passage. */
  public static final class Passage implements Serializable {
    public final String id;
    public final String text;
    public final String url;
    public final String title;
    public final float score;

    public Passage(String id, String text, String url, String title, float score) {
      this.id = id;
      this.text = text;
      this.url = url;
      this.title = title;
      this.score = score;
    }
  }

  /** An answer plus the passages it was grounded in. */
  public static final class Answer implements Serializable {
    public final String question;
    public final String text;
    public final List<Passage> sources;

    public Answer(String question, String text, List<Passage> sources) {
      this.question = question;
      this.text = text;
      this.sources = sources;
    }

    @Override
    public String toString() {
      return text;
    }
  }
}
