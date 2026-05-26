package org.agentic.flink.web;

import org.agentic.flink.channel.Channel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Multi-source crawler: consumes {@link UrlRequest}s from any number of {@link Channel}s (seed
 * lists, Kafka, Redis pub/sub, LLM tool invocations, sitemap discovery, …) and emits {@link
 * CrawledPage}s. <b>Doesn't own its inputs</b> — the caller wires whichever channels are
 * relevant. That's what makes the crawler targetable by both AI and external means.
 *
 * <p>The fetch + extract loop runs inside a {@link ProcessFunction}; each subtask holds a
 * {@link Fetcher} and a {@link DocumentExtractor} bound from the serializable
 * {@link WebToolkitOptions} spec.
 */
public final class CrawlerCore {

  private CrawlerCore() {}

  public static Builder builder() {
    return new Builder();
  }

  /** Builder DSL. */
  public static final class Builder {
    private final List<Channel<UrlRequest>> frontier = new ArrayList<>();
    private WebToolkitOptions options = WebToolkitOptions.defaults();

    @SafeVarargs
    public final Builder frontier(Channel<UrlRequest>... channels) {
      if (channels != null) {
        for (Channel<UrlRequest> c : channels) frontier.add(c);
      }
      return this;
    }

    public Builder options(WebToolkitOptions options) {
      this.options = options == null ? WebToolkitOptions.defaults() : options;
      return this;
    }

    /** Wire all frontier channels through a single fetch + extract operator. */
    public DataStream<CrawledPage> open(StreamExecutionEnvironment env) throws Exception {
      if (frontier.isEmpty()) {
        throw new IllegalStateException("CrawlerCore requires at least one frontier channel");
      }
      DataStream<UrlRequest> merged = null;
      for (Channel<UrlRequest> c : frontier) {
        DataStream<UrlRequest> s = c.open(env);
        merged = merged == null ? s : merged.union(s);
      }
      return merged
          .process(new FetchAndExtractFn(options))
          .name("crawler-fetch-extract")
          .returns(TypeInformation.of(new TypeHint<CrawledPage>() {}));
    }
  }

  /** Per-subtask fetch + extract operator. Holds the Fetcher + DocumentExtractor. */
  static final class FetchAndExtractFn extends ProcessFunction<UrlRequest, CrawledPage> {
    private static final long serialVersionUID = 1L;

    private final WebToolkitOptions options;
    private transient Fetcher fetcher;
    private transient DocumentExtractor extractor;
    private transient Set<String> dedup;

    FetchAndExtractFn(WebToolkitOptions options) {
      this.options = options;
    }

    @Override
    public void open(OpenContext openContext) {
      fetcher = new Fetcher(options);
      extractor = new DocumentExtractor();
      dedup = new HashSet<>();
    }

    @Override
    public void processElement(UrlRequest req, Context ctx, Collector<CrawledPage> out) {
      if (req == null || req.getUrl() == null) return;
      if (req.getDepth() > options.getMaxDepth()) return;
      if (!dedup.add(req.getUrl())) return;
      try {
        Fetcher.FetchResult fetched = fetcher.fetch(req.getUrl());
        if (!fetched.isOk()) return;
        DocumentExtractor.ExtractedDocument doc =
            extractor.extract(fetched.getFinalUrl(), fetched.getBody(), fetched.getContentType());
        out.collect(
            new CrawledPage(
                req.getUrl(),
                fetched.getFinalUrl(),
                fetched.getContentType(),
                doc.getTitle(),
                doc.getText(),
                doc.getLinks(),
                System.currentTimeMillis(),
                req.getDepth(),
                doc.getMetadata()));
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }
}
