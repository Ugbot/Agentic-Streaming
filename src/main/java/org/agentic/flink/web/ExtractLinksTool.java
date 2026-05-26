package org.agentic.flink.web;

import org.agentic.flink.tools.ToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Cheaper companion to {@link WebFetchTool}: GETs a URL and returns only the discovered links
 * (no body text). Useful when the LLM is exploring a site's structure before deciding which
 * pages are worth a full fetch.
 */
public final class ExtractLinksTool implements ToolExecutor {
  private static final long serialVersionUID = 1L;

  private final Fetcher fetcher;
  private final DocumentExtractor extractor;

  public ExtractLinksTool(WebToolkitOptions options) {
    this(new Fetcher(options), new DocumentExtractor());
  }

  public ExtractLinksTool(Fetcher fetcher, DocumentExtractor extractor) {
    this.fetcher = fetcher;
    this.extractor = extractor;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(
        () -> {
          String url = parameters == null ? null : (String) parameters.get("url");
          if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("ExtractLinksTool requires a 'url' parameter");
          }
          Map<String, Object> out = new HashMap<>();
          out.put("url", url);
          try {
            Fetcher.FetchResult fetched = fetcher.fetch(url);
            if (!fetched.isOk()) {
              out.put("ok", false);
              out.put("status", fetched.getStatus());
              return out;
            }
            DocumentExtractor.ExtractedDocument doc =
                extractor.extract(fetched.getFinalUrl(), fetched.getBody(), fetched.getContentType());
            out.put("ok", true);
            out.put("title", doc.getTitle());
            out.put("links", doc.getLinks());
            return out;
          } catch (Exception e) {
            out.put("ok", false);
            out.put("error", e.getMessage());
            return out;
          }
        });
  }

  @Override
  public String getToolId() {
    return "extract-links";
  }

  @Override
  public String getDescription() {
    return "Fetch a URL and return only its title + discovered links (cheaper than web-fetch).";
  }
}
