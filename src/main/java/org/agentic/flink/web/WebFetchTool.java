package org.agentic.flink.web;

import org.agentic.flink.tools.ToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * {@link ToolExecutor} the agent can call to GET a URL and receive parsed text + discovered
 * links + content type. Routes through {@link Fetcher} so robots.txt + user-agent +
 * max-page-size are all honoured.
 *
 * <p>Suitable as a general-purpose "look at this URL" tool exposed to the LLM. Pair with
 * {@link org.agentic.flink.channel.ToolInvocationChannel} if you also want the
 * invocation to feed a downstream crawler operator.
 */
public final class WebFetchTool implements ToolExecutor {
  private static final long serialVersionUID = 1L;

  private final Fetcher fetcher;
  private final DocumentExtractor extractor;

  public WebFetchTool(WebToolkitOptions options) {
    this(new Fetcher(options), new DocumentExtractor());
  }

  public WebFetchTool(Fetcher fetcher, DocumentExtractor extractor) {
    this.fetcher = fetcher;
    this.extractor = extractor;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(
        () -> {
          String url = parameters == null ? null : (String) parameters.get("url");
          if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("WebFetchTool requires a 'url' parameter");
          }
          Map<String, Object> result = new HashMap<>();
          result.put("url", url);
          try {
            Fetcher.FetchResult fetched = fetcher.fetch(url);
            if (!fetched.isOk()) {
              result.put("ok", false);
              result.put("status", fetched.getStatus());
              result.put("disallowed", fetched.isDisallowed());
              return result;
            }
            DocumentExtractor.ExtractedDocument doc =
                extractor.extract(fetched.getFinalUrl(), fetched.getBody(), fetched.getContentType());
            result.put("ok", true);
            result.put("status", fetched.getStatus());
            result.put("finalUrl", fetched.getFinalUrl());
            result.put("contentType", fetched.getContentType());
            result.put("title", doc.getTitle());
            result.put("text", doc.getText());
            result.put("links", doc.getLinks());
            return result;
          } catch (Exception e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
            return result;
          }
        });
  }

  @Override
  public String getToolId() {
    return "web-fetch";
  }

  @Override
  public String getDescription() {
    return "Fetch a URL and return its parsed text, title, content type, and discovered links.";
  }
}
