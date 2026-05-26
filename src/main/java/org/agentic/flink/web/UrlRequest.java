package org.agentic.flink.web;

import java.io.Serializable;
import java.util.Objects;

/**
 * A request to fetch a URL, addressable across the crawler's many input channels (seeds,
 * sitemap discovery, LLM-driven requests, external producers).
 */
public final class UrlRequest implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String url;
  /** Free-form origin tag — "seed" / "discovered" / "agent" / "external:kafka" — for tracing. */
  private final String source;

  private final int depth;

  public UrlRequest(String url, String source) {
    this(url, source, 0);
  }

  public UrlRequest(String url, String source, int depth) {
    this.url = Objects.requireNonNull(url, "url");
    this.source = source == null ? "unknown" : source;
    this.depth = Math.max(0, depth);
  }

  public String getUrl() {
    return url;
  }

  public String getSource() {
    return source;
  }

  public int getDepth() {
    return depth;
  }

  @Override
  public String toString() {
    return "UrlRequest[" + url + " (" + source + ", depth=" + depth + ")]";
  }
}
