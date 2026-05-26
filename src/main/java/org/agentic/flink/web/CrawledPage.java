package org.agentic.flink.web;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of a successful fetch + extract. Carries enough to chunk and index downstream.
 *
 * <p>Held as a Serializable POJO so it rides through the Flink job graph.
 */
public final class CrawledPage implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String url;
  private final String finalUrl;
  private final String contentType;
  private final String title;
  private final String text;
  private final List<String> discoveredLinks;
  private final long fetchedAt;
  private final int depth;
  private final Map<String, String> metadata;

  public CrawledPage(
      String url,
      String finalUrl,
      String contentType,
      String title,
      String text,
      List<String> discoveredLinks,
      long fetchedAt,
      int depth,
      Map<String, String> metadata) {
    this.url = Objects.requireNonNull(url, "url");
    this.finalUrl = finalUrl == null ? url : finalUrl;
    this.contentType = contentType == null ? "text/plain" : contentType;
    this.title = title == null ? "" : title;
    this.text = text == null ? "" : text;
    this.discoveredLinks =
        discoveredLinks == null ? Collections.emptyList() : List.copyOf(discoveredLinks);
    this.fetchedAt = fetchedAt;
    this.depth = depth;
    this.metadata = metadata == null ? Collections.emptyMap() : Map.copyOf(metadata);
  }

  public String getUrl() {
    return url;
  }

  public String getFinalUrl() {
    return finalUrl;
  }

  public String getContentType() {
    return contentType;
  }

  public String getTitle() {
    return title;
  }

  public String getText() {
    return text;
  }

  public List<String> getDiscoveredLinks() {
    return discoveredLinks;
  }

  public long getFetchedAt() {
    return fetchedAt;
  }

  public int getDepth() {
    return depth;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "CrawledPage[" + url + ", " + text.length() + " chars, "
        + discoveredLinks.size() + " links, depth=" + depth + "]";
  }
}
