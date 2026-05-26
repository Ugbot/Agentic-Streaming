package org.agentic.flink.web;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP fetcher that honours {@link RobotsCache} and the framework's
 * {@link WebToolkitOptions} (user-agent, timeouts, max-page-size).
 */
public final class Fetcher implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

  private final WebToolkitOptions options;
  private final RobotsCache robots;
  private transient HttpClient http;

  public Fetcher(WebToolkitOptions options) {
    this(options, new RobotsCache(options.getUserAgent(), options.getFetchTimeout()));
  }

  public Fetcher(WebToolkitOptions options, RobotsCache robots) {
    this.options = Objects.requireNonNull(options, "options");
    this.robots = Objects.requireNonNull(robots, "robots");
  }

  public FetchResult fetch(String url) throws IOException {
    if (options.isRespectRobots() && !robots.isAllowed(url)) {
      LOG.info("robots.txt disallows {}", url);
      return FetchResult.disallowed(url);
    }
    try {
      if (http == null) {
        http =
            HttpClient.newBuilder()
                .connectTimeout(options.getFetchTimeout())
                .followRedirects(
                    options.isFollowRedirects()
                        ? HttpClient.Redirect.NORMAL
                        : HttpClient.Redirect.NEVER)
                .build();
      }
      HttpRequest req =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(options.getFetchTimeout())
              .header("User-Agent", options.getUserAgent())
              .GET()
              .build();
      HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
      byte[] body = resp.body();
      if (body.length > options.getMaxPageBytes()) {
        byte[] truncated = new byte[options.getMaxPageBytes()];
        System.arraycopy(body, 0, truncated, 0, options.getMaxPageBytes());
        body = truncated;
      }
      String ct = resp.headers().firstValue("content-type").orElse("text/plain");
      String finalUrl = resp.uri().toString();
      return new FetchResult(url, finalUrl, resp.statusCode(), ct, body);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException(ie);
    }
  }

  /** Outcome of a fetch attempt. */
  public static final class FetchResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String url;
    private final String finalUrl;
    private final int status;
    private final String contentType;
    private final byte[] body;
    private final boolean disallowed;

    public FetchResult(String url, String finalUrl, int status, String contentType, byte[] body) {
      this.url = url;
      this.finalUrl = finalUrl;
      this.status = status;
      this.contentType = contentType;
      this.body = body == null ? new byte[0] : body;
      this.disallowed = false;
    }

    private FetchResult(String url) {
      this.url = url;
      this.finalUrl = url;
      this.status = 403;
      this.contentType = "text/plain";
      this.body = new byte[0];
      this.disallowed = true;
    }

    static FetchResult disallowed(String url) {
      return new FetchResult(url);
    }

    public String getUrl() {
      return url;
    }

    public String getFinalUrl() {
      return finalUrl;
    }

    public int getStatus() {
      return status;
    }

    public String getContentType() {
      return contentType;
    }

    public byte[] getBody() {
      return body;
    }

    public boolean isDisallowed() {
      return disallowed;
    }

    public boolean isOk() {
      return !disallowed && status >= 200 && status < 300;
    }
  }
}
