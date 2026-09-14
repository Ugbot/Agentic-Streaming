package org.agentic.flink.web;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.Optional;
import org.agentic.flink.net.OutboundUrlPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP fetcher that honours {@link RobotsCache} and the framework's
 * {@link WebToolkitOptions} (user-agent, timeouts, max-page-size, egress policy).
 *
 * <p>Every URL, including each redirect target, is checked against the options'
 * {@link OutboundUrlPolicy} before a connection is opened; redirects are followed manually so
 * the check runs per hop and the hop count is capped by the policy.
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
    OutboundUrlPolicy policy = options.getUrlPolicy();
    URI target = policy.validate(url);
    if (options.isRespectRobots() && !robots.isAllowed(url)) {
      LOG.info("robots.txt disallows {}", url);
      return FetchResult.disallowed(url);
    }
    try {
      if (http == null) {
        http =
            HttpClient.newBuilder()
                .connectTimeout(options.getFetchTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
      }
      HttpResponse<byte[]> resp = null;
      int hops = 0;
      while (true) {
        HttpRequest req =
            HttpRequest.newBuilder()
                .uri(target)
                .timeout(options.getFetchTimeout())
                .header("User-Agent", options.getUserAgent())
                .GET()
                .build();
        resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        Optional<String> location = redirectLocation(resp);
        if (location.isEmpty() || !options.isFollowRedirects()) {
          break;
        }
        if (++hops > policy.getMaxRedirects()) {
          throw new IOException("too many redirects (>" + policy.getMaxRedirects() + ") from " + url);
        }
        URI next = target.resolve(location.get().trim());
        target = policy.validate(next);
        if (options.isRespectRobots() && !robots.isAllowed(target.toString())) {
          LOG.info("robots.txt disallows redirect target {}", target);
          return FetchResult.disallowed(target.toString());
        }
      }
      byte[] body = resp.body();
      if (body.length > options.getMaxPageBytes()) {
        byte[] truncated = new byte[options.getMaxPageBytes()];
        System.arraycopy(body, 0, truncated, 0, options.getMaxPageBytes());
        body = truncated;
      }
      String ct = resp.headers().firstValue("content-type").orElse("text/plain");
      String finalUrl = target.toString();
      return new FetchResult(url, finalUrl, resp.statusCode(), ct, body);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException(ie);
    }
  }

  private static Optional<String> redirectLocation(HttpResponse<?> resp) {
    int s = resp.statusCode();
    if (s == 301 || s == 302 || s == 303 || s == 307 || s == 308) {
      return resp.headers().firstValue("location");
    }
    return Optional.empty();
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
