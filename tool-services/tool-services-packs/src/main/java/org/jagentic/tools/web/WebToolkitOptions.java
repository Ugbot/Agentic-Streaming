package org.jagentic.tools.web;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** Tunables for the framework's web toolkit. Defaults are conservative and well-behaved. */
public final class WebToolkitOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String userAgent;
  private final Duration fetchTimeout;
  private final int maxPageBytes;
  private final int maxDepth;
  private final boolean respectRobots;
  private final boolean followRedirects;

  private WebToolkitOptions(
      String userAgent,
      Duration fetchTimeout,
      int maxPageBytes,
      int maxDepth,
      boolean respectRobots,
      boolean followRedirects) {
    this.userAgent =
        userAgent == null
            ? "AgenticFlink/1.0 (+https://github.com/Ugbot/Agentic-Flink)"
            : userAgent;
    this.fetchTimeout = fetchTimeout == null ? Duration.ofSeconds(15) : fetchTimeout;
    this.maxPageBytes = maxPageBytes <= 0 ? 10 * 1024 * 1024 : maxPageBytes;
    this.maxDepth = Math.max(0, maxDepth);
    this.respectRobots = respectRobots;
    this.followRedirects = followRedirects;
  }

  public static WebToolkitOptions defaults() {
    return new WebToolkitOptions(null, null, 0, 4, true, true);
  }

  public String getUserAgent() {
    return userAgent;
  }

  public Duration getFetchTimeout() {
    return fetchTimeout;
  }

  public int getMaxPageBytes() {
    return maxPageBytes;
  }

  public int getMaxDepth() {
    return maxDepth;
  }

  public boolean isRespectRobots() {
    return respectRobots;
  }

  public boolean isFollowRedirects() {
    return followRedirects;
  }

  public WebToolkitOptions withUserAgent(String ua) {
    return new WebToolkitOptions(
        ua, fetchTimeout, maxPageBytes, maxDepth, respectRobots, followRedirects);
  }

  public WebToolkitOptions withFetchTimeout(Duration t) {
    return new WebToolkitOptions(
        userAgent, t, maxPageBytes, maxDepth, respectRobots, followRedirects);
  }

  public WebToolkitOptions withMaxPageBytes(int bytes) {
    return new WebToolkitOptions(
        userAgent, fetchTimeout, bytes, maxDepth, respectRobots, followRedirects);
  }

  public WebToolkitOptions withMaxDepth(int depth) {
    return new WebToolkitOptions(
        userAgent, fetchTimeout, maxPageBytes, depth, respectRobots, followRedirects);
  }

  public WebToolkitOptions withRespectRobots(boolean b) {
    return new WebToolkitOptions(
        userAgent, fetchTimeout, maxPageBytes, maxDepth, b, followRedirects);
  }

  public WebToolkitOptions withFollowRedirects(boolean b) {
    return new WebToolkitOptions(
        userAgent, fetchTimeout, maxPageBytes, maxDepth, respectRobots, b);
  }
}
