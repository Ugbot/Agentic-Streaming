package org.jagentic.tools.web;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-host cache of robots.txt rules. Lazily fetches on first request, caches for 24h.
 *
 * <p>Falls open (allows the fetch) if the robots.txt request fails — matches the behaviour of
 * most well-behaved crawlers including StormCrawler.
 */
public final class RobotsCache implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RobotsCache.class);

  private static final Duration TTL = Duration.ofHours(24);

  private final String userAgent;
  private final Duration fetchTimeout;
  private final transient ConcurrentMap<String, Entry> cache = new ConcurrentHashMap<>();
  private transient HttpClient http;
  private transient SimpleRobotRulesParser parser;

  public RobotsCache(String userAgent, Duration fetchTimeout) {
    this.userAgent = userAgent;
    this.fetchTimeout = fetchTimeout == null ? Duration.ofSeconds(10) : fetchTimeout;
  }

  /** Returns true if the user-agent is allowed to fetch {@code url} per its host's robots.txt. */
  public boolean isAllowed(String url) {
    try {
      URL u = URI.create(url).toURL();
      String host = u.getProtocol() + "://" + u.getHost() + (u.getPort() == -1 ? "" : ":" + u.getPort());
      Entry e = cache.get(host);
      Instant now = Instant.now();
      if (e == null || e.fetchedAt.plus(TTL).isBefore(now)) {
        e = fetch(host);
        cache.put(host, e);
      }
      return e.rules.isAllowed(url);
    } catch (Exception ex) {
      LOG.debug("RobotsCache fall-open for {}: {}", url, ex.getMessage());
      return true;
    }
  }

  private Entry fetch(String hostBase) {
    BaseRobotRules rules;
    try {
      if (http == null) {
        http = HttpClient.newBuilder().connectTimeout(fetchTimeout).build();
      }
      if (parser == null) {
        parser = new SimpleRobotRulesParser();
      }
      String robotsUrl = hostBase + "/robots.txt";
      HttpRequest req =
          HttpRequest.newBuilder()
              .uri(URI.create(robotsUrl))
              .timeout(fetchTimeout)
              .header("User-Agent", userAgent)
              .GET()
              .build();
      HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
      if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
        rules = parser.parseContent(robotsUrl, resp.body(), "text/plain", java.util.List.of(userAgent));
      } else {
        // 4xx → fall-open per RFC 9309. 5xx → treat as deny-all (safer) until next refresh.
        rules =
            (resp.statusCode() >= 500)
                ? parser.failedFetch(503)
                : parser.failedFetch(404);
      }
    } catch (Exception e) {
      LOG.debug("robots.txt fetch failed for {}; falling open: {}", hostBase, e.getMessage());
      rules = parser == null ? new SimpleRobotRulesParser().failedFetch(404) : parser.failedFetch(404);
    }
    return new Entry(rules, Instant.now());
  }

  private static final class Entry {
    final BaseRobotRules rules;
    final Instant fetchedAt;

    Entry(BaseRobotRules rules, Instant fetchedAt) {
      this.rules = rules;
      this.fetchedAt = fetchedAt;
    }
  }
}
