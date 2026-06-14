package org.jagentic.core.web;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jagentic.core.ToolRegistry;

/** Web toolkit (Tier 4) — a robots-aware fetch + text/link extraction tool built on
 * {@code java.net.http} and a self-contained HTML extractor (no Jsoup dependency, keeping
 * the core lean). Ships as an opt-in tool, default off: call
 * {@link #register(ToolRegistry)} to expose {@code web_fetch} / {@code web_links} to an
 * agent. Mirrors the Flink {@code web/} toolkit (Jsoup + crawler-commons robots) at a
 * portable subset. */
public final class WebFetcher {

  /** Result of a web fetch. */
  public record FetchResult(String url, int status, String title, String text, List<String> links) {}

  private static final String DEFAULT_UA = "jagentic-webfetch/0.1 (+https://github.com/jagentic)";
  private static final Pattern SCRIPT_STYLE =
      Pattern.compile("<(script|style|noscript|template)\\b[^>]*>.*?</\\1>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern TITLE =
      Pattern.compile("<title[^>]*>(.*?)</title>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern HREF =
      Pattern.compile("<a\\b[^>]*\\bhref\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE);
  private static final Pattern TAG = Pattern.compile("<[^>]+>");
  private static final Pattern WS = Pattern.compile("[ \\t\\x0B\\f\\r]+");
  private static final Pattern BLANK_LINES = Pattern.compile("\\n{2,}");

  private final HttpClient http = HttpClient.newBuilder()
      .followRedirects(HttpClient.Redirect.NORMAL)
      .connectTimeout(Duration.ofSeconds(5)).build();
  private final String userAgent = DEFAULT_UA;
  private boolean respectRobots = true;

  public WebFetcher respectRobots(boolean v) {
    this.respectRobots = v;
    return this;
  }

  public FetchResult fetch(String rawUrl) {
    URI uri = URI.create(rawUrl);
    String scheme = uri.getScheme();
    if (scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
      throw new IllegalArgumentException("unsupported URL: " + rawUrl);
    }
    if (respectRobots && !robotsAllows(uri)) {
      throw new SecurityException("robots.txt disallows fetching " + rawUrl);
    }
    try {
      HttpResponse<String> resp = http.send(HttpRequest.newBuilder(uri)
          .header("User-Agent", userAgent).header("Accept", "text/html,*/*")
          .timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString());
      String body = resp.body();
      String finalUrl = resp.uri().toString();
      String title = extractTitle(body);
      List<String> links = extractLinks(body, resp.uri());
      String text = extractText(body);
      return new FetchResult(finalUrl, resp.statusCode(), title, text, links);
    } catch (Exception e) {
      throw new RuntimeException("web fetch " + rawUrl + ": " + e.getMessage(), e);
    }
  }

  private static String extractTitle(String html) {
    Matcher m = TITLE.matcher(html);
    return m.find() ? collapse(stripTags(decode(m.group(1)))) : "";
  }

  private static List<String> extractLinks(String html, URI base) {
    Set<String> seen = new LinkedHashSet<>();
    Matcher m = HREF.matcher(html);
    while (m.find()) {
      try {
        URI ref = base.resolve(m.group(1).trim());
        String s = ref.getScheme();
        if (s != null && (s.equals("http") || s.equals("https"))) {
          // drop fragment
          URI noFrag = new URI(ref.getScheme(), ref.getSchemeSpecificPart(), null);
          seen.add(noFrag.toString());
        }
      } catch (Exception ignored) {
        // skip malformed hrefs
      }
    }
    return new ArrayList<>(seen);
  }

  private static String extractText(String html) {
    String noScript = SCRIPT_STYLE.matcher(html).replaceAll(" ");
    String noTitle = TITLE.matcher(noScript).replaceAll(" ");
    // turn block-ish tags into newlines so paragraphs survive
    String breaks = noTitle.replaceAll("(?i)<(/p|/div|/h[1-6]|br|/li|/tr)[^>]*>", "\n");
    String text = decode(stripTags(breaks));
    text = WS.matcher(text).replaceAll(" ");
    StringBuilder sb = new StringBuilder();
    for (String line : text.split("\n")) {
      String t = line.strip();
      if (!t.isEmpty()) {
        sb.append(t).append("\n");
      }
    }
    return BLANK_LINES.matcher(sb.toString()).replaceAll("\n").strip();
  }

  private static String stripTags(String s) {
    return TAG.matcher(s).replaceAll(" ");
  }

  private static String collapse(String s) {
    return WS.matcher(s).replaceAll(" ").strip();
  }

  private static String decode(String s) {
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        .replace("&quot;", "\"").replace("&#39;", "'").replace("&apos;", "'").replace("&nbsp;", " ");
  }

  private boolean robotsAllows(URI uri) {
    try {
      URI robots = new URI(uri.getScheme(), uri.getAuthority(), "/robots.txt", null, null);
      HttpResponse<String> resp = http.send(HttpRequest.newBuilder(robots)
          .header("User-Agent", userAgent).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() >= 400) {
        return true; // no robots.txt => allowed
      }
      return robotsCanFetch(resp.body(), uri.getPath() == null || uri.getPath().isEmpty() ? "/" : uri.getPath());
    } catch (Exception e) {
      return true; // unreachable => fail open
    }
  }

  /** Minimal robots.txt evaluator: gathers Disallow/Allow under the "*" group; Allow wins. */
  static boolean robotsCanFetch(String robots, String path) {
    List<String> disallow = new ArrayList<>();
    List<String> allow = new ArrayList<>();
    boolean applies = false;
    for (String raw : robots.split("\n")) {
      String line = raw.strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      int colon = line.indexOf(':');
      if (colon < 0) {
        continue;
      }
      String key = line.substring(0, colon).strip().toLowerCase();
      String val = line.substring(colon + 1).strip();
      switch (key) {
        case "user-agent" -> applies = val.equals("*");
        case "disallow" -> {
          if (applies && !val.isEmpty()) {
            disallow.add(val);
          }
        }
        case "allow" -> {
          if (applies && !val.isEmpty()) {
            allow.add(val);
          }
        }
        default -> {
          // ignore other directives
        }
      }
    }
    for (String a : allow) {
      if (path.startsWith(a)) {
        return true;
      }
    }
    for (String d : disallow) {
      if (path.startsWith(d)) {
        return false;
      }
    }
    return true;
  }

  /** Register {@code web_fetch} (title+text) and {@code web_links} (link list). Opt-in. */
  public List<String> register(ToolRegistry reg) {
    reg.register("web_fetch", "Fetch a web page (robots-aware) and return its title + text.", params -> {
      FetchResult r = fetch(String.valueOf(params.getOrDefault("url", ""))); // NOSONAR
      return r.title().isEmpty() ? r.text() : r.title() + "\n\n" + r.text();
    });
    reg.register("web_links", "Fetch a web page and return its outbound links, one per line.", params -> {
      FetchResult r = fetch(String.valueOf(params.getOrDefault("url", "")));
      return String.join("\n", r.links());
    });
    return List.of("web_fetch", "web_links");
  }
}
