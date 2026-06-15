package org.jagentic.tools.web;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.jagentic.tools.OneShotTool;

/** A <b>bounded</b> breadth-first crawl: fetch the seed, extract links, follow up to
 * {@code max_pages} / {@code max_depth}, optionally staying on the seed host. A simple
 * synchronous loop over {@link Fetcher} + {@link DocumentExtractor} — the Flink
 * {@code CrawlerCore} streaming operator is the design source, not lifted (it's Flink-bound). */
public final class WebCrawlTool implements OneShotTool {

  private final Fetcher fetcher;
  private final DocumentExtractor extractor;

  public WebCrawlTool(WebToolkitOptions options) {
    this(new Fetcher(options), new DocumentExtractor());
  }

  public WebCrawlTool(Fetcher fetcher, DocumentExtractor extractor) {
    this.fetcher = fetcher;
    this.extractor = extractor;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    return CompletableFuture.supplyAsync(() -> crawl(parameters));
  }

  private Object crawl(Map<String, Object> parameters) {
    String seed = str(parameters, "url", str(parameters, "seed", null));
    if (seed == null || seed.isEmpty()) {
      throw new IllegalArgumentException("web_crawl requires a 'url' (seed) parameter");
    }
    int maxPages = clamp(intVal(parameters, "max_pages", 10), 1, 100);
    int maxDepth = clamp(intVal(parameters, "max_depth", 2), 0, 5);
    boolean sameHost = boolVal(parameters, "same_host", true);
    String seedHost = host(seed);

    Set<String> seen = new HashSet<>();
    Deque<String[]> queue = new ArrayDeque<>(); // {url, depth}
    queue.add(new String[] {seed, "0"});
    seen.add(seed);
    List<Map<String, Object>> pages = new ArrayList<>();

    while (!queue.isEmpty() && pages.size() < maxPages) {
      String[] item = queue.poll();
      String url = item[0];
      int depth = Integer.parseInt(item[1]);
      try {
        Fetcher.FetchResult fetched = fetcher.fetch(url);
        if (!fetched.isOk()) {
          continue;
        }
        DocumentExtractor.ExtractedDocument doc =
            extractor.extract(fetched.getFinalUrl(), fetched.getBody(), fetched.getContentType());
        Map<String, Object> page = new LinkedHashMap<>();
        page.put("url", fetched.getFinalUrl());
        page.put("title", doc.getTitle());
        page.put("text", snippet(doc.getText()));
        pages.add(page);

        if (depth < maxDepth) {
          for (String link : doc.getLinks()) {
            if (pages.size() + queue.size() >= maxPages * 4) {
              break; // bound the frontier too
            }
            if (seen.contains(link)) {
              continue;
            }
            if (sameHost && !sameHost(seedHost, link)) {
              continue;
            }
            seen.add(link);
            queue.add(new String[] {link, Integer.toString(depth + 1)});
          }
        }
      } catch (Exception e) {
        // skip a page that fails; keep crawling
      }
    }

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("seed", seed);
    result.put("page_count", pages.size());
    result.put("pages", pages);
    return result;
  }

  @Override
  public String getToolId() {
    return "web_crawl";
  }

  @Override
  public String getDescription() {
    return "Bounded breadth-first crawl from a seed URL; returns fetched pages (url, title, text).";
  }

  private static String snippet(String text) {
    if (text == null) {
      return "";
    }
    String t = text.strip();
    return t.length() > 2000 ? t.substring(0, 2000) : t;
  }

  private static String host(String url) {
    try {
      return URI.create(url).getHost();
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean sameHost(String seedHost, String url) {
    if (seedHost == null) {
      return true;
    }
    String h = host(url);
    return seedHost.equalsIgnoreCase(h);
  }

  private static String str(Map<String, Object> m, String k, String def) {
    Object v = m == null ? null : m.get(k);
    return v == null ? def : String.valueOf(v);
  }

  private static int intVal(Map<String, Object> m, String k, int def) {
    Object v = m == null ? null : m.get(k);
    if (v instanceof Number n) {
      return n.intValue();
    }
    if (v != null) {
      try {
        return Integer.parseInt(String.valueOf(v));
      } catch (NumberFormatException ignored) {
        return def;
      }
    }
    return def;
  }

  private static boolean boolVal(Map<String, Object> m, String k, boolean def) {
    Object v = m == null ? null : m.get(k);
    if (v instanceof Boolean b) {
      return b;
    }
    if (v != null) {
      return Boolean.parseBoolean(String.valueOf(v));
    }
    return def;
  }

  private static int clamp(int v, int lo, int hi) {
    return Math.max(lo, Math.min(hi, v));
  }
}
