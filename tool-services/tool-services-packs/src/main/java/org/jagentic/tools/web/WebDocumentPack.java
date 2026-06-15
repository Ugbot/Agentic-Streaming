package org.jagentic.tools.web;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.ToolRegistry;
import org.jagentic.tools.OneShotTool;
import org.jagentic.tools.ToolPack;

/** Web + document pack — robots-aware fetch, link extraction, bounded crawl, and Tika
 * document extraction (PDF/DOCX/HTML/… → text). Built on the lifted, Flink-free
 * Fetcher/DocumentExtractor/WebFetchTool/ExtractLinksTool. Tools are id-prefixed {@code web_}. */
public final class WebDocumentPack implements ToolPack {

  private final WebToolkitOptions options;

  public WebDocumentPack() {
    this(WebToolkitOptions.defaults());
  }

  public WebDocumentPack(WebToolkitOptions options) {
    this.options = options;
  }

  @Override
  public String name() {
    return "web";
  }

  @Override
  public List<String> register(ToolRegistry registry) {
    Fetcher fetcher = new Fetcher(options);
    DocumentExtractor extractor = new DocumentExtractor();

    OneShotTool fetch = new WebFetchTool(fetcher, extractor);
    OneShotTool links = new ExtractLinksTool(fetcher, extractor);
    OneShotTool crawl = new WebCrawlTool(fetcher, extractor);

    registry.register("web_fetch",
        "Fetch a URL (robots-aware) and return its title, text, content type and links.",
        urlSchema(), p -> fetch.execute(p).join());

    registry.register("web_links",
        "Fetch a URL and return only its discovered outbound links.",
        urlSchema(), p -> links.execute(p).join());

    registry.register("web_crawl",
        "Bounded breadth-first crawl from a seed URL; returns fetched pages (url, title, text).",
        crawlSchema(), p -> crawl.execute(p).join());

    registry.register("doc_extract",
        "Extract clean text (+ title) from a document via Apache Tika — a URL, base64 bytes, "
            + "or inline text. Handles PDF/DOCX/PPTX/HTML/RTF/plain text.",
        docExtractSchema(), p -> docExtract(fetcher, extractor, p));

    return List.of("web_fetch", "web_links", "web_crawl", "doc_extract");
  }

  private Object docExtract(Fetcher fetcher, DocumentExtractor extractor, Map<String, Object> p) {
    String url = str(p, "url");
    String contentType = str(p, "content_type");
    DocumentExtractor.ExtractedDocument doc;
    try {
      if (url != null && !url.isEmpty()) {
        Fetcher.FetchResult fetched = fetcher.fetch(url);
        if (!fetched.isOk()) {
          return Map.of("ok", false, "status", fetched.getStatus(), "disallowed", fetched.isDisallowed());
        }
        doc = extractor.extract(fetched.getFinalUrl(), fetched.getBody(),
            contentType != null ? contentType : fetched.getContentType());
      } else if (str(p, "content_base64") != null) {
        byte[] bytes = Base64.getDecoder().decode(str(p, "content_base64"));
        doc = extractor.extract("urn:bytes", bytes, contentType);
      } else if (str(p, "text") != null) {
        byte[] bytes = str(p, "text").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        doc = extractor.extract("urn:text", bytes, contentType != null ? contentType : "text/plain");
      } else {
        throw new IllegalArgumentException("doc_extract requires one of: url, content_base64, text");
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      return Map.of("ok", false, "error", String.valueOf(e.getMessage()));
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("ok", true);
    result.put("title", doc.getTitle());
    result.put("text", doc.getText());
    result.put("links", doc.getLinks());
    result.put("metadata", doc.getMetadata());
    return result;
  }

  private static String str(Map<String, Object> m, String k) {
    Object v = m == null ? null : m.get(k);
    return v == null ? null : String.valueOf(v);
  }

  private static Map<String, Object> urlSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("url", Map.of("type", "string", "description", "Absolute http(s) URL."));
    return objectSchema(props, List.of("url"));
  }

  private static Map<String, Object> crawlSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("url", Map.of("type", "string", "description", "Seed http(s) URL."));
    props.put("max_pages", Map.of("type", "integer", "description", "Max pages to fetch (1-100, default 10)."));
    props.put("max_depth", Map.of("type", "integer", "description", "Max link depth (0-5, default 2)."));
    props.put("same_host", Map.of("type", "boolean", "description", "Stay on the seed host (default true)."));
    return objectSchema(props, List.of("url"));
  }

  private static Map<String, Object> docExtractSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("url", Map.of("type", "string", "description", "URL to fetch then extract."));
    props.put("content_base64", Map.of("type", "string", "description", "Base64-encoded document bytes."));
    props.put("text", Map.of("type", "string", "description", "Inline document text."));
    props.put("content_type", Map.of("type", "string", "description", "MIME type hint (e.g. application/pdf)."));
    return objectSchema(props, List.of());
  }

  private static Map<String, Object> objectSchema(Map<String, Object> properties, List<String> required) {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    return schema;
  }
}
