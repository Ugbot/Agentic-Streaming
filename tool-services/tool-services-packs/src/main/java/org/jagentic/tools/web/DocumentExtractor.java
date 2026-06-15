package org.jagentic.tools.web;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Pulls clean text + links out of arbitrary fetched content via Apache Tika.
 *
 * <p>HTML payloads get a Jsoup-driven extraction (better for link discovery + title); everything
 * else (PDF / DOC / PPT / EPUB / RTF / plain text / …) is routed through Tika's auto-detect
 * parser. Either way the result is the same {@link ExtractedDocument} shape.
 */
public final class DocumentExtractor implements Serializable {
  private static final long serialVersionUID = 1L;

  private transient Tika tika;
  private transient AutoDetectParser parser;

  public ExtractedDocument extract(String url, byte[] body, String contentType) {
    String ct = contentType == null ? "" : contentType.toLowerCase();
    if (ct.contains("html") || ct.contains("xhtml") || ct.isEmpty()) {
      return extractHtml(url, body);
    }
    return extractViaTika(body, contentType);
  }

  /** Extract HTML using Jsoup — title, body text, and discovered links. */
  private ExtractedDocument extractHtml(String url, byte[] body) {
    try {
      Document doc = Jsoup.parse(new String(body, java.nio.charset.StandardCharsets.UTF_8), url);
      String title = doc.title() == null ? "" : doc.title();
      String text = doc.body() == null ? "" : doc.body().text();
      Set<String> links = new LinkedHashSet<>();
      Elements anchors = doc.select("a[href]");
      for (Element a : anchors) {
        String abs = a.absUrl("href");
        if (!abs.isEmpty() && (abs.startsWith("http://") || abs.startsWith("https://"))) {
          links.add(abs);
        }
      }
      Map<String, String> metadata = new HashMap<>();
      Elements metas = doc.select("meta[name][content]");
      for (Element m : metas) {
        metadata.put(m.attr("name"), m.attr("content"));
      }
      return new ExtractedDocument(title, text, List.copyOf(links), metadata);
    } catch (Exception e) {
      return new ExtractedDocument("", "", List.of(), Map.of("extract_error", e.getMessage()));
    }
  }

  /** Extract arbitrary documents via Tika. */
  private ExtractedDocument extractViaTika(byte[] body, String contentType) {
    if (tika == null) tika = new Tika();
    if (parser == null) parser = new AutoDetectParser();
    try (InputStream in = new ByteArrayInputStream(body)) {
      BodyContentHandler handler = new BodyContentHandler(-1);
      Metadata md = new Metadata();
      if (contentType != null && !contentType.isEmpty()) {
        md.set(Metadata.CONTENT_TYPE, contentType);
      }
      parser.parse(in, handler, md, new ParseContext());
      String text = handler.toString();
      String title = md.get("title");
      Map<String, String> metadata = new HashMap<>();
      for (String name : md.names()) {
        metadata.put(name, md.get(name));
      }
      return new ExtractedDocument(title == null ? "" : title, text == null ? "" : text, List.of(), metadata);
    } catch (Exception e) {
      return new ExtractedDocument("", "", List.of(), Map.of("extract_error", e.getMessage()));
    }
  }

  /** Stripped-down result type. */
  public static final class ExtractedDocument implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String title;
    private final String text;
    private final List<String> links;
    private final Map<String, String> metadata;

    public ExtractedDocument(String title, String text, List<String> links, Map<String, String> metadata) {
      this.title = title == null ? "" : title;
      this.text = text == null ? "" : text;
      this.links = links == null ? List.of() : List.copyOf(links);
      this.metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    public String getTitle() {
      return title;
    }

    public String getText() {
      return text;
    }

    public List<String> getLinks() {
      return links;
    }

    public Map<String, String> getMetadata() {
      return metadata;
    }
  }

  /** Convenience for tests: parse raw bytes when no URL context exists. */
  public ExtractedDocument extractBytes(byte[] body, String contentType) throws IOException {
    return extract("urn:bytes", body, contentType);
  }
}
